use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::process::Stdio;

use anyhow::{Context, Result};
use codex_protocol::models::PermissionProfile;
use codex_protocol::permissions::NetworkSandboxPolicy;
use codex_utils_absolute_path::AbsolutePathBuf;
use tokio::fs::File;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt};
use tokio::process::Command;

const INTERNAL_CAPTURE_LIMIT: usize = 64 * 1024;
const STREAM_BUFFER_BYTES: usize = 8 * 1024;

pub struct Output {
    pub status: i32,
    pub stdout: Vec<u8>,
    pub stderr: Vec<u8>,
}

#[derive(Debug)]
pub struct StreamCapture {
    pub bytes: u64,
    pub head: Vec<u8>,
    pub tail: Vec<u8>,
}

#[derive(Debug)]
pub struct LoggedOutput {
    pub status: i32,
    pub stdout: StreamCapture,
    pub stderr: StreamCapture,
}

fn absolute(path: &Path) -> Result<AbsolutePathBuf> {
    let path = if path.is_absolute() {
        path.to_owned()
    } else {
        std::env::current_dir()?.join(path)
    };
    AbsolutePathBuf::from_absolute_path(path).map_err(|error| anyhow::anyhow!(error))
}

fn sandboxed_process(
    command: &[String],
    cwd: &Path,
    writable_roots: &[PathBuf],
) -> Result<Command> {
    anyhow::ensure!(!command.is_empty(), "command must not be empty");
    let cwd = std::fs::canonicalize(cwd)
        .with_context(|| format!("cannot resolve cwd {}", cwd.display()))?;
    let roots = writable_roots
        .iter()
        .map(|path| absolute(path))
        .collect::<Result<Vec<_>>>()?;
    let permissions = PermissionProfile::workspace_write_with(
        &roots,
        NetworkSandboxPolicy::Restricted,
        true,
        true,
    )
    .materialize_project_roots_with_workspace_roots(&[absolute(&cwd)?]);

    #[cfg(target_os = "linux")]
    let mut process = {
        let args =
            codex_sandboxing::landlock::create_linux_sandbox_command_args_for_permission_profile(
                command.to_vec(),
                &cwd,
                &permissions,
                &cwd,
                false,
                false,
            );
        let executable = std::env::current_exe()?
            .parent()
            .context("local-mcp executable has no parent directory")?
            .join("codex-linux-sandbox");
        anyhow::ensure!(
            executable.is_file(),
            "sandbox helper is missing: {}",
            executable.display()
        );
        let mut process = Command::new(executable);
        process.args(args);
        process
    };

    #[cfg(target_os = "macos")]
    let mut process = {
        use codex_sandboxing::seatbelt::CreateSeatbeltCommandArgsParams;
        use codex_sandboxing::seatbelt::MACOS_PATH_TO_SEATBELT_EXECUTABLE;
        use codex_sandboxing::seatbelt::create_seatbelt_command_args;

        let (file_system_policy, network_policy) = permissions.to_runtime_permissions();
        let args = create_seatbelt_command_args(CreateSeatbeltCommandArgsParams {
            command: command.to_vec(),
            file_system_sandbox_policy: &file_system_policy,
            network_sandbox_policy: network_policy,
            sandbox_policy_cwd: &cwd,
            enforce_managed_network: false,
            network: None,
            extra_allow_unix_sockets: &[],
        });
        let mut process = Command::new(MACOS_PATH_TO_SEATBELT_EXECUTABLE);
        process.args(args);
        process
    };

    #[cfg(windows)]
    let mut process = {
        // Windows has no application sandbox here. Preserve argv execution and
        // the restricted environment while documenting that this is direct host
        // execution rather than filesystem/network isolation.
        let mut process = Command::new(&command[0]);
        process.args(&command[1..]);
        process
    };

    #[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "windows")))]
    let mut process = { anyhow::bail!("sandboxed execution is unsupported on this platform") };

    process
        .kill_on_drop(true)
        .current_dir(&cwd)
        .env_clear()
        .envs(safe_environment());
    Ok(process)
}

fn unrestricted_process(command: &[String], cwd: &Path) -> Result<Command> {
    anyhow::ensure!(!command.is_empty(), "command must not be empty");
    let cwd = std::fs::canonicalize(cwd)
        .with_context(|| format!("cannot resolve cwd {}", cwd.display()))?;
    let mut process = Command::new(&command[0]);
    process
        .kill_on_drop(true)
        .args(&command[1..])
        .current_dir(cwd);
    Ok(process)
}

pub async fn run(
    command: &[String],
    cwd: &Path,
    writable_roots: &[PathBuf],
    stdin: Option<&[u8]>,
) -> Result<Output> {
    let process = sandboxed_process(command, cwd, writable_roots)?;
    let output = run_process(process, stdin, None, None, INTERNAL_CAPTURE_LIMIT).await?;
    Ok(Output {
        status: output.status,
        stdout: bounded_stream_bytes(output.stdout),
        stderr: bounded_stream_bytes(output.stderr),
    })
}

pub async fn run_logged(
    command: &[String],
    cwd: &Path,
    writable_roots: &[PathBuf],
    stdin: Option<&[u8]>,
    stdout_path: &Path,
    stderr_path: &Path,
    preview_limit: usize,
) -> Result<LoggedOutput> {
    let stdout_file = File::create(stdout_path)
        .await
        .with_context(|| format!("cannot create stdout log {}", stdout_path.display()))?;
    let stderr_file = File::create(stderr_path)
        .await
        .with_context(|| format!("cannot create stderr log {}", stderr_path.display()))?;
    let process = sandboxed_process(command, cwd, writable_roots)?;
    run_process(
        process,
        stdin,
        Some(stdout_file),
        Some(stderr_file),
        preview_limit,
    )
    .await
}

pub async fn run_unrestricted_logged(
    command: &[String],
    cwd: &Path,
    stdin: Option<&[u8]>,
    stdout_path: &Path,
    stderr_path: &Path,
    preview_limit: usize,
) -> Result<LoggedOutput> {
    let stdout_file = File::create(stdout_path)
        .await
        .with_context(|| format!("cannot create stdout log {}", stdout_path.display()))?;
    let stderr_file = File::create(stderr_path)
        .await
        .with_context(|| format!("cannot create stderr log {}", stderr_path.display()))?;
    let process = unrestricted_process(command, cwd)?;
    run_process(
        process,
        stdin,
        Some(stdout_file),
        Some(stderr_file),
        preview_limit,
    )
    .await
}

async fn run_process(
    mut process: Command,
    stdin: Option<&[u8]>,
    stdout_file: Option<File>,
    stderr_file: Option<File>,
    preview_limit: usize,
) -> Result<LoggedOutput> {
    process
        .stdin(if stdin.is_some() {
            Stdio::piped()
        } else {
            Stdio::null()
        })
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let mut child = process.spawn().context("failed to start command")?;
    let stdout = child
        .stdout
        .take()
        .context("command stdout was not piped")?;
    let stderr = child
        .stderr
        .take()
        .context("command stderr was not piped")?;
    if let Some(bytes) = stdin
        && let Some(mut child_stdin) = child.stdin.take()
    {
        child_stdin.write_all(bytes).await?;
        child_stdin.shutdown().await?;
    }

    let (status, stdout, stderr) = tokio::try_join!(
        async { child.wait().await.context("failed to wait for command") },
        async {
            pump_stream(stdout, stdout_file, preview_limit)
                .await
                .context("failed to capture command stdout")
        },
        async {
            pump_stream(stderr, stderr_file, preview_limit)
                .await
                .context("failed to capture command stderr")
        },
    )?;
    Ok(LoggedOutput {
        status: status.code().unwrap_or(-1),
        stdout,
        stderr,
    })
}

async fn pump_stream<R>(
    mut reader: R,
    mut file: Option<File>,
    preview_limit: usize,
) -> Result<StreamCapture>
where
    R: AsyncRead + Unpin,
{
    let mut capture = StreamCapture {
        bytes: 0,
        head: Vec::with_capacity(preview_limit.min(STREAM_BUFFER_BYTES)),
        tail: Vec::with_capacity(preview_limit.min(STREAM_BUFFER_BYTES)),
    };
    let mut buffer = vec![0_u8; STREAM_BUFFER_BYTES];
    loop {
        let read = reader.read(&mut buffer).await?;
        if read == 0 {
            break;
        }
        let chunk = &buffer[..read];
        if let Some(file) = file.as_mut() {
            file.write_all(chunk).await?;
        }
        capture.bytes = capture.bytes.saturating_add(read as u64);
        let head_remaining = preview_limit.saturating_sub(capture.head.len());
        capture
            .head
            .extend_from_slice(&chunk[..chunk.len().min(head_remaining)]);
        update_tail(&mut capture.tail, chunk, preview_limit);
    }
    if let Some(file) = file.as_mut() {
        file.flush().await?;
    }
    Ok(capture)
}

fn update_tail(tail: &mut Vec<u8>, chunk: &[u8], limit: usize) {
    if limit == 0 {
        return;
    }
    if chunk.len() >= limit {
        tail.clear();
        tail.extend_from_slice(&chunk[chunk.len() - limit..]);
        return;
    }
    let overflow = tail.len().saturating_add(chunk.len()).saturating_sub(limit);
    if overflow > 0 {
        tail.drain(..overflow);
    }
    tail.extend_from_slice(chunk);
}

fn bounded_stream_bytes(capture: StreamCapture) -> Vec<u8> {
    if capture.bytes <= capture.head.len() as u64 {
        return capture.head;
    }
    let mut bytes = capture.head;
    bytes.extend_from_slice(b"\n... output truncated ...\n");
    bytes.extend_from_slice(&capture.tail);
    bytes
}

fn safe_environment() -> HashMap<String, String> {
    [
        "PATH",
        "LANG",
        "LC_ALL",
        "TERM",
        "TMPDIR",
        "TEMP",
        "TMP",
        "SystemRoot",
    ]
    .into_iter()
    .filter_map(|name| {
        std::env::var(name)
            .ok()
            .map(|value| (name.to_owned(), value))
    })
    .collect()
}

#[cfg(all(test, target_os = "macos"))]
mod tests {
    use super::*;
    use uuid::Uuid;

    fn test_directory() -> PathBuf {
        std::env::temp_dir().join(format!("local-mcp-sandbox-test-{}", Uuid::new_v4()))
    }

    #[tokio::test]
    async fn seatbelt_allows_workspace_writes_and_denies_other_writes() -> Result<()> {
        // Nix's macOS build sandbox does not allow a nested Seatbelt profile.
        if std::env::var_os("NIX_BUILD_TOP").is_some() {
            return Ok(());
        }
        let root = test_directory();
        let workspace = root.join("workspace");
        let outside = root.join("outside");
        std::fs::create_dir_all(&workspace)?;
        std::fs::create_dir_all(&outside)?;

        let allowed = run(
            &["/usr/bin/touch".into(), "allowed".into()],
            &workspace,
            &[],
            None,
        )
        .await?;
        assert_eq!(
            allowed.status,
            0,
            "{}",
            String::from_utf8_lossy(&allowed.stderr)
        );
        assert!(workspace.join("allowed").is_file());

        let denied_path = outside.join("denied");
        let denied = run(
            &[
                "/usr/bin/touch".into(),
                denied_path.to_string_lossy().into_owned(),
            ],
            &workspace,
            &[],
            None,
        )
        .await?;
        assert_ne!(denied.status, 0);
        assert!(!denied_path.exists());

        std::fs::remove_dir_all(root)?;
        Ok(())
    }

    #[tokio::test]
    async fn seatbelt_denies_network_access() -> Result<()> {
        if std::env::var_os("NIX_BUILD_TOP").is_some() {
            return Ok(());
        }
        let workspace = test_directory();
        std::fs::create_dir_all(&workspace)?;
        let output = run(
            &[
                "/usr/bin/curl".into(),
                "--fail".into(),
                "--max-time".into(),
                "2".into(),
                "https://example.com".into(),
            ],
            &workspace,
            &[],
            None,
        )
        .await?;
        assert_ne!(output.status, 0);

        std::fs::remove_dir_all(workspace)?;
        Ok(())
    }
}

#[cfg(all(test, unix))]
mod streaming_tests {
    use super::*;
    use uuid::Uuid;

    #[tokio::test]
    async fn unrestricted_output_streams_to_files_with_bounded_memory_previews() -> Result<()> {
        let directory =
            std::env::temp_dir().join(format!("local-mcp-stream-test-{}", Uuid::new_v4()));
        tokio::fs::create_dir_all(&directory).await?;
        let stdout_path = directory.join("stdout");
        let stderr_path = directory.join("stderr");
        let output = run_unrestricted_logged(
            &[
                "sh".into(),
                "-c".into(),
                "head -c 1048576 /dev/zero; printf failure >&2".into(),
            ],
            &directory,
            None,
            &stdout_path,
            &stderr_path,
            1024,
        )
        .await?;

        assert_eq!(output.status, 0);
        assert_eq!(output.stdout.bytes, 1_048_576);
        assert!(output.stdout.head.len() <= 1024);
        assert!(output.stdout.tail.len() <= 1024);
        assert_eq!(output.stderr.bytes, 7);
        assert_eq!(tokio::fs::metadata(&stdout_path).await?.len(), 1_048_576);
        assert_eq!(tokio::fs::read(&stderr_path).await?, b"failure");

        tokio::fs::remove_dir_all(directory).await?;
        Ok(())
    }
}
