use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::process::{ExitStatus, Stdio};
use std::time::Duration;

use anyhow::{Context, Result};
use codex_protocol::models::PermissionProfile;
use codex_protocol::permissions::NetworkSandboxPolicy;
use codex_utils_absolute_path::AbsolutePathBuf;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt};
use tokio::process::{Child, Command};
use tokio::sync::watch;

const TERMINATION_GRACE: Duration = Duration::from_millis(500);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StopTrigger {
    Requested,
    Timeout,
    Cancellation,
}

impl StopTrigger {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Requested => "stop",
            Self::Timeout => "timeout",
            Self::Cancellation => "cancellation",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Termination {
    Exited,
    Stopped,
    TimedOut,
    Cancelled,
    ForcedKill(StopTrigger),
}

impl Termination {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Exited => "exited",
            Self::Stopped => "stopped",
            Self::TimedOut => "timeout",
            Self::Cancelled => "cancelled",
            Self::ForcedKill(_) => "forced_kill",
        }
    }

    pub fn trigger(self) -> Option<StopTrigger> {
        match self {
            Self::Exited => None,
            Self::Stopped => Some(StopTrigger::Requested),
            Self::TimedOut => Some(StopTrigger::Timeout),
            Self::Cancelled => Some(StopTrigger::Cancellation),
            Self::ForcedKill(trigger) => Some(trigger),
        }
    }
}

#[derive(Debug)]
pub struct Output {
    pub status: i32,
    pub stdout: String,
    pub stderr: String,
    pub termination: Termination,
}

#[derive(Clone)]
pub struct CommandControl {
    sender: watch::Sender<Option<StopTrigger>>,
}

pub struct CommandCancellation {
    receiver: watch::Receiver<Option<StopTrigger>>,
}

impl CommandControl {
    pub fn request_stop(&self) -> bool {
        self.sender.send(Some(StopTrigger::Requested)).is_ok()
    }
}

pub fn command_control() -> (CommandControl, CommandCancellation) {
    let (sender, receiver) = watch::channel(None);
    (CommandControl { sender }, CommandCancellation { receiver })
}

fn absolute(path: &Path) -> Result<AbsolutePathBuf> {
    let path = if path.is_absolute() {
        path.to_owned()
    } else {
        std::env::current_dir()?.join(path)
    };
    AbsolutePathBuf::from_absolute_path(path).map_err(|error| anyhow::anyhow!(error))
}

pub async fn run(
    command: &[String],
    cwd: &Path,
    writable_roots: &[PathBuf],
    stdin: Option<&[u8]>,
) -> Result<Output> {
    let (control, cancellation) = command_control();
    let result = run_controlled(command, cwd, writable_roots, stdin, cancellation, None).await;
    drop(control);
    result
}

pub async fn run_controlled(
    command: &[String],
    cwd: &Path,
    writable_roots: &[PathBuf],
    stdin: Option<&[u8]>,
    cancellation: CommandCancellation,
    execution_timeout: Option<Duration>,
) -> Result<Output> {
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
        // Windows has no equivalent of Landlock/Seatbelt in this application.
        // Preserve argv execution and the restricted environment so the
        // feature remains usable, while documenting that this is not a
        // filesystem/network sandbox.
        let mut process = Command::new(&command[0]);
        process.args(&command[1..]);
        process
    };

    #[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "windows")))]
    let mut process = { anyhow::bail!("sandboxed execution is unsupported on this platform") };

    process
        .current_dir(&cwd)
        .env_clear()
        .envs(safe_environment());
    run_process(process, stdin, cancellation, execution_timeout, "sandboxed").await
}

pub async fn run_unrestricted(
    command: &[String],
    cwd: &Path,
    stdin: Option<&[u8]>,
) -> Result<Output> {
    let (control, cancellation) = command_control();
    let result = run_unrestricted_controlled(command, cwd, stdin, cancellation, None).await;
    drop(control);
    result
}

pub async fn run_unrestricted_controlled(
    command: &[String],
    cwd: &Path,
    stdin: Option<&[u8]>,
    cancellation: CommandCancellation,
    execution_timeout: Option<Duration>,
) -> Result<Output> {
    anyhow::ensure!(!command.is_empty(), "command must not be empty");
    let cwd = std::fs::canonicalize(cwd)
        .with_context(|| format!("cannot resolve cwd {}", cwd.display()))?;
    let mut process = Command::new(&command[0]);
    process.args(&command[1..]).current_dir(cwd);
    run_process(
        process,
        stdin,
        cancellation,
        execution_timeout,
        "unsandboxed",
    )
    .await
}

async fn run_process(
    mut process: Command,
    stdin: Option<&[u8]>,
    mut cancellation: CommandCancellation,
    execution_timeout: Option<Duration>,
    context: &str,
) -> Result<Output> {
    configure_process_group(&mut process);
    process
        .kill_on_drop(true)
        .stdin(if stdin.is_some() {
            Stdio::piped()
        } else {
            Stdio::null()
        })
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());

    let mut child = process
        .spawn()
        .with_context(|| format!("failed to start {context} command"))?;
    let process_id = child.id().context("spawned command has no process ID")?;
    let mut tree = ProcessTreeGuard::new(process_id);

    if let Some(bytes) = stdin
        && let Some(mut child_stdin) = child.stdin.take()
    {
        child_stdin.write_all(bytes).await?;
        child_stdin.shutdown().await?;
    }

    let stdout = child
        .stdout
        .take()
        .context("command stdout was not piped")?;
    let stderr = child
        .stderr
        .take()
        .context("command stderr was not piped")?;
    let stdout_task = tokio::spawn(read_all(stdout));
    let stderr_task = tokio::spawn(read_all(stderr));

    let event = wait_event(&mut child, &mut cancellation, execution_timeout).await;
    let (status, termination) = match event {
        WaitEvent::Exited(status) => {
            let status = status.context("failed to wait for command")?;
            tree.disarm();
            (status, Termination::Exited)
        }
        WaitEvent::Triggered(trigger) => terminate_and_reap(&mut child, &mut tree, trigger).await?,
    };

    let stdout = stdout_task
        .await
        .context("stdout reader task failed")?
        .context("failed to read command stdout")?;
    let stderr = stderr_task
        .await
        .context("stderr reader task failed")?
        .context("failed to read command stderr")?;

    Ok(Output {
        status: status.code().unwrap_or(-1),
        stdout: String::from_utf8_lossy(&stdout).into_owned(),
        stderr: String::from_utf8_lossy(&stderr).into_owned(),
        termination,
    })
}

async fn read_all(mut reader: impl AsyncRead + Unpin) -> std::io::Result<Vec<u8>> {
    let mut bytes = Vec::new();
    reader.read_to_end(&mut bytes).await?;
    Ok(bytes)
}

enum WaitEvent {
    Exited(std::io::Result<ExitStatus>),
    Triggered(StopTrigger),
}

async fn wait_event(
    child: &mut Child,
    cancellation: &mut CommandCancellation,
    execution_timeout: Option<Duration>,
) -> WaitEvent {
    if let Some(timeout) = execution_timeout {
        tokio::select! {
            status = child.wait() => WaitEvent::Exited(status),
            trigger = next_trigger(cancellation) => WaitEvent::Triggered(trigger),
            _ = tokio::time::sleep(timeout) => WaitEvent::Triggered(StopTrigger::Timeout),
        }
    } else {
        tokio::select! {
            status = child.wait() => WaitEvent::Exited(status),
            trigger = next_trigger(cancellation) => WaitEvent::Triggered(trigger),
        }
    }
}

async fn next_trigger(cancellation: &mut CommandCancellation) -> StopTrigger {
    loop {
        if let Some(trigger) = *cancellation.receiver.borrow() {
            return trigger;
        }
        if cancellation.receiver.changed().await.is_err() {
            return StopTrigger::Cancellation;
        }
    }
}

async fn terminate_and_reap(
    child: &mut Child,
    tree: &mut ProcessTreeGuard,
    trigger: StopTrigger,
) -> Result<(ExitStatus, Termination)> {
    tree.request_graceful().await;
    match tokio::time::timeout(TERMINATION_GRACE, child.wait()).await {
        Ok(status) => {
            let status = status.context("failed to reap terminated command")?;
            tree.disarm();
            Ok((status, graceful_termination(trigger)))
        }
        Err(_) => {
            tree.force().await;
            let _ = child.start_kill();
            let status = child
                .wait()
                .await
                .context("failed to reap force-killed command")?;
            tree.disarm();
            Ok((status, Termination::ForcedKill(trigger)))
        }
    }
}

fn graceful_termination(trigger: StopTrigger) -> Termination {
    match trigger {
        StopTrigger::Requested => Termination::Stopped,
        StopTrigger::Timeout => Termination::TimedOut,
        StopTrigger::Cancellation => Termination::Cancelled,
    }
}

struct ProcessTreeGuard {
    process_id: u32,
    armed: bool,
}

impl ProcessTreeGuard {
    fn new(process_id: u32) -> Self {
        Self {
            process_id,
            armed: true,
        }
    }

    fn disarm(&mut self) {
        self.armed = false;
    }

    async fn request_graceful(&self) {
        if self.armed {
            terminate_tree_async(self.process_id, false).await;
        }
    }

    async fn force(&self) {
        if self.armed {
            terminate_tree_async(self.process_id, true).await;
        }
    }
}

impl Drop for ProcessTreeGuard {
    fn drop(&mut self) {
        if self.armed {
            terminate_tree_sync(self.process_id, true);
        }
    }
}

#[cfg(unix)]
fn configure_process_group(process: &mut Command) {
    process.process_group(0);
}

#[cfg(windows)]
fn configure_process_group(_process: &mut Command) {}

#[cfg(not(any(unix, windows)))]
fn configure_process_group(_process: &mut Command) {}

#[cfg(unix)]
async fn terminate_tree_async(process_id: u32, force: bool) {
    terminate_unix_process_group(process_id, force);
}

#[cfg(unix)]
fn terminate_tree_sync(process_id: u32, force: bool) {
    terminate_unix_process_group(process_id, force);
}

#[cfg(unix)]
fn terminate_unix_process_group(process_id: u32, force: bool) {
    let signal = if force { libc::SIGKILL } else { libc::SIGTERM };
    let Ok(group_id) = i32::try_from(process_id) else {
        return;
    };
    // A negative PID targets every process in the dedicated process group.
    unsafe {
        libc::kill(-group_id, signal);
    }
}

#[cfg(windows)]
async fn terminate_tree_async(process_id: u32, force: bool) {
    let mut command = Command::new("taskkill.exe");
    let process_id = process_id.to_string();
    command
        .args(["/PID", process_id.as_str(), "/T"])
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    if force {
        command.arg("/F");
    }
    let _ = command.status().await;
}

#[cfg(windows)]
fn terminate_tree_sync(process_id: u32, force: bool) {
    let mut command = std::process::Command::new("taskkill.exe");
    let process_id = process_id.to_string();
    command
        .args(["/PID", process_id.as_str(), "/T"])
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    if force {
        command.arg("/F");
    }
    let _ = command.status();
}

#[cfg(not(any(unix, windows)))]
async fn terminate_tree_async(_process_id: u32, _force: bool) {}

#[cfg(not(any(unix, windows)))]
fn terminate_tree_sync(_process_id: u32, _force: bool) {}

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

#[cfg(all(test, unix))]
mod process_tree_tests {
    use super::*;
    use uuid::Uuid;

    fn test_directory() -> PathBuf {
        std::env::temp_dir().join(format!("local-mcp-tree-test-{}", Uuid::new_v4()))
    }

    async fn wait_for_pids(path: &Path) -> (i32, i32) {
        for _ in 0..200 {
            if let Ok(text) = tokio::fs::read_to_string(path).await {
                let ids = text
                    .split_whitespace()
                    .map(str::parse::<i32>)
                    .collect::<std::result::Result<Vec<_>, _>>()
                    .unwrap();
                if ids.len() == 2 {
                    return (ids[0], ids[1]);
                }
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("process IDs were not written");
    }

    fn process_exists(process_id: i32) -> bool {
        let result = unsafe { libc::kill(process_id, 0) };
        if result == 0 {
            true
        } else {
            std::io::Error::last_os_error().raw_os_error() == Some(libc::EPERM)
        }
    }

    async fn wait_until_gone(process_ids: [i32; 2]) {
        for _ in 0..200 {
            if process_ids
                .iter()
                .all(|process_id| !process_exists(*process_id))
            {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("process tree still exists: {process_ids:?}");
    }

    fn tree_command(pid_file: &Path, ignore_term: bool) -> Vec<String> {
        let trap = if ignore_term { "trap '' TERM; " } else { "" };
        vec![
            "sh".to_owned(),
            "-c".to_owned(),
            format!(
                "{trap}sleep 30 & child=$!; printf '%s %s\\n' $$ $child > '{}'; wait",
                pid_file.display()
            ),
        ]
    }

    #[tokio::test]
    async fn normal_exit_is_distinct_from_lifecycle_termination() {
        let directory = test_directory();
        tokio::fs::create_dir_all(&directory).await.unwrap();
        let (control, cancellation) = command_control();
        let output = run_unrestricted_controlled(
            &["sh".to_owned(), "-c".to_owned(), "exit 0".to_owned()],
            &directory,
            None,
            cancellation,
            None,
        )
        .await
        .unwrap();
        drop(control);

        assert_eq!(output.status, 0);
        assert_eq!(output.termination, Termination::Exited);
        tokio::fs::remove_dir_all(directory).await.unwrap();
    }

    #[tokio::test]
    async fn requested_stop_terminates_parent_and_child_and_reaps_parent() {
        let directory = test_directory();
        tokio::fs::create_dir_all(&directory).await.unwrap();
        let pid_file = directory.join("pids");
        let command = tree_command(&pid_file, false);
        let (control, cancellation) = command_control();
        let task_directory = directory.clone();
        let task = tokio::spawn(async move {
            run_unrestricted_controlled(&command, &task_directory, None, cancellation, None).await
        });
        let process_ids = wait_for_pids(&pid_file).await;

        assert!(control.request_stop());
        let output = task.await.unwrap().unwrap();
        assert_eq!(output.termination, Termination::Stopped);
        wait_until_gone([process_ids.0, process_ids.1]).await;
        tokio::fs::remove_dir_all(directory).await.unwrap();
    }

    #[tokio::test]
    async fn ignored_graceful_signal_escalates_to_forced_tree_kill() {
        let directory = test_directory();
        tokio::fs::create_dir_all(&directory).await.unwrap();
        let pid_file = directory.join("pids");
        let command = tree_command(&pid_file, true);
        let (control, cancellation) = command_control();
        let task_directory = directory.clone();
        let task = tokio::spawn(async move {
            run_unrestricted_controlled(&command, &task_directory, None, cancellation, None).await
        });
        let process_ids = wait_for_pids(&pid_file).await;

        assert!(control.request_stop());
        let output = task.await.unwrap().unwrap();
        assert_eq!(
            output.termination,
            Termination::ForcedKill(StopTrigger::Requested)
        );
        wait_until_gone([process_ids.0, process_ids.1]).await;
        tokio::fs::remove_dir_all(directory).await.unwrap();
    }

    #[tokio::test]
    async fn execution_timeout_uses_the_same_tree_termination_path() {
        let directory = test_directory();
        tokio::fs::create_dir_all(&directory).await.unwrap();
        let pid_file = directory.join("pids");
        let command = tree_command(&pid_file, false);
        let (control, cancellation) = command_control();
        let task_directory = directory.clone();
        let task = tokio::spawn(async move {
            run_unrestricted_controlled(
                &command,
                &task_directory,
                None,
                cancellation,
                Some(Duration::from_millis(100)),
            )
            .await
        });
        let process_ids = wait_for_pids(&pid_file).await;
        let output = task.await.unwrap().unwrap();
        drop(control);

        assert_eq!(output.termination, Termination::TimedOut);
        wait_until_gone([process_ids.0, process_ids.1]).await;
        tokio::fs::remove_dir_all(directory).await.unwrap();
    }

    #[tokio::test]
    async fn task_abort_drop_guard_kills_the_complete_process_group() {
        let directory = test_directory();
        tokio::fs::create_dir_all(&directory).await.unwrap();
        let pid_file = directory.join("pids");
        let command = tree_command(&pid_file, false);
        let (_control, cancellation) = command_control();
        let task_directory = directory.clone();
        let task = tokio::spawn(async move {
            run_unrestricted_controlled(&command, &task_directory, None, cancellation, None).await
        });
        let process_ids = wait_for_pids(&pid_file).await;

        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        wait_until_gone([process_ids.0, process_ids.1]).await;
        tokio::fs::remove_dir_all(directory).await.unwrap();
    }
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
        assert_eq!(allowed.status, 0, "{}", allowed.stderr);
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
