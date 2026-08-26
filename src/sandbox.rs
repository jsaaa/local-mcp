use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::process::{ExitStatus, Stdio};
use std::time::Duration;

use anyhow::{Context, Result};
use codex_protocol::models::PermissionProfile;
use codex_protocol::permissions::NetworkSandboxPolicy;
use codex_utils_absolute_path::AbsolutePathBuf;
use tokio::fs::File;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt};
use tokio::process::{Child, Command};
use tokio::sync::watch;

const TERMINATION_GRACE: Duration = Duration::from_millis(500);
const INTERNAL_CAPTURE_LIMIT: usize = 64 * 1024;
const STREAM_BUFFER_BYTES: usize = 8 * 1024;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StopTrigger {
    Requested,
    Timeout,
    Cancellation,
    Completion,
}

impl StopTrigger {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Requested => "stop",
            Self::Timeout => "timeout",
            Self::Cancellation => "cancellation",
            Self::Completion => "completion",
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
    Ok(process)
}

fn unrestricted_process(command: &[String], cwd: &Path) -> Result<Command> {
    anyhow::ensure!(!command.is_empty(), "command must not be empty");
    let cwd = std::fs::canonicalize(cwd)
        .with_context(|| format!("cannot resolve cwd {}", cwd.display()))?;
    let mut process = Command::new(&command[0]);
    process.args(&command[1..]).current_dir(cwd);
    Ok(process)
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
    let process = sandboxed_process(command, cwd, writable_roots)?;
    let output = run_process(
        process,
        stdin,
        cancellation,
        execution_timeout,
        "sandboxed",
        None,
        None,
        INTERNAL_CAPTURE_LIMIT,
    )
    .await?;
    Ok(logged_output_to_output(output))
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
    let (control, cancellation) = command_control();
    let result = run_logged_controlled(
        command,
        cwd,
        writable_roots,
        stdin,
        cancellation,
        None,
        stdout_path,
        stderr_path,
        preview_limit,
    )
    .await;
    drop(control);
    result
}

#[allow(clippy::too_many_arguments)]
pub async fn run_logged_controlled(
    command: &[String],
    cwd: &Path,
    writable_roots: &[PathBuf],
    stdin: Option<&[u8]>,
    cancellation: CommandCancellation,
    execution_timeout: Option<Duration>,
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
        cancellation,
        execution_timeout,
        "sandboxed",
        Some(stdout_file),
        Some(stderr_file),
        preview_limit,
    )
    .await
}

#[allow(dead_code)]
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
    let process = unrestricted_process(command, cwd)?;
    let output = run_process(
        process,
        stdin,
        cancellation,
        execution_timeout,
        "unsandboxed",
        None,
        None,
        INTERNAL_CAPTURE_LIMIT,
    )
    .await?;
    Ok(logged_output_to_output(output))
}

pub async fn run_unrestricted_logged(
    command: &[String],
    cwd: &Path,
    stdin: Option<&[u8]>,
    stdout_path: &Path,
    stderr_path: &Path,
    preview_limit: usize,
) -> Result<LoggedOutput> {
    let (control, cancellation) = command_control();
    let result = run_unrestricted_logged_controlled(
        command,
        cwd,
        stdin,
        cancellation,
        None,
        stdout_path,
        stderr_path,
        preview_limit,
    )
    .await;
    drop(control);
    result
}

#[allow(clippy::too_many_arguments)]
pub async fn run_unrestricted_logged_controlled(
    command: &[String],
    cwd: &Path,
    stdin: Option<&[u8]>,
    cancellation: CommandCancellation,
    execution_timeout: Option<Duration>,
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
        cancellation,
        execution_timeout,
        "unsandboxed",
        Some(stdout_file),
        Some(stderr_file),
        preview_limit,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn run_process(
    mut process: Command,
    stdin: Option<&[u8]>,
    mut cancellation: CommandCancellation,
    execution_timeout: Option<Duration>,
    context: &str,
    stdout_file: Option<File>,
    stderr_file: Option<File>,
    preview_limit: usize,
) -> Result<LoggedOutput> {
    let prepared_tree = prepare_process_tree(&mut process)?;
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
    let mut tree = match prepared_tree.attach(&child, process_id) {
        Ok(tree) => tree,
        Err(error) => {
            let _ = child.start_kill();
            let _ = child.wait().await;
            return Err(error).context("failed to establish process-tree ownership");
        }
    };

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

    let lifecycle = async {
        let event = wait_event(&mut child, &mut cancellation, execution_timeout).await;
        match event {
            WaitEvent::Exited(status) => {
                let status = status.context("failed to wait for command")?;
                let forced_cleanup = tree.cleanup_after_direct_exit(false).await?;
                let termination = if forced_cleanup {
                    Termination::ForcedKill(StopTrigger::Completion)
                } else {
                    Termination::Exited
                };
                Ok::<_, anyhow::Error>((status, termination))
            }
            WaitEvent::Triggered(trigger) => {
                terminate_and_reap(&mut child, &mut tree, trigger).await
            }
        }
    };

    let ((status, termination), stdout, stderr) = tokio::try_join!(
        lifecycle,
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
        termination,
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

fn logged_output_to_output(output: LoggedOutput) -> Output {
    Output {
        status: output.status,
        stdout: String::from_utf8_lossy(&bounded_stream_bytes(output.stdout)).into_owned(),
        stderr: String::from_utf8_lossy(&bounded_stream_bytes(output.stderr)).into_owned(),
        termination: output.termination,
    }
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
    let graceful_requested = tree.request_graceful().await.is_ok();
    match tokio::time::timeout(TERMINATION_GRACE, child.wait()).await {
        Ok(status) => {
            let status = status.context("failed to reap terminated command")?;
            let forced_cleanup = tree.cleanup_after_direct_exit(graceful_requested).await?;
            let termination = if forced_cleanup {
                Termination::ForcedKill(trigger)
            } else {
                graceful_termination(trigger)
            };
            Ok((status, termination))
        }
        Err(_) => {
            tree.force().await?;
            let _ = child.start_kill();
            let status = child
                .wait()
                .await
                .context("failed to reap force-killed command")?;
            tree.finish_forced_cleanup().await?;
            Ok((status, Termination::ForcedKill(trigger)))
        }
    }
}

fn graceful_termination(trigger: StopTrigger) -> Termination {
    match trigger {
        StopTrigger::Requested => Termination::Stopped,
        StopTrigger::Timeout => Termination::TimedOut,
        StopTrigger::Cancellation => Termination::Cancelled,
        StopTrigger::Completion => Termination::Exited,
    }
}

#[cfg(windows)]
use self::windows_process_tree::WindowsJob;

struct PreparedProcessTree {
    #[cfg(windows)]
    job: WindowsJob,
}

#[cfg(unix)]
fn prepare_process_tree(process: &mut Command) -> Result<PreparedProcessTree> {
    process.process_group(0);
    Ok(PreparedProcessTree {})
}

#[cfg(windows)]
fn prepare_process_tree(process: &mut Command) -> Result<PreparedProcessTree> {
    use windows_sys::Win32::System::Threading::CREATE_SUSPENDED;

    process.creation_flags(CREATE_SUSPENDED);
    Ok(PreparedProcessTree {
        job: WindowsJob::new()?,
    })
}

#[cfg(not(any(unix, windows)))]
fn prepare_process_tree(_process: &mut Command) -> Result<PreparedProcessTree> {
    Ok(PreparedProcessTree {})
}

#[cfg(windows)]
impl PreparedProcessTree {
    fn attach(self, child: &Child, process_id: u32) -> Result<ProcessTreeGuard> {
        self.job.assign_and_resume(child, process_id)?;
        Ok(ProcessTreeGuard {
            process_id,
            armed: true,
            job: self.job,
        })
    }
}

#[cfg(not(windows))]
impl PreparedProcessTree {
    fn attach(self, _child: &Child, process_id: u32) -> Result<ProcessTreeGuard> {
        Ok(ProcessTreeGuard {
            process_id,
            armed: true,
        })
    }
}

struct ProcessTreeGuard {
    process_id: u32,
    armed: bool,
    #[cfg(windows)]
    job: WindowsJob,
}

impl ProcessTreeGuard {
    fn disarm(&mut self) {
        self.armed = false;
    }

    async fn request_graceful(&self) -> Result<()> {
        if self.armed {
            terminate_tree_async(self.process_id, false).await?;
        }
        Ok(())
    }

    async fn force(&self) -> Result<()> {
        if !self.armed {
            return Ok(());
        }
        #[cfg(windows)]
        {
            self.job.terminate()
        }
        #[cfg(not(windows))]
        {
            terminate_tree_async(self.process_id, true).await
        }
    }

    async fn cleanup_after_direct_exit(
        &mut self,
        graceful_already_requested: bool,
    ) -> Result<bool> {
        #[cfg(unix)]
        {
            if !unix_process_group_exists(self.process_id) {
                self.disarm();
                return Ok(false);
            }
            if !graceful_already_requested {
                self.request_graceful().await?;
            }
            if wait_for_unix_process_group_exit(self.process_id, TERMINATION_GRACE).await {
                self.disarm();
                return Ok(false);
            }
            self.force().await?;
            let _ = wait_for_unix_process_group_exit(self.process_id, TERMINATION_GRACE).await;
            self.disarm();
            Ok(true)
        }
        #[cfg(windows)]
        {
            if self.job.active_processes()? == 0 {
                self.disarm();
                return Ok(false);
            }
            if !graceful_already_requested {
                // The direct parent may already be gone. Failure here is not
                // treated as success; the owned Job Object is verified below
                // and force-terminated if descendants remain.
                let _ = self.request_graceful().await;
            }
            if wait_for_windows_job_exit(&self.job, TERMINATION_GRACE).await? {
                self.disarm();
                return Ok(false);
            }
            self.force().await?;
            anyhow::ensure!(
                wait_for_windows_job_exit(&self.job, TERMINATION_GRACE).await?,
                "Windows Job Object still has active processes after termination"
            );
            self.disarm();
            Ok(true)
        }
        #[cfg(not(any(unix, windows)))]
        {
            let _ = graceful_already_requested;
            self.disarm();
            Ok(false)
        }
    }

    async fn finish_forced_cleanup(&mut self) -> Result<()> {
        #[cfg(unix)]
        {
            let _ = wait_for_unix_process_group_exit(self.process_id, TERMINATION_GRACE).await;
        }
        #[cfg(windows)]
        {
            anyhow::ensure!(
                wait_for_windows_job_exit(&self.job, TERMINATION_GRACE).await?,
                "Windows Job Object still has active processes after forced termination"
            );
        }
        self.disarm();
        Ok(())
    }
}

impl Drop for ProcessTreeGuard {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        #[cfg(unix)]
        terminate_tree_sync(self.process_id, true);
        // On Windows, dropping the last Job Object handle enforces
        // JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE for every associated process.
    }
}

#[cfg(windows)]
async fn wait_for_windows_job_exit(job: &WindowsJob, timeout: Duration) -> Result<bool> {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        if job.active_processes()? == 0 {
            return Ok(true);
        }
        if tokio::time::Instant::now() >= deadline {
            return Ok(false);
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

#[cfg(unix)]
fn unix_process_group_exists(process_id: u32) -> bool {
    let Ok(group_id) = i32::try_from(process_id) else {
        return false;
    };
    let result = unsafe { libc::kill(-group_id, 0) };
    if result == 0 {
        true
    } else {
        std::io::Error::last_os_error().raw_os_error() == Some(libc::EPERM)
    }
}

#[cfg(unix)]
async fn wait_for_unix_process_group_exit(process_id: u32, timeout: Duration) -> bool {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        if !unix_process_group_exists(process_id) {
            return true;
        }
        if tokio::time::Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

#[cfg(unix)]
async fn terminate_tree_async(process_id: u32, force: bool) -> Result<()> {
    terminate_unix_process_group(process_id, force);
    Ok(())
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
async fn terminate_tree_async(process_id: u32, force: bool) -> Result<()> {
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
    let status = command
        .status()
        .await
        .context("failed to run taskkill.exe")?;
    anyhow::ensure!(status.success(), "taskkill.exe exited with {status}");
    Ok(())
}

#[cfg(not(any(unix, windows)))]
async fn terminate_tree_async(_process_id: u32, _force: bool) -> Result<()> {
    Ok(())
}

#[cfg(windows)]
mod windows_process_tree {
    use std::mem::size_of;
    use std::os::windows::io::{AsRawHandle, FromRawHandle, OwnedHandle, RawHandle};
    use std::ptr::{null, null_mut};

    use anyhow::{Context, Result};
    use tokio::process::Child;
    #[cfg(test)]
    use windows_sys::Win32::Foundation::{ERROR_INVALID_PARAMETER, WAIT_OBJECT_0, WAIT_TIMEOUT};
    use windows_sys::Win32::Foundation::{ERROR_NO_MORE_FILES, HANDLE, INVALID_HANDLE_VALUE};
    use windows_sys::Win32::System::Diagnostics::ToolHelp::{
        CreateToolhelp32Snapshot, TH32CS_SNAPTHREAD, THREADENTRY32, Thread32First, Thread32Next,
    };
    use windows_sys::Win32::System::JobObjects::{
        AssignProcessToJobObject, CreateJobObjectW, JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE,
        JOBOBJECT_BASIC_ACCOUNTING_INFORMATION, JOBOBJECT_EXTENDED_LIMIT_INFORMATION,
        JobObjectBasicAccountingInformation, JobObjectExtendedLimitInformation,
        QueryInformationJobObject, SetInformationJobObject, TerminateJobObject,
    };
    #[cfg(test)]
    use windows_sys::Win32::System::Threading::{
        OpenProcess, PROCESS_QUERY_LIMITED_INFORMATION, PROCESS_SYNCHRONIZE, WaitForSingleObject,
    };
    use windows_sys::Win32::System::Threading::{OpenThread, ResumeThread, THREAD_SUSPEND_RESUME};

    pub struct WindowsJob {
        handle: Option<OwnedHandle>,
    }

    impl WindowsJob {
        pub fn new() -> Result<Self> {
            let handle = unsafe { CreateJobObjectW(null(), null()) };
            if handle.is_null() {
                return Err(std::io::Error::last_os_error())
                    .context("failed to create Windows Job Object");
            }
            let handle = unsafe { OwnedHandle::from_raw_handle(handle as RawHandle) };
            let job = Self {
                handle: Some(handle),
            };
            let mut limits = JOBOBJECT_EXTENDED_LIMIT_INFORMATION::default();
            limits.BasicLimitInformation.LimitFlags = JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE;
            let success = unsafe {
                SetInformationJobObject(
                    job.raw_handle()?,
                    JobObjectExtendedLimitInformation,
                    (&limits as *const JOBOBJECT_EXTENDED_LIMIT_INFORMATION).cast(),
                    size_of::<JOBOBJECT_EXTENDED_LIMIT_INFORMATION>() as u32,
                )
            };
            if success == 0 {
                return Err(std::io::Error::last_os_error())
                    .context("failed to configure Windows Job Object kill-on-close");
            }
            Ok(job)
        }

        fn raw_handle(&self) -> Result<HANDLE> {
            self.handle
                .as_ref()
                .map(|handle| handle.as_raw_handle() as HANDLE)
                .context("Windows Job Object handle is closed")
        }

        pub fn assign_and_resume(&self, child: &Child, process_id: u32) -> Result<()> {
            let process_handle = child
                .raw_handle()
                .context("spawned Windows command has no process handle")?
                as HANDLE;
            let success = unsafe { AssignProcessToJobObject(self.raw_handle()?, process_handle) };
            if success == 0 {
                return Err(std::io::Error::last_os_error())
                    .context("failed to assign command to Windows Job Object");
            }
            resume_suspended_process(process_id)
        }

        pub fn active_processes(&self) -> Result<u32> {
            let mut accounting = JOBOBJECT_BASIC_ACCOUNTING_INFORMATION::default();
            let success = unsafe {
                QueryInformationJobObject(
                    self.raw_handle()?,
                    JobObjectBasicAccountingInformation,
                    (&mut accounting as *mut JOBOBJECT_BASIC_ACCOUNTING_INFORMATION).cast(),
                    size_of::<JOBOBJECT_BASIC_ACCOUNTING_INFORMATION>() as u32,
                    null_mut(),
                )
            };
            if success == 0 {
                return Err(std::io::Error::last_os_error())
                    .context("failed to query Windows Job Object accounting");
            }
            Ok(accounting.ActiveProcesses)
        }

        pub fn terminate(&self) -> Result<()> {
            let success = unsafe { TerminateJobObject(self.raw_handle()?, 1) };
            if success == 0 {
                return Err(std::io::Error::last_os_error())
                    .context("failed to terminate Windows Job Object");
            }
            Ok(())
        }

        #[cfg(test)]
        pub fn invalidate_for_test(&mut self) {
            self.handle.take();
        }
    }

    fn resume_suspended_process(process_id: u32) -> Result<()> {
        let snapshot = unsafe { CreateToolhelp32Snapshot(TH32CS_SNAPTHREAD, 0) };
        if snapshot == INVALID_HANDLE_VALUE {
            return Err(std::io::Error::last_os_error())
                .context("failed to enumerate suspended Windows process threads");
        }
        let snapshot = unsafe { OwnedHandle::from_raw_handle(snapshot as RawHandle) };
        let mut entry = THREADENTRY32 {
            dwSize: size_of::<THREADENTRY32>() as u32,
            ..Default::default()
        };
        if unsafe { Thread32First(snapshot.as_raw_handle() as HANDLE, &mut entry) } == 0 {
            return Err(std::io::Error::last_os_error())
                .context("failed to read suspended Windows process threads");
        }

        let mut resumed = 0_usize;
        loop {
            if entry.th32OwnerProcessID == process_id {
                let thread = unsafe { OpenThread(THREAD_SUSPEND_RESUME, 0, entry.th32ThreadID) };
                if thread.is_null() {
                    return Err(std::io::Error::last_os_error())
                        .context("failed to open suspended Windows process thread");
                }
                let thread = unsafe { OwnedHandle::from_raw_handle(thread as RawHandle) };
                let previous_count = unsafe { ResumeThread(thread.as_raw_handle() as HANDLE) };
                if previous_count == u32::MAX {
                    return Err(std::io::Error::last_os_error())
                        .context("failed to resume suspended Windows process thread");
                }
                resumed = resumed.saturating_add(1);
            }

            if unsafe { Thread32Next(snapshot.as_raw_handle() as HANDLE, &mut entry) } == 0 {
                let error = std::io::Error::last_os_error();
                if error.raw_os_error() == Some(ERROR_NO_MORE_FILES as i32) {
                    break;
                }
                return Err(error).context("failed while enumerating Windows process threads");
            }
        }
        anyhow::ensure!(
            resumed > 0,
            "no suspended thread was found for spawned Windows process {process_id}"
        );
        Ok(())
    }

    #[cfg(test)]
    pub fn process_is_running(process_id: u32) -> Result<bool> {
        let handle = unsafe {
            OpenProcess(
                PROCESS_QUERY_LIMITED_INFORMATION | PROCESS_SYNCHRONIZE,
                0,
                process_id,
            )
        };
        if handle.is_null() {
            let error = std::io::Error::last_os_error();
            if error.raw_os_error() == Some(ERROR_INVALID_PARAMETER as i32) {
                return Ok(false);
            }
            return Err(error).context("failed to open Windows process for liveness check");
        }
        let handle = unsafe { OwnedHandle::from_raw_handle(handle as RawHandle) };
        match unsafe { WaitForSingleObject(handle.as_raw_handle() as HANDLE, 0) } {
            WAIT_OBJECT_0 => Ok(false),
            WAIT_TIMEOUT => Ok(true),
            result => anyhow::bail!(
                "WaitForSingleObject failed for process {process_id}: result={result}, error={}",
                std::io::Error::last_os_error()
            ),
        }
    }
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

    fn parent_exits_first_command(pid_file: &Path, ignore_term: bool) -> Vec<String> {
        let ready_file = pid_file.with_extension("ready");
        let ignored_signals = if ignore_term { "HUP TERM" } else { "HUP" };
        let child = format!(
            "sh -c 'trap \"\" {ignored_signals}; printf ready > \"{}\"; exec sleep 30'",
            ready_file.display()
        );
        let wait_until_ready = format!(
            "while [ ! -f '{}' ]; do sleep 0.01; done; ",
            ready_file.display()
        );
        vec![
            "sh".to_owned(),
            "-c".to_owned(),
            format!(
                "{child} >/dev/null 2>&1 & child=$!; {wait_until_ready}printf '%s %s\\n' $$ $child > '{}'; exit 0",
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
    async fn normal_parent_exit_cleans_up_background_descendants_before_returning() {
        let directory = test_directory();
        tokio::fs::create_dir_all(&directory).await.unwrap();
        let pid_file = directory.join("pids");
        let command = parent_exits_first_command(&pid_file, false);
        let (control, cancellation) = command_control();
        let output = run_unrestricted_controlled(&command, &directory, None, cancellation, None)
            .await
            .unwrap();
        drop(control);
        let process_ids = wait_for_pids(&pid_file).await;

        assert_eq!(output.status, 0);
        assert_eq!(output.termination, Termination::Exited);
        wait_until_gone([process_ids.0, process_ids.1]).await;
        tokio::fs::remove_dir_all(directory).await.unwrap();
    }

    #[tokio::test]
    async fn normal_parent_exit_force_kills_descendant_that_ignores_term() {
        let directory = test_directory();
        tokio::fs::create_dir_all(&directory).await.unwrap();
        let pid_file = directory.join("pids");
        let command = parent_exits_first_command(&pid_file, true);
        let (control, cancellation) = command_control();
        let output = run_unrestricted_controlled(&command, &directory, None, cancellation, None)
            .await
            .unwrap();
        drop(control);
        let process_ids = wait_for_pids(&pid_file).await;

        assert_eq!(output.status, 0);
        assert_eq!(
            output.termination,
            Termination::ForcedKill(StopTrigger::Completion)
        );
        wait_until_gone([process_ids.0, process_ids.1]).await;
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
        assert_eq!(output.termination, Termination::Exited);
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

#[cfg(all(test, windows))]
mod windows_process_tree_tests {
    use std::process::Stdio as StdStdio;

    use super::*;
    use crate::sandbox::windows_process_tree::process_is_running;
    use uuid::Uuid;

    const PARENT_HELPER: &str =
        "sandbox::windows_process_tree_tests::parent_exits_after_spawning_helper";
    const CHILD_HELPER: &str = "sandbox::windows_process_tree_tests::long_lived_descendant_helper";
    const PID_FILE_ENV: &str = "LOCAL_MCP_WINDOWS_TREE_PID_FILE";

    fn test_directory() -> PathBuf {
        std::env::temp_dir().join(format!("local-mcp-windows-tree-test-{}", Uuid::new_v4()))
    }

    fn cmd_set_value(value: &str) -> String {
        value.replace('%', "%%").replace('"', "\"")
    }

    #[test]
    #[ignore = "spawned only by the Windows process-tree integration test"]
    fn long_lived_descendant_helper() {
        std::thread::sleep(Duration::from_secs(30));
    }

    #[test]
    #[ignore = "spawned only by the Windows process-tree integration test"]
    fn parent_exits_after_spawning_helper() {
        let pid_file = PathBuf::from(
            std::env::var_os(PID_FILE_ENV).expect("Windows process-tree PID file is not set"),
        );
        let executable = std::env::current_exe().unwrap();
        let child = std::process::Command::new(executable)
            .args(["--ignored", "--exact", CHILD_HELPER, "--nocapture"])
            .stdin(StdStdio::null())
            .stdout(StdStdio::null())
            .stderr(StdStdio::null())
            .spawn()
            .unwrap();
        std::fs::write(pid_file, child.id().to_string()).unwrap();
        // Dropping std::process::Child does not terminate it. The helper exits
        // immediately and leaves the long-lived descendant owned by the Job.
    }

    #[tokio::test]
    async fn parent_exit_cleans_job_owned_descendant_before_returning() {
        let directory = test_directory();
        tokio::fs::create_dir_all(&directory).await.unwrap();
        let pid_file = directory.join("child.pid");
        let executable = std::env::current_exe().unwrap();
        let wrapper = directory.join("spawn-parent.cmd");
        let script = format!(
            "@echo off\r\nset \"{PID_FILE_ENV}={}\"\r\n\"{}\" --ignored --exact {PARENT_HELPER} --nocapture\r\n",
            cmd_set_value(&pid_file.to_string_lossy()),
            cmd_set_value(&executable.to_string_lossy()),
        );
        tokio::fs::write(&wrapper, script).await.unwrap();
        let command = vec![
            "cmd.exe".to_owned(),
            "/D".to_owned(),
            "/C".to_owned(),
            wrapper.to_string_lossy().into_owned(),
        ];
        let (control, cancellation) = command_control();
        let output = run_unrestricted_controlled(&command, &directory, None, cancellation, None)
            .await
            .unwrap();
        drop(control);
        let child_id = tokio::fs::read_to_string(&pid_file)
            .await
            .unwrap()
            .trim()
            .parse::<u32>()
            .unwrap();

        assert_eq!(output.status, 0);
        assert_eq!(
            output.termination,
            Termination::ForcedKill(StopTrigger::Completion)
        );
        assert!(!process_is_running(child_id).unwrap());
        tokio::fs::remove_dir_all(directory).await.unwrap();
    }

    #[tokio::test]
    async fn job_query_failure_is_returned_instead_of_normal_exit() {
        let mut job = WindowsJob::new().unwrap();
        job.invalidate_for_test();
        let mut tree = ProcessTreeGuard {
            process_id: 0,
            armed: true,
            job,
        };

        let error = tree.cleanup_after_direct_exit(false).await.unwrap_err();
        assert!(error.to_string().contains("handle is closed"));
        tree.disarm();
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
