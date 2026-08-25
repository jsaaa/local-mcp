use std::collections::HashSet;
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, OnceLock};
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

use crate::{config, sandbox};

const WORKFLOW_SCHEMA_VERSION: u32 = 1;
const MAX_IDENTIFIER_BYTES: usize = 128;
const MAX_DEPENDENCIES: usize = 128;
const MAX_PATHS: usize = 256;
const MAX_COMMAND_ITEMS: usize = 4096;

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct WorkflowStepArgs {
    pub session_id: String,
    pub step_id: String,
    #[serde(default)]
    pub depends_on: Vec<String>,
    pub idempotency_key: String,
    #[serde(default)]
    pub required_files: Vec<String>,
    #[serde(default)]
    pub expected_outputs: Vec<String>,
    pub command: Vec<String>,
    #[serde(default)]
    pub continue_on_error: bool,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum StepStatus {
    Pending,
    Running,
    Succeeded,
    Failed,
    Blocked,
}

impl StepStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Running => "running",
            Self::Succeeded => "succeeded",
            Self::Failed => "failed",
            Self::Blocked => "blocked",
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct WorkflowInvocation {
    step_id: String,
    depends_on: Vec<String>,
    required_files: Vec<String>,
    expected_outputs: Vec<String>,
    command: Vec<String>,
    continue_on_error: bool,
}

impl From<&WorkflowStepArgs> for WorkflowInvocation {
    fn from(args: &WorkflowStepArgs) -> Self {
        Self {
            step_id: args.step_id.clone(),
            depends_on: args.depends_on.clone(),
            required_files: args.required_files.clone(),
            expected_outputs: args.expected_outputs.clone(),
            command: args.command.clone(),
            continue_on_error: args.continue_on_error,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct StepTransition {
    pub status: StepStatus,
    pub at_ms: u64,
    pub detail: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct StepRecord {
    version: u32,
    session_id: String,
    step_id: String,
    idempotency_key: String,
    invocation: WorkflowInvocation,
    runner_instance_id: String,
    status: StepStatus,
    continue_on_error: bool,
    created_at_ms: u64,
    updated_at_ms: u64,
    finished_at_ms: Option<u64>,
    exit_code: Option<i32>,
    stdout: String,
    stderr: String,
    error: Option<String>,
    blocked_by: Vec<String>,
    missing_required_files: Vec<String>,
    missing_expected_outputs: Vec<String>,
    history: Vec<StepTransition>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct WorkflowStepResult {
    pub schema_version: u32,
    pub step_id: String,
    pub idempotency_key: String,
    pub status: StepStatus,
    pub reused: bool,
    pub continue_on_error: bool,
    pub created_at_ms: Option<u64>,
    pub updated_at_ms: Option<u64>,
    pub finished_at_ms: Option<u64>,
    pub exit_code: Option<i32>,
    pub stdout: String,
    pub stderr: String,
    pub error: Option<String>,
    pub blocked_by: Vec<String>,
    pub missing_required_files: Vec<String>,
    pub missing_expected_outputs: Vec<String>,
    pub history: Vec<StepTransition>,
}

impl StepRecord {
    fn result(&self, reused: bool) -> WorkflowStepResult {
        WorkflowStepResult {
            schema_version: WORKFLOW_SCHEMA_VERSION,
            step_id: self.step_id.clone(),
            idempotency_key: self.idempotency_key.clone(),
            status: self.status,
            reused,
            continue_on_error: self.continue_on_error,
            created_at_ms: Some(self.created_at_ms),
            updated_at_ms: Some(self.updated_at_ms),
            finished_at_ms: self.finished_at_ms,
            exit_code: self.exit_code,
            stdout: self.stdout.clone(),
            stderr: self.stderr.clone(),
            error: self.error.clone(),
            blocked_by: self.blocked_by.clone(),
            missing_required_files: self.missing_required_files.clone(),
            missing_expected_outputs: self.missing_expected_outputs.clone(),
            history: self.history.clone(),
        }
    }
}

impl WorkflowStepResult {
    fn conflict(args: &WorkflowStepArgs, message: String) -> Self {
        Self {
            schema_version: WORKFLOW_SCHEMA_VERSION,
            step_id: args.step_id.clone(),
            idempotency_key: args.idempotency_key.clone(),
            status: StepStatus::Blocked,
            reused: false,
            continue_on_error: args.continue_on_error,
            created_at_ms: None,
            updated_at_ms: None,
            finished_at_ms: None,
            exit_code: None,
            stdout: String::new(),
            stderr: String::new(),
            error: Some(message),
            blocked_by: Vec::new(),
            missing_required_files: Vec::new(),
            missing_expected_outputs: Vec::new(),
            history: Vec::new(),
        }
    }
}

enum Reservation {
    Reused(StepRecord),
    Blocked(StepRecord),
    Launch(StepRecord),
    Conflict(WorkflowStepResult),
}

fn runner_instance_id() -> &'static str {
    static INSTANCE_ID: OnceLock<String> = OnceLock::new();
    INSTANCE_ID
        .get_or_init(|| Uuid::new_v4().to_string())
        .as_str()
}

fn journal_lock() -> &'static Mutex<()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
}

pub async fn run(args: &Value, session: &config::Session) -> Result<WorkflowStepResult> {
    #[cfg(windows)]
    {
        let _ = (args, session);
        anyhow::bail!(
            "run_workflow_step is unsupported on Windows because this release cannot provide a filesystem/network sandbox there"
        )
    }
    #[cfg(not(windows))]
    {
        run_with_mode(args, session, WorkflowExecutionMode::Sandboxed).await
    }
}

#[derive(Clone, Copy)]
enum WorkflowExecutionMode {
    #[cfg(not(windows))]
    Sandboxed,
    #[cfg(test)]
    Unrestricted,
}

#[cfg(test)]
async fn run_for_tests(args: &Value, session: &config::Session) -> Result<WorkflowStepResult> {
    run_with_mode(args, session, WorkflowExecutionMode::Unrestricted).await
}

async fn run_with_mode(
    args: &Value,
    session: &config::Session,
    execution_mode: WorkflowExecutionMode,
) -> Result<WorkflowStepResult> {
    let args: WorkflowStepArgs =
        serde_json::from_value(args.clone()).context("invalid run_workflow_step arguments")?;
    anyhow::ensure!(
        args.session_id == session.id,
        "session_id does not match the loaded session"
    );
    validate_args(&args)?;
    let invocation = WorkflowInvocation::from(&args);

    let reservation = {
        let _guard = journal_lock().lock().unwrap();
        reserve_locked(&args, &invocation, session)?
    };
    let record = match reservation {
        Reservation::Reused(record) => return Ok(record.result(true)),
        Reservation::Blocked(record) => return Ok(record.result(false)),
        Reservation::Conflict(result) => return Ok(result),
        Reservation::Launch(record) => record,
    };

    let mut roots = session.permitted_directories.clone();
    if !roots.iter().any(|root| session.cwd.starts_with(root)) {
        roots.push(session.cwd.clone());
    }
    let output = match execution_mode {
        #[cfg(not(windows))]
        WorkflowExecutionMode::Sandboxed => {
            sandbox::run(&args.command, &session.cwd, &roots, None).await
        }
        #[cfg(test)]
        WorkflowExecutionMode::Unrestricted => {
            sandbox::run_unrestricted(&args.command, &session.cwd, None).await
        }
    };
    let missing_expected_outputs = if output.as_ref().is_ok_and(|output| output.status == 0) {
        missing_files(&session.cwd, &args.expected_outputs)
    } else {
        Vec::new()
    };

    let final_record = {
        let _guard = journal_lock().lock().unwrap();
        let mut current = load_required_locked(&session.id, &record.step_id)?;
        anyhow::ensure!(
            current.idempotency_key == record.idempotency_key
                && current.invocation == record.invocation,
            "workflow step journal changed while the command was running"
        );
        anyhow::ensure!(
            current.status == StepStatus::Running,
            "workflow step is no longer running"
        );
        let now = now_ms();
        current.updated_at_ms = now;
        current.finished_at_ms = Some(now);
        match output {
            Ok(output) => {
                current.exit_code = Some(output.status);
                current.stdout = output.stdout;
                current.stderr = output.stderr;
                if output.status != 0 {
                    current.status = StepStatus::Failed;
                    current.error = Some(format!("command exited with status {}", output.status));
                } else if !missing_expected_outputs.is_empty() {
                    current.status = StepStatus::Failed;
                    current.missing_expected_outputs = missing_expected_outputs;
                    current.error = Some(
                        "command exited successfully but expected outputs were not created"
                            .to_owned(),
                    );
                } else {
                    current.status = StepStatus::Succeeded;
                    current.error = None;
                }
            }
            Err(error) => {
                current.status = StepStatus::Failed;
                current.error = Some(format!("command could not be executed: {error:#}"));
            }
        }
        current.history.push(StepTransition {
            status: current.status,
            at_ms: now,
            detail: current.error.clone(),
        });
        publish_locked(&current)?;
        current
    };

    Ok(final_record.result(false))
}

fn reserve_locked(
    args: &WorkflowStepArgs,
    invocation: &WorkflowInvocation,
    session: &config::Session,
) -> Result<Reservation> {
    cleanup_temporary_files_locked(&session.id)?;

    if let Some(existing) = find_by_idempotency_locked(&session.id, &args.idempotency_key)? {
        if existing.invocation == *invocation {
            return Ok(Reservation::Reused(existing));
        }
        return Ok(Reservation::Conflict(WorkflowStepResult::conflict(
            args,
            format!(
                "idempotency key {:?} already belongs to step {:?} with a different invocation",
                args.idempotency_key, existing.step_id
            ),
        )));
    }

    if let Some(existing) = load_optional_locked(&session.id, &args.step_id)? {
        return Ok(Reservation::Conflict(WorkflowStepResult::conflict(
            args,
            format!(
                "step_id {:?} already exists with idempotency key {:?}",
                args.step_id, existing.idempotency_key
            ),
        )));
    }

    let mut blocked_by = Vec::new();
    for dependency in &args.depends_on {
        match load_optional_locked(&session.id, dependency)? {
            None => blocked_by.push(format!("{dependency}: missing")),
            Some(record) if dependency_satisfied(&record) => {}
            Some(record) => blocked_by.push(format!("{dependency}: {}", record.status.as_str())),
        }
    }
    let missing_required_files = missing_files(&session.cwd, &args.required_files);
    let now = now_ms();
    let mut record = StepRecord {
        version: WORKFLOW_SCHEMA_VERSION,
        session_id: session.id.clone(),
        step_id: args.step_id.clone(),
        idempotency_key: args.idempotency_key.clone(),
        invocation: invocation.clone(),
        runner_instance_id: runner_instance_id().to_owned(),
        status: StepStatus::Pending,
        continue_on_error: args.continue_on_error,
        created_at_ms: now,
        updated_at_ms: now,
        finished_at_ms: None,
        exit_code: None,
        stdout: String::new(),
        stderr: String::new(),
        error: None,
        blocked_by,
        missing_required_files,
        missing_expected_outputs: Vec::new(),
        history: vec![StepTransition {
            status: StepStatus::Pending,
            at_ms: now,
            detail: None,
        }],
    };

    if !record.blocked_by.is_empty() || !record.missing_required_files.is_empty() {
        record.status = StepStatus::Blocked;
        record.finished_at_ms = Some(now);
        record.error = Some(block_reason(&record));
        record.history.push(StepTransition {
            status: StepStatus::Blocked,
            at_ms: now,
            detail: record.error.clone(),
        });
        publish_locked(&record)?;
        return Ok(Reservation::Blocked(record));
    }

    // Publish pending and then running before releasing the reservation lock.
    // A concurrent retry can therefore observe and reuse the reservation, and
    // no process starts unless the running state was durably published.
    publish_locked(&record)?;
    record.status = StepStatus::Running;
    record.updated_at_ms = now_ms();
    record.history.push(StepTransition {
        status: StepStatus::Running,
        at_ms: record.updated_at_ms,
        detail: None,
    });
    publish_locked(&record)?;
    Ok(Reservation::Launch(record))
}

fn dependency_satisfied(record: &StepRecord) -> bool {
    record.status == StepStatus::Succeeded
        || (record.status == StepStatus::Failed && record.continue_on_error)
}

fn block_reason(record: &StepRecord) -> String {
    let mut reasons = Vec::new();
    if !record.blocked_by.is_empty() {
        reasons.push(format!(
            "unsatisfied dependencies: {}",
            record.blocked_by.join(", ")
        ));
    }
    if !record.missing_required_files.is_empty() {
        reasons.push(format!(
            "missing required files: {}",
            record.missing_required_files.join(", ")
        ));
    }
    reasons.join("; ")
}

fn validate_args(args: &WorkflowStepArgs) -> Result<()> {
    validate_identifier("step_id", &args.step_id)?;
    validate_identifier("idempotency_key", &args.idempotency_key)?;
    anyhow::ensure!(
        args.depends_on.len() <= MAX_DEPENDENCIES,
        "depends_on may contain at most {MAX_DEPENDENCIES} entries"
    );
    anyhow::ensure!(
        args.required_files.len() <= MAX_PATHS,
        "required_files may contain at most {MAX_PATHS} entries"
    );
    anyhow::ensure!(
        args.expected_outputs.len() <= MAX_PATHS,
        "expected_outputs may contain at most {MAX_PATHS} entries"
    );
    anyhow::ensure!(
        !args.command.is_empty() && !args.command[0].is_empty(),
        "command must contain a non-empty executable"
    );
    anyhow::ensure!(
        args.command.len() <= MAX_COMMAND_ITEMS,
        "command may contain at most {MAX_COMMAND_ITEMS} entries"
    );

    let mut dependencies = HashSet::new();
    for dependency in &args.depends_on {
        validate_identifier("dependency", dependency)?;
        anyhow::ensure!(
            dependency != &args.step_id,
            "a step cannot depend on itself"
        );
        anyhow::ensure!(
            dependencies.insert(dependency),
            "depends_on contains duplicate step {dependency:?}"
        );
    }
    validate_unique_paths("required_files", &args.required_files)?;
    validate_unique_paths("expected_outputs", &args.expected_outputs)?;
    Ok(())
}

fn validate_identifier(name: &str, value: &str) -> Result<()> {
    anyhow::ensure!(!value.is_empty(), "{name} must not be empty");
    anyhow::ensure!(
        value.len() <= MAX_IDENTIFIER_BYTES,
        "{name} must be at most {MAX_IDENTIFIER_BYTES} bytes"
    );
    anyhow::ensure!(
        value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.')),
        "{name} may contain only ASCII letters, digits, '.', '_' and '-'"
    );
    Ok(())
}

fn validate_unique_paths(name: &str, paths: &[String]) -> Result<()> {
    let mut unique = HashSet::new();
    for path in paths {
        anyhow::ensure!(!path.is_empty(), "{name} entries must not be empty");
        anyhow::ensure!(
            unique.insert(path),
            "{name} contains duplicate path {path:?}"
        );
    }
    Ok(())
}

fn resolve_path(cwd: &Path, path: &str) -> PathBuf {
    let path = PathBuf::from(path);
    if path.is_absolute() {
        path
    } else {
        cwd.join(path)
    }
}

fn missing_files(cwd: &Path, paths: &[String]) -> Vec<String> {
    paths
        .iter()
        .filter(|path| {
            fs::metadata(resolve_path(cwd, path))
                .map(|metadata| !metadata.is_file())
                .unwrap_or(true)
        })
        .cloned()
        .collect()
}

fn session_root(session_id: &str) -> Result<PathBuf> {
    config::validate_session_id(session_id)?;
    Ok(config::state_dir()?.join("workflow-steps").join(session_id))
}

fn step_root(session_id: &str, step_id: &str) -> Result<PathBuf> {
    validate_identifier("step_id", step_id)?;
    Ok(session_root(session_id)?.join(step_id))
}

fn publish_locked(record: &StepRecord) -> Result<()> {
    anyhow::ensure!(
        record.version == WORKFLOW_SCHEMA_VERSION,
        "unsupported workflow journal version"
    );
    config::validate_session_id(&record.session_id)?;
    validate_identifier("step_id", &record.step_id)?;
    let directory = step_root(&record.session_id, &record.step_id)?;
    fs::create_dir_all(&directory)?;
    let base = format!("{:039}-{}", now_nanos(), Uuid::new_v4());
    let temporary = directory.join(format!(".{base}.tmp"));
    let published = directory.join(format!("{base}.json"));
    let bytes = serde_json::to_vec_pretty(record)?;
    let mut file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&temporary)?;
    file.write_all(&bytes)?;
    file.sync_all()?;
    fs::rename(&temporary, &published)?;
    sync_directory(&directory);

    for entry in fs::read_dir(&directory)? {
        let path = entry?.path();
        if path != published && path.extension().and_then(|value| value.to_str()) == Some("json") {
            let _ = fs::remove_file(path);
        }
    }
    Ok(())
}

fn load_optional_locked(session_id: &str, step_id: &str) -> Result<Option<StepRecord>> {
    let directory = step_root(session_id, step_id)?;
    let entries = match fs::read_dir(&directory) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    let mut snapshots = entries
        .filter_map(|entry| entry.ok())
        .map(|entry| entry.path())
        .filter(|path| path.extension().and_then(|value| value.to_str()) == Some("json"))
        .collect::<Vec<_>>();
    snapshots.sort();
    let Some(path) = snapshots.pop() else {
        return Ok(None);
    };
    let bytes = fs::read(&path)
        .with_context(|| format!("cannot read workflow journal {}", path.display()))?;
    let record: StepRecord = serde_json::from_slice(&bytes)
        .with_context(|| format!("corrupt workflow journal {}", path.display()))?;
    anyhow::ensure!(
        record.version == WORKFLOW_SCHEMA_VERSION,
        "unsupported workflow journal version"
    );
    anyhow::ensure!(record.session_id == session_id, "workflow session mismatch");
    anyhow::ensure!(record.step_id == step_id, "workflow step ID mismatch");
    if matches!(record.status, StepStatus::Pending | StepStatus::Running)
        && record.runner_instance_id != runner_instance_id()
    {
        let mut record = record;
        let now = now_ms();
        record.status = StepStatus::Failed;
        record.updated_at_ms = now;
        record.finished_at_ms = Some(now);
        record.error = Some(
            "workflow runner restarted before this step reached a terminal state; the command is not assumed to be running"
                .to_owned(),
        );
        record.history.push(StepTransition {
            status: StepStatus::Failed,
            at_ms: now,
            detail: record.error.clone(),
        });
        publish_locked(&record)?;
        return Ok(Some(record));
    }
    Ok(Some(record))
}

fn load_required_locked(session_id: &str, step_id: &str) -> Result<StepRecord> {
    load_optional_locked(session_id, step_id)?
        .with_context(|| format!("workflow step {step_id:?} disappeared"))
}

fn find_by_idempotency_locked(
    session_id: &str,
    idempotency_key: &str,
) -> Result<Option<StepRecord>> {
    let root = session_root(session_id)?;
    let entries = match fs::read_dir(&root) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    for entry in entries {
        let entry = entry?;
        if !entry.file_type()?.is_dir() {
            continue;
        }
        let step_id = entry.file_name().to_string_lossy().into_owned();
        let Some(record) = load_optional_locked(session_id, &step_id)? else {
            continue;
        };
        if record.idempotency_key == idempotency_key {
            return Ok(Some(record));
        }
    }
    Ok(None)
}

fn cleanup_temporary_files_locked(session_id: &str) -> Result<()> {
    let root = session_root(session_id)?;
    let entries = match fs::read_dir(&root) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error.into()),
    };
    for entry in entries {
        let entry = entry?;
        if !entry.file_type()?.is_dir() {
            continue;
        }
        for child in fs::read_dir(entry.path())? {
            let path = child?.path();
            if path.extension().and_then(|value| value.to_str()) == Some("tmp") {
                let _ = fs::remove_file(path);
            }
        }
    }
    Ok(())
}

fn sync_directory(path: &Path) {
    if let Ok(directory) = File::open(path) {
        let _ = directory.sync_all();
    }
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(u64::MAX as u128) as u64
}

fn now_nanos() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos()
}

#[cfg(test)]
pub fn remove_session_for_tests(session_id: &str) {
    if let Ok(path) = session_root(session_id) {
        let _ = fs::remove_dir_all(path);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn session(directory: &Path) -> config::Session {
        config::Session {
            id: format!("workflow-test-{}", Uuid::new_v4()),
            cwd: directory.to_owned(),
            permitted_directories: vec![directory.to_owned()],
        }
    }

    fn args(session: &config::Session, step_id: &str, command: Vec<String>) -> Value {
        serde_json::json!({
            "session_id": session.id,
            "step_id": step_id,
            "idempotency_key": format!("{step_id}-v1"),
            "command": command
        })
    }

    #[cfg(unix)]
    fn successful_command() -> Vec<String> {
        vec!["sh".to_owned(), "-c".to_owned(), "exit 0".to_owned()]
    }

    #[cfg(windows)]
    fn successful_command() -> Vec<String> {
        vec!["cmd.exe".to_owned(), "/C".to_owned(), "exit 0".to_owned()]
    }

    #[cfg(unix)]
    fn failing_command() -> Vec<String> {
        vec!["sh".to_owned(), "-c".to_owned(), "exit 7".to_owned()]
    }

    #[cfg(windows)]
    fn failing_command() -> Vec<String> {
        vec!["cmd.exe".to_owned(), "/C".to_owned(), "exit 7".to_owned()]
    }

    #[cfg(unix)]
    fn write_command(path: &Path, append: bool) -> Vec<String> {
        let operator = if append { ">>" } else { ">" };
        vec![
            "sh".to_owned(),
            "-c".to_owned(),
            format!("printf 'ran\\n' {operator} '{}'", path.display()),
        ]
    }

    #[cfg(windows)]
    fn write_command(path: &Path, append: bool) -> Vec<String> {
        let path = path.to_string_lossy().replace('\'', "''");
        let operation = if append { "Add-Content" } else { "Set-Content" };
        vec![
            "powershell.exe".to_owned(),
            "-NoProfile".to_owned(),
            "-Command".to_owned(),
            format!("{operation} -LiteralPath '{path}' -Value ran"),
        ]
    }

    #[test]
    fn continue_on_error_defaults_to_false_and_unknown_fields_fail() {
        let value = serde_json::json!({
            "session_id": "example",
            "step_id": "test",
            "idempotency_key": "test-v1",
            "command": ["true"]
        });
        let parsed: WorkflowStepArgs = serde_json::from_value(value).unwrap();
        assert!(!parsed.continue_on_error);
        let invalid = serde_json::json!({
            "session_id": "example",
            "step_id": "test",
            "idempotency_key": "test-v1",
            "command": ["true"],
            "unexpected": true
        });
        assert!(serde_json::from_value::<WorkflowStepArgs>(invalid).is_err());
    }

    #[tokio::test]
    async fn missing_dependency_blocks_before_process_spawn() {
        let directory = std::env::temp_dir().join(format!("local-mcp-workflow-{}", Uuid::new_v4()));
        tokio::fs::create_dir_all(&directory).await.unwrap();
        let session = session(&directory);
        let marker = directory.join("marker");
        let mut request = args(&session, "dependent", write_command(&marker, false));
        request["depends_on"] = serde_json::json!(["missing-step"]);

        let result = run_for_tests(&request, &session).await.unwrap();
        assert_eq!(result.status, StepStatus::Blocked);
        assert!(!marker.exists());
        assert_eq!(result.history[0].status, StepStatus::Pending);
        assert_eq!(result.history[1].status, StepStatus::Blocked);
        remove_session_for_tests(&session.id);
        tokio::fs::remove_dir_all(directory).await.unwrap();
    }

    #[tokio::test]
    async fn failed_prerequisite_blocks_every_dependent_side_effect() {
        let directory = std::env::temp_dir().join(format!("local-mcp-workflow-{}", Uuid::new_v4()));
        tokio::fs::create_dir_all(&directory).await.unwrap();
        let session = session(&directory);
        let prerequisite = args(&session, "focused-tests", failing_command());
        let failed = run_for_tests(&prerequisite, &session).await.unwrap();
        assert_eq!(failed.status, StepStatus::Failed);

        for index in 0..3 {
            let marker = directory.join(format!("dependent-{index}"));
            let step_id = format!("dependent-{index}");
            let mut request = args(&session, &step_id, write_command(&marker, false));
            request["depends_on"] = serde_json::json!(["focused-tests"]);
            let result = run_for_tests(&request, &session).await.unwrap();
            assert_eq!(result.status, StepStatus::Blocked);
            assert!(!marker.exists());
        }
        remove_session_for_tests(&session.id);
        tokio::fs::remove_dir_all(directory).await.unwrap();
    }

    #[tokio::test]
    async fn missing_required_file_blocks_before_process_spawn() {
        let directory = std::env::temp_dir().join(format!("local-mcp-workflow-{}", Uuid::new_v4()));
        tokio::fs::create_dir_all(&directory).await.unwrap();
        let session = session(&directory);
        let marker = directory.join("marker");
        let mut request = args(&session, "required-gate", write_command(&marker, false));
        request["required_files"] = serde_json::json!(["missing.ok"]);

        let result = run_for_tests(&request, &session).await.unwrap();
        assert_eq!(result.status, StepStatus::Blocked);
        assert_eq!(result.missing_required_files, vec!["missing.ok"]);
        assert!(!marker.exists());
        remove_session_for_tests(&session.id);
        tokio::fs::remove_dir_all(directory).await.unwrap();
    }

    #[tokio::test]
    async fn successful_process_without_expected_output_is_failed() {
        let directory = std::env::temp_dir().join(format!("local-mcp-workflow-{}", Uuid::new_v4()));
        tokio::fs::create_dir_all(&directory).await.unwrap();
        let session = session(&directory);
        let mut request = args(&session, "missing-output", successful_command());
        request["expected_outputs"] = serde_json::json!(["expected.json"]);

        let result = run_for_tests(&request, &session).await.unwrap();
        assert_eq!(result.status, StepStatus::Failed);
        assert_eq!(result.exit_code, Some(0));
        assert_eq!(result.missing_expected_outputs, vec!["expected.json"]);
        remove_session_for_tests(&session.id);
        tokio::fs::remove_dir_all(directory).await.unwrap();
    }

    #[tokio::test]
    async fn idempotency_reuses_prior_result_and_conflicts_deterministically() {
        let directory = std::env::temp_dir().join(format!("local-mcp-workflow-{}", Uuid::new_v4()));
        tokio::fs::create_dir_all(&directory).await.unwrap();
        let session = session(&directory);
        let marker = directory.join("count");
        let request = args(&session, "idempotent", write_command(&marker, true));

        let first = run_for_tests(&request, &session).await.unwrap();
        let second = run_for_tests(&request, &session).await.unwrap();
        assert_eq!(first.status, StepStatus::Succeeded);
        assert_eq!(second.status, StepStatus::Succeeded);
        assert!(second.reused);
        let lines = tokio::fs::read_to_string(&marker).await.unwrap();
        assert_eq!(lines.lines().count(), 1);

        let conflict_marker = directory.join("conflict");
        let mut conflict = request.clone();
        conflict["command"] = serde_json::json!(write_command(&conflict_marker, false));
        let first_conflict = run_for_tests(&conflict, &session).await.unwrap();
        let second_conflict = run_for_tests(&conflict, &session).await.unwrap();
        assert_eq!(first_conflict, second_conflict);
        assert_eq!(first_conflict.status, StepStatus::Blocked);
        assert!(
            first_conflict
                .error
                .unwrap()
                .contains("different invocation")
        );
        assert!(!conflict_marker.exists());
        remove_session_for_tests(&session.id);
        tokio::fs::remove_dir_all(directory).await.unwrap();
    }

    #[tokio::test]
    async fn successful_dependency_and_required_gate_allow_execution() {
        let directory = std::env::temp_dir().join(format!("local-mcp-workflow-{}", Uuid::new_v4()));
        tokio::fs::create_dir_all(&directory).await.unwrap();
        let session = session(&directory);
        let gate = directory.join("focused.ok");
        let marker = directory.join("accepted");
        let mut prerequisite = args(&session, "focused-tests", write_command(&gate, false));
        prerequisite["expected_outputs"] = serde_json::json!(["focused.ok"]);
        assert_eq!(
            run_for_tests(&prerequisite, &session).await.unwrap().status,
            StepStatus::Succeeded
        );

        let mut dependent = args(&session, "acceptance", write_command(&marker, false));
        dependent["depends_on"] = serde_json::json!(["focused-tests"]);
        dependent["required_files"] = serde_json::json!(["focused.ok"]);
        dependent["expected_outputs"] = serde_json::json!(["accepted"]);
        let result = run_for_tests(&dependent, &session).await.unwrap();
        assert_eq!(result.status, StepStatus::Succeeded);
        assert!(marker.is_file());
        assert_eq!(
            result
                .history
                .iter()
                .map(|transition| transition.status)
                .collect::<Vec<_>>(),
            vec![
                StepStatus::Pending,
                StepStatus::Running,
                StepStatus::Succeeded
            ]
        );
        remove_session_for_tests(&session.id);
        tokio::fs::remove_dir_all(directory).await.unwrap();
    }

    #[tokio::test]
    async fn journal_keeps_one_complete_atomic_snapshot() {
        let directory = std::env::temp_dir().join(format!("local-mcp-workflow-{}", Uuid::new_v4()));
        tokio::fs::create_dir_all(&directory).await.unwrap();
        let session = session(&directory);
        let request = args(&session, "atomic-step", successful_command());
        let result = run_for_tests(&request, &session).await.unwrap();
        assert_eq!(result.status, StepStatus::Succeeded);

        let step_directory = step_root(&session.id, "atomic-step").unwrap();
        let entries = fs::read_dir(&step_directory)
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .collect::<Vec<_>>();
        let snapshots = entries
            .iter()
            .filter(|path| path.extension().and_then(|value| value.to_str()) == Some("json"))
            .collect::<Vec<_>>();
        assert_eq!(snapshots.len(), 1);
        assert!(
            entries
                .iter()
                .all(|path| path.extension().and_then(|value| value.to_str()) != Some("tmp"))
        );
        let record: StepRecord = serde_json::from_slice(&fs::read(snapshots[0]).unwrap()).unwrap();
        assert_eq!(record.status, StepStatus::Succeeded);
        assert_eq!(
            record
                .history
                .iter()
                .map(|entry| entry.status)
                .collect::<Vec<_>>(),
            vec![
                StepStatus::Pending,
                StepStatus::Running,
                StepStatus::Succeeded
            ]
        );

        remove_session_for_tests(&session.id);
        tokio::fs::remove_dir_all(directory).await.unwrap();
    }

    #[test]
    fn stale_incomplete_record_is_failed_closed_after_runner_restart() {
        let directory = std::env::temp_dir().join(format!("local-mcp-workflow-{}", Uuid::new_v4()));
        fs::create_dir_all(&directory).unwrap();
        let session = session(&directory);
        let value = args(&session, "stale-step", successful_command());
        let parsed: WorkflowStepArgs = serde_json::from_value(value).unwrap();
        let invocation = WorkflowInvocation::from(&parsed);
        let _guard = journal_lock().lock().unwrap();
        let Reservation::Launch(mut record) =
            reserve_locked(&parsed, &invocation, &session).unwrap()
        else {
            panic!("expected launch reservation");
        };
        record.runner_instance_id = "previous-runner".to_owned();
        publish_locked(&record).unwrap();
        let recovered = load_required_locked(&session.id, "stale-step").unwrap();
        assert_eq!(recovered.status, StepStatus::Failed);
        assert!(
            recovered
                .error
                .as_deref()
                .unwrap()
                .contains("runner restarted")
        );
        drop(_guard);
        remove_session_for_tests(&session.id);
        fs::remove_dir_all(directory).unwrap();
    }

    #[tokio::test]
    async fn journal_is_session_scoped() {
        let directory = std::env::temp_dir().join(format!("local-mcp-workflow-{}", Uuid::new_v4()));
        tokio::fs::create_dir_all(&directory).await.unwrap();
        let first = session(&directory);
        let second = session(&directory);
        let request = args(&first, "shared-name", successful_command());
        assert_eq!(
            run_for_tests(&request, &first).await.unwrap().status,
            StepStatus::Succeeded
        );

        let mut second_request = args(&second, "dependent", successful_command());
        second_request["depends_on"] = serde_json::json!(["shared-name"]);
        let result = run_for_tests(&second_request, &second).await.unwrap();
        assert_eq!(result.status, StepStatus::Blocked);
        assert!(result.blocked_by[0].contains("missing"));
        remove_session_for_tests(&first.id);
        remove_session_for_tests(&second.id);
        tokio::fs::remove_dir_all(directory).await.unwrap();
    }

    #[tokio::test]
    async fn explicit_continue_on_error_allows_a_failed_gate() {
        let directory = std::env::temp_dir().join(format!("local-mcp-workflow-{}", Uuid::new_v4()));
        tokio::fs::create_dir_all(&directory).await.unwrap();
        let session = session(&directory);
        let marker = directory.join("continued");
        let mut prerequisite = args(&session, "optional-check", failing_command());
        prerequisite["continue_on_error"] = Value::Bool(true);
        assert_eq!(
            run_for_tests(&prerequisite, &session).await.unwrap().status,
            StepStatus::Failed
        );

        let mut dependent = args(&session, "continued-step", write_command(&marker, false));
        dependent["depends_on"] = serde_json::json!(["optional-check"]);
        assert_eq!(
            run_for_tests(&dependent, &session).await.unwrap().status,
            StepStatus::Succeeded
        );
        assert!(marker.is_file());
        remove_session_for_tests(&session.id);
        tokio::fs::remove_dir_all(directory).await.unwrap();
    }
}
