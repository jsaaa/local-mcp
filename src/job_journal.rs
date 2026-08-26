use std::collections::HashSet;
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

use crate::config;

const JOURNAL_VERSION: u32 = 1;
const MAX_JOBS_PER_SESSION: usize = 256;
const RETENTION_AGE: Duration = Duration::from_secs(30 * 24 * 60 * 60);
const MAX_COMMAND_BYTES: usize = 8 * 1024;
pub const MAX_PERSISTED_FIELD_SERIALIZED_BYTES: usize = 64 * 1024;
pub const MAX_JOURNAL_SNAPSHOT_BYTES: u64 = 128 * 1024;

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum JobState {
    Running,
    Completed,
    Failed,
    Stopped,
    Orphaned,
}

impl JobState {
    pub fn parse(value: &str) -> Result<Self> {
        match value {
            "running" => Ok(Self::Running),
            "completed" => Ok(Self::Completed),
            "failed" => Ok(Self::Failed),
            "stopped" => Ok(Self::Stopped),
            "orphaned" => Ok(Self::Orphaned),
            _ => anyhow::bail!("unknown job state: {value}"),
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Running => "running",
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::Stopped => "stopped",
            Self::Orphaned => "orphaned",
        }
    }

    fn terminal(self) -> bool {
        self != Self::Running
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionMode {
    Sandboxed,
    Unrestricted,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct JobRecord {
    pub version: u32,
    pub job_id: String,
    pub session_id: String,
    pub command: String,
    pub cwd: PathBuf,
    pub execution_mode: ExecutionMode,
    pub state: JobState,
    pub created_at_ms: u64,
    pub started_at_ms: u64,
    pub updated_at_ms: u64,
    pub finished_at_ms: Option<u64>,
    pub exit_code: Option<i32>,
    pub result: Option<String>,
    pub error: Option<String>,
    pub server_pid: u32,
    pub process_id: Option<u32>,
    pub reattachable: bool,
}

#[derive(Debug, Serialize)]
pub struct JobSummary {
    pub job_id: String,
    pub command: String,
    pub cwd: PathBuf,
    pub execution_mode: ExecutionMode,
    pub state: JobState,
    pub created_at_ms: u64,
    pub updated_at_ms: u64,
    pub finished_at_ms: Option<u64>,
    pub exit_code: Option<i32>,
    pub result_available: bool,
    pub error_available: bool,
    pub process_id: Option<u32>,
    pub reattachable: bool,
}

impl From<&JobRecord> for JobSummary {
    fn from(record: &JobRecord) -> Self {
        Self {
            job_id: record.job_id.clone(),
            command: record.command.clone(),
            cwd: record.cwd.clone(),
            execution_mode: record.execution_mode,
            state: record.state,
            created_at_ms: record.created_at_ms,
            updated_at_ms: record.updated_at_ms,
            finished_at_ms: record.finished_at_ms,
            exit_code: record.exit_code,
            result_available: record.result.is_some(),
            error_available: record.error.is_some(),
            process_id: record.process_id,
            reattachable: record.reattachable,
        }
    }
}

#[derive(Debug, Serialize)]
pub struct ListPage {
    pub jobs: Vec<JobSummary>,
    pub total: usize,
    pub offset: usize,
    pub next_offset: Option<usize>,
    pub corrupt_entries: usize,
}

fn journal_lock() -> &'static Mutex<()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
}

pub fn create_running(
    session_id: &str,
    job_id: Uuid,
    command: String,
    cwd: PathBuf,
    execution_mode: ExecutionMode,
) -> Result<JobRecord> {
    let _guard = journal_lock().lock().unwrap();
    cleanup_locked(
        session_id,
        MAX_JOBS_PER_SESSION.saturating_sub(1),
        RETENTION_AGE,
    )?;
    let retained = count_job_directories_locked(session_id)?;
    anyhow::ensure!(
        retained < MAX_JOBS_PER_SESSION,
        "job journal limit reached: {MAX_JOBS_PER_SESSION} records are already retained for this session"
    );

    let now = now_ms();
    let record = JobRecord {
        version: JOURNAL_VERSION,
        job_id: job_id.to_string(),
        session_id: session_id.to_owned(),
        command: bound_plain_text(&command, MAX_COMMAND_BYTES),
        cwd,
        execution_mode,
        state: JobState::Running,
        created_at_ms: now,
        started_at_ms: now,
        updated_at_ms: now,
        finished_at_ms: None,
        exit_code: None,
        result: None,
        error: None,
        server_pid: std::process::id(),
        process_id: None,
        reattachable: false,
    };
    publish_locked(&record)?;
    Ok(record)
}

pub fn finish(session_id: &str, job_id: Uuid, result: &Result<String>) -> Result<JobRecord> {
    let _guard = journal_lock().lock().unwrap();
    let mut record = load_latest_locked(session_id, job_id)?;
    if record.state.terminal() {
        return Ok(record);
    }
    let now = now_ms();
    record.updated_at_ms = now;
    record.finished_at_ms = Some(now);
    match result {
        Ok(text) => {
            record.state = JobState::Completed;
            record.exit_code = parse_exit_code(text).or(Some(0));
            record.result = Some(bound_persisted_payload(text, record.exit_code, "result"));
            record.error = None;
        }
        Err(error) => {
            let text = error.to_string();
            record.state = JobState::Failed;
            record.exit_code = parse_exit_code(&text);
            record.result = None;
            record.error = Some(bound_persisted_payload(&text, record.exit_code, "error"));
        }
    }
    publish_locked(&record)?;
    Ok(record)
}

pub fn mark_stopped(session_id: &str, job_id: Uuid) -> Result<JobRecord> {
    let _guard = journal_lock().lock().unwrap();
    let mut record = load_latest_locked(session_id, job_id)?;
    if record.state.terminal() {
        return Ok(record);
    }
    let now = now_ms();
    record.state = JobState::Stopped;
    record.updated_at_ms = now;
    record.finished_at_ms.get_or_insert(now);
    record.error = Some("stopped by request".to_owned());
    publish_locked(&record)?;
    Ok(record)
}

pub fn load(session_id: &str, job_id: Uuid, active_in_process: bool) -> Result<JobRecord> {
    let _guard = journal_lock().lock().unwrap();
    let mut record = load_latest_locked(session_id, job_id)?;
    if record.state == JobState::Running && !active_in_process {
        mark_orphaned_locked(&mut record)?;
    }
    Ok(record)
}

pub fn list(
    session_id: &str,
    active_jobs: &HashSet<Uuid>,
    state_filter: Option<JobState>,
    offset: usize,
    limit: usize,
) -> Result<ListPage> {
    anyhow::ensure!(limit > 0 && limit <= 100, "limit must be between 1 and 100");
    let _guard = journal_lock().lock().unwrap();
    cleanup_locked(session_id, MAX_JOBS_PER_SESSION, RETENTION_AGE)?;
    let root = session_root(session_id)?;
    let mut records = Vec::new();
    let mut corrupt_entries = 0;
    let entries = match fs::read_dir(&root) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok(ListPage {
                jobs: Vec::new(),
                total: 0,
                offset,
                next_offset: None,
                corrupt_entries: 0,
            });
        }
        Err(error) => return Err(error.into()),
    };

    for entry in entries {
        let entry = entry?;
        if !entry.file_type()?.is_dir() {
            continue;
        }
        let Ok(job_id) = Uuid::parse_str(&entry.file_name().to_string_lossy()) else {
            corrupt_entries += 1;
            continue;
        };
        match load_latest_locked(session_id, job_id) {
            Ok(mut record) => {
                if record.state == JobState::Running
                    && !active_jobs.contains(&job_id)
                    && mark_orphaned_locked(&mut record).is_err()
                {
                    corrupt_entries += 1;
                    continue;
                }
                if state_filter.is_none_or(|state| state == record.state) {
                    records.push(record);
                }
            }
            Err(_) => corrupt_entries += 1,
        }
    }

    records.sort_by(|left, right| {
        right
            .created_at_ms
            .cmp(&left.created_at_ms)
            .then_with(|| right.job_id.cmp(&left.job_id))
    });
    let total = records.len();
    let jobs = records
        .iter()
        .skip(offset)
        .take(limit)
        .map(JobSummary::from)
        .collect::<Vec<_>>();
    let consumed = offset.saturating_add(jobs.len());
    let next_offset = (consumed < total).then_some(consumed);
    Ok(ListPage {
        jobs,
        total,
        offset,
        next_offset,
        corrupt_entries,
    })
}

#[cfg(test)]
pub fn remove_session_for_tests(session_id: &str) {
    if let Ok(root) = session_root(session_id) {
        let _ = fs::remove_dir_all(root);
    }
}

fn mark_orphaned_locked(record: &mut JobRecord) -> Result<()> {
    let now = now_ms();
    record.state = JobState::Orphaned;
    record.updated_at_ms = now;
    record.finished_at_ms.get_or_insert(now);
    record.error = Some(
        "MCP server restarted or lost the in-memory process handle; this process cannot be reattached"
            .to_owned(),
    );
    publish_locked(record)
}

fn bound_persisted_payload(text: &str, exit_code: Option<i32>, kind: &str) -> String {
    if serde_json::to_vec(text)
        .map(|bytes| bytes.len() <= MAX_PERSISTED_FIELD_SERIALIZED_BYTES)
        .unwrap_or(false)
    {
        return text.to_owned();
    }

    let mut preview_budget = MAX_PERSISTED_FIELD_SERIALIZED_BYTES.saturating_sub(1024);
    loop {
        let head_budget = preview_budget / 2 + preview_budget % 2;
        let tail_budget = preview_budget / 2;
        let value = serde_json::json!({
            "exit_code": exit_code,
            "journal_truncated": true,
            "kind": kind,
            "original_bytes": text.len(),
            "head": bounded_utf8_prefix(text, head_budget),
            "tail": bounded_utf8_suffix(text, tail_budget),
        });
        let rendered = value.to_string();
        let serialized_bytes = serde_json::to_vec(&rendered)
            .expect("bounded journal payload must serialize")
            .len();
        if serialized_bytes <= MAX_PERSISTED_FIELD_SERIALIZED_BYTES {
            return rendered;
        }
        preview_budget = preview_budget.saturating_sub(
            serialized_bytes
                .saturating_sub(MAX_PERSISTED_FIELD_SERIALIZED_BYTES)
                .max(64),
        );
    }
}

fn bound_plain_text(text: &str, max_bytes: usize) -> String {
    if text.len() <= max_bytes {
        return text.to_owned();
    }
    const MARKER: &str = "\n... journal text truncated ...\n";
    let content_budget = max_bytes.saturating_sub(MARKER.len());
    let head_budget = content_budget / 2 + content_budget % 2;
    let tail_budget = content_budget / 2;
    format!(
        "{}{}{}",
        bounded_utf8_prefix(text, head_budget),
        MARKER,
        bounded_utf8_suffix(text, tail_budget)
    )
}

fn bounded_utf8_prefix(text: &str, max_bytes: usize) -> &str {
    let mut end = 0;
    for (index, character) in text.char_indices() {
        let next = index + character.len_utf8();
        if next > max_bytes {
            break;
        }
        end = next;
    }
    &text[..end]
}

fn bounded_utf8_suffix(text: &str, max_bytes: usize) -> &str {
    let mut start = text.len();
    let mut used = 0;
    for (index, character) in text.char_indices().rev() {
        let width = character.len_utf8();
        if used + width > max_bytes {
            break;
        }
        used += width;
        start = index;
    }
    &text[start..]
}

fn parse_exit_code(text: &str) -> Option<i32> {
    serde_json::from_str::<Value>(text)
        .ok()?
        .get("exit_code")?
        .as_i64()
        .and_then(|value| i32::try_from(value).ok())
}

fn session_root(session_id: &str) -> Result<PathBuf> {
    config::validate_session_id(session_id)?;
    Ok(config::state_dir()?.join("jobs").join(session_id))
}

fn job_root(session_id: &str, job_id: Uuid) -> Result<PathBuf> {
    Ok(session_root(session_id)?.join(job_id.to_string()))
}

fn publish_locked(record: &JobRecord) -> Result<()> {
    anyhow::ensure!(
        record.version == JOURNAL_VERSION,
        "unsupported job journal version"
    );
    config::validate_session_id(&record.session_id)?;
    let job_id = Uuid::parse_str(&record.job_id).context("invalid journal job_id")?;
    let directory = job_root(&record.session_id, job_id)?;
    fs::create_dir_all(&directory)?;
    let stamp = now_nanos();
    let nonce = Uuid::new_v4();
    let base = format!("{stamp:039}-{nonce}");
    let temporary = directory.join(format!(".{base}.tmp"));
    let published = directory.join(format!("{base}.json"));
    let bytes = serde_json::to_vec_pretty(record)?;
    anyhow::ensure!(
        bytes.len() as u64 <= MAX_JOURNAL_SNAPSHOT_BYTES,
        "job journal snapshot exceeds {MAX_JOURNAL_SNAPSHOT_BYTES} bytes"
    );
    let mut file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&temporary)?;
    file.write_all(&bytes)?;
    file.sync_all()?;
    fs::rename(&temporary, &published)?;
    sync_directory(&directory);

    for entry in fs::read_dir(&directory)? {
        let entry = entry?;
        let path = entry.path();
        if path != published && path.extension().and_then(|value| value.to_str()) == Some("json") {
            let _ = fs::remove_file(path);
        }
    }
    Ok(())
}

fn load_latest_locked(session_id: &str, job_id: Uuid) -> Result<JobRecord> {
    let directory = job_root(session_id, job_id)?;
    let mut snapshots = fs::read_dir(&directory)
        .with_context(|| format!("unknown job_id: {job_id}"))?
        .filter_map(|entry| entry.ok())
        .map(|entry| entry.path())
        .filter(|path| path.extension().and_then(|value| value.to_str()) == Some("json"))
        .collect::<Vec<_>>();
    snapshots.sort();
    let path = snapshots
        .pop()
        .with_context(|| format!("unknown job_id: {job_id}"))?;
    let metadata = fs::metadata(&path)
        .with_context(|| format!("cannot stat job journal {}", path.display()))?;
    anyhow::ensure!(
        metadata.len() <= MAX_JOURNAL_SNAPSHOT_BYTES,
        "job journal {} exceeds {MAX_JOURNAL_SNAPSHOT_BYTES} bytes",
        path.display()
    );
    let bytes =
        fs::read(&path).with_context(|| format!("cannot read job journal {}", path.display()))?;
    let record: JobRecord = serde_json::from_slice(&bytes)
        .with_context(|| format!("corrupt job journal {}", path.display()))?;
    anyhow::ensure!(
        record.version == JOURNAL_VERSION,
        "unsupported job journal version"
    );
    anyhow::ensure!(
        record.session_id == session_id,
        "job journal session mismatch"
    );
    anyhow::ensure!(
        record.job_id == job_id.to_string(),
        "job journal ID mismatch"
    );
    Ok(record)
}

fn cleanup_locked(session_id: &str, max_jobs: usize, retention_age: Duration) -> Result<()> {
    let root = session_root(session_id)?;
    let entries = match fs::read_dir(&root) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error.into()),
    };
    let now = now_ms();
    let cutoff = now.saturating_sub(retention_age.as_millis().min(u64::MAX as u128) as u64);
    let mut terminal = Vec::new();
    let mut total: usize = 0;
    for entry in entries {
        let entry = entry?;
        if !entry.file_type()?.is_dir() {
            continue;
        }
        total += 1;
        let path = entry.path();
        for child in fs::read_dir(&path)? {
            let child = child?;
            if child.path().extension().and_then(|value| value.to_str()) == Some("tmp") {
                let _ = fs::remove_file(child.path());
            }
        }
        let Ok(job_id) = Uuid::parse_str(&entry.file_name().to_string_lossy()) else {
            continue;
        };
        if let Ok(record) = load_latest_locked(session_id, job_id)
            && record.state.terminal()
        {
            if record.updated_at_ms < cutoff {
                let _ = fs::remove_dir_all(&path);
                total = total.saturating_sub(1);
            } else {
                terminal.push((record.updated_at_ms, path));
            }
        }
    }
    terminal.sort_by(|left, right| left.0.cmp(&right.0).then_with(|| left.1.cmp(&right.1)));
    let remove_count = total.saturating_sub(max_jobs).min(terminal.len());
    for (_, path) in terminal.into_iter().take(remove_count) {
        let _ = fs::remove_dir_all(path);
    }
    Ok(())
}

fn count_job_directories_locked(session_id: &str) -> Result<usize> {
    let root = session_root(session_id)?;
    let entries = match fs::read_dir(&root) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(0),
        Err(error) => return Err(error.into()),
    };
    let mut count = 0_usize;
    for entry in entries {
        if entry?.file_type()?.is_dir() {
            count = count.saturating_add(1);
        }
    }
    Ok(count)
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
mod tests {
    use super::*;

    fn record_result(exit_code: i32, stdout: &str) -> Result<String> {
        if exit_code == 0 {
            Ok(serde_json::json!({
                "exit_code": exit_code,
                "stdout": stdout,
                "stderr": ""
            })
            .to_string())
        } else {
            Err(anyhow::anyhow!(
                serde_json::json!({
                    "exit_code": exit_code,
                    "stdout": "",
                    "stderr": stdout
                })
                .to_string()
            ))
        }
    }

    #[test]
    fn completed_result_survives_reload() {
        let session_id = format!("journal-complete-{}", Uuid::new_v4());
        let job_id = Uuid::new_v4();
        create_running(
            &session_id,
            job_id,
            "cargo test".to_owned(),
            PathBuf::from("/workspace"),
            ExecutionMode::Sandboxed,
        )
        .unwrap();
        finish(&session_id, job_id, &record_result(0, "ok")).unwrap();

        let record = load(&session_id, job_id, false).unwrap();
        assert_eq!(record.state, JobState::Completed);
        assert_eq!(record.exit_code, Some(0));
        assert!(record.result.as_deref().unwrap().contains("ok"));
        remove_session_for_tests(&session_id);
    }

    #[test]
    fn running_record_becomes_orphaned_after_reload_without_active_handle() {
        let session_id = format!("journal-running-{}", Uuid::new_v4());
        let job_id = Uuid::new_v4();
        create_running(
            &session_id,
            job_id,
            "sleep 10".to_owned(),
            PathBuf::from("/workspace"),
            ExecutionMode::Sandboxed,
        )
        .unwrap();

        let record = load(&session_id, job_id, false).unwrap();
        assert_eq!(record.state, JobState::Orphaned);
        assert!(
            record
                .error
                .as_deref()
                .unwrap()
                .contains("cannot be reattached")
        );
        remove_session_for_tests(&session_id);
    }

    #[test]
    fn stopped_record_is_idempotent_and_reloadable() {
        let session_id = format!("journal-stopped-{}", Uuid::new_v4());
        let job_id = Uuid::new_v4();
        create_running(
            &session_id,
            job_id,
            "sleep 10".to_owned(),
            PathBuf::from("/workspace"),
            ExecutionMode::Sandboxed,
        )
        .unwrap();
        mark_stopped(&session_id, job_id).unwrap();
        let again = mark_stopped(&session_id, job_id).unwrap();
        assert_eq!(again.state, JobState::Stopped);
        assert_eq!(
            load(&session_id, job_id, false).unwrap().state,
            JobState::Stopped
        );
        remove_session_for_tests(&session_id);
    }

    #[test]
    fn stop_does_not_overwrite_completed_or_failed_terminal_results() {
        for (exit_code, expected_state) in [(0, JobState::Completed), (7, JobState::Failed)] {
            let session_id = format!("journal-terminal-stop-{}", Uuid::new_v4());
            let job_id = Uuid::new_v4();
            create_running(
                &session_id,
                job_id,
                "terminal".to_owned(),
                PathBuf::from("/workspace"),
                ExecutionMode::Sandboxed,
            )
            .unwrap();
            let finished =
                finish(&session_id, job_id, &record_result(exit_code, "original")).unwrap();
            let stopped = mark_stopped(&session_id, job_id).unwrap();

            assert_eq!(stopped.state, expected_state);
            assert_eq!(stopped.result, finished.result);
            assert_eq!(stopped.error, finished.error);
            assert_eq!(stopped.exit_code, finished.exit_code);
            remove_session_for_tests(&session_id);
        }
    }

    #[test]
    fn persisted_result_and_error_payloads_and_snapshots_are_bounded() {
        for exit_code in [0, 9] {
            let session_id = format!("journal-bounded-{}", Uuid::new_v4());
            let job_id = Uuid::new_v4();
            create_running(
                &session_id,
                job_id,
                "quoted command".repeat(10_000),
                PathBuf::from("/workspace"),
                ExecutionMode::Sandboxed,
            )
            .unwrap();
            let noisy = "\\\"日本語🙂\n".repeat(200_000);
            let record = finish(&session_id, job_id, &record_result(exit_code, &noisy)).unwrap();
            let payload = record
                .result
                .as_ref()
                .or(record.error.as_ref())
                .expect("terminal record must contain a payload");

            assert!(payload.contains("journal_truncated"));
            assert!(payload.contains("original_bytes"));
            assert!(
                serde_json::to_vec(payload).unwrap().len() <= MAX_PERSISTED_FIELD_SERIALIZED_BYTES
            );
            assert!(
                serde_json::to_vec(&record).unwrap().len() as u64 <= MAX_JOURNAL_SNAPSHOT_BYTES
            );
            assert_eq!(record.exit_code, Some(exit_code));
            remove_session_for_tests(&session_id);
        }
    }

    #[test]
    fn oversized_snapshot_fails_closed_before_allocation() {
        let session_id = format!("journal-oversized-{}", Uuid::new_v4());
        let job_id = Uuid::new_v4();
        create_running(
            &session_id,
            job_id,
            "true".to_owned(),
            PathBuf::from("/workspace"),
            ExecutionMode::Sandboxed,
        )
        .unwrap();
        let directory = job_root(&session_id, job_id).unwrap();
        for entry in fs::read_dir(&directory).unwrap() {
            let path = entry.unwrap().path();
            if path.extension().and_then(|value| value.to_str()) == Some("json") {
                fs::remove_file(path).unwrap();
            }
        }
        let path = directory.join(format!("{:039}-{}.json", now_nanos(), Uuid::new_v4()));
        let file = File::create(&path).unwrap();
        file.set_len(MAX_JOURNAL_SNAPSHOT_BYTES + 1).unwrap();

        let error = load(&session_id, job_id, false).unwrap_err();
        assert!(error.to_string().contains("exceeds"));
        remove_session_for_tests(&session_id);
    }

    #[test]
    fn corrupt_latest_snapshot_fails_closed_without_hiding_other_jobs() {
        let session_id = format!("journal-corrupt-{}", Uuid::new_v4());
        let good_job = Uuid::new_v4();
        let corrupt_job = Uuid::new_v4();
        create_running(
            &session_id,
            good_job,
            "true".to_owned(),
            PathBuf::from("/workspace"),
            ExecutionMode::Sandboxed,
        )
        .unwrap();
        finish(&session_id, good_job, &record_result(0, "done")).unwrap();
        create_running(
            &session_id,
            corrupt_job,
            "false".to_owned(),
            PathBuf::from("/workspace"),
            ExecutionMode::Sandboxed,
        )
        .unwrap();
        let directory = job_root(&session_id, corrupt_job).unwrap();
        fs::write(
            directory.join(format!("{:039}-{}.json", now_nanos() + 1, Uuid::new_v4())),
            b"{not valid json",
        )
        .unwrap();

        assert!(load(&session_id, corrupt_job, false).is_err());
        let page = list(&session_id, &HashSet::new(), None, 0, 100).unwrap();
        assert_eq!(page.jobs.len(), 1);
        assert_eq!(page.jobs[0].job_id, good_job.to_string());
        assert_eq!(page.corrupt_entries, 1);
        remove_session_for_tests(&session_id);
    }

    #[test]
    fn stale_temporary_snapshot_is_ignored_and_removed() {
        let session_id = format!("journal-temp-{}", Uuid::new_v4());
        let job_id = Uuid::new_v4();
        create_running(
            &session_id,
            job_id,
            "true".to_owned(),
            PathBuf::from("/workspace"),
            ExecutionMode::Sandboxed,
        )
        .unwrap();
        finish(&session_id, job_id, &record_result(0, "done")).unwrap();
        let temporary = job_root(&session_id, job_id).unwrap().join("stale.tmp");
        fs::write(&temporary, b"partial").unwrap();

        let page = list(&session_id, &HashSet::new(), None, 0, 10).unwrap();
        assert_eq!(page.jobs.len(), 1);
        assert!(!temporary.exists());
        remove_session_for_tests(&session_id);
    }

    #[test]
    fn list_is_session_scoped_filterable_and_paginated() {
        let first_session = format!("journal-list-a-{}", Uuid::new_v4());
        let second_session = format!("journal-list-b-{}", Uuid::new_v4());
        let completed_job = Uuid::new_v4();
        let failed_job = Uuid::new_v4();
        let other_job = Uuid::new_v4();

        create_running(
            &first_session,
            completed_job,
            "completed".to_owned(),
            PathBuf::from("/workspace"),
            ExecutionMode::Sandboxed,
        )
        .unwrap();
        finish(&first_session, completed_job, &record_result(0, "done")).unwrap();
        std::thread::sleep(Duration::from_millis(2));
        create_running(
            &first_session,
            failed_job,
            "failed".to_owned(),
            PathBuf::from("/workspace"),
            ExecutionMode::Sandboxed,
        )
        .unwrap();
        finish(&first_session, failed_job, &record_result(9, "failure")).unwrap();
        create_running(
            &second_session,
            other_job,
            "other".to_owned(),
            PathBuf::from("/workspace"),
            ExecutionMode::Sandboxed,
        )
        .unwrap();
        finish(&second_session, other_job, &record_result(0, "other")).unwrap();

        let first_page = list(&first_session, &HashSet::new(), None, 0, 1).unwrap();
        assert_eq!(first_page.total, 2);
        assert_eq!(first_page.jobs.len(), 1);
        assert_eq!(first_page.next_offset, Some(1));
        assert_ne!(first_page.jobs[0].job_id, other_job.to_string());

        let second_page = list(&first_session, &HashSet::new(), None, 1, 1).unwrap();
        assert_eq!(second_page.jobs.len(), 1);
        assert_eq!(second_page.next_offset, None);
        assert_ne!(second_page.jobs[0].job_id, other_job.to_string());

        let failed = list(
            &first_session,
            &HashSet::new(),
            Some(JobState::Failed),
            0,
            10,
        )
        .unwrap();
        assert_eq!(failed.total, 1);
        assert_eq!(failed.jobs[0].job_id, failed_job.to_string());
        assert_eq!(failed.jobs[0].state, JobState::Failed);

        assert!(load(&first_session, other_job, false).is_err());
        remove_session_for_tests(&first_session);
        remove_session_for_tests(&second_session);
    }

    #[test]
    fn retention_removes_oldest_terminal_records() {
        let session_id = format!("journal-retention-{}", Uuid::new_v4());
        let mut ids = Vec::new();
        for index in 0..3 {
            let job_id = Uuid::new_v4();
            ids.push(job_id);
            create_running(
                &session_id,
                job_id,
                format!("job {index}"),
                PathBuf::from("/workspace"),
                ExecutionMode::Sandboxed,
            )
            .unwrap();
            finish(&session_id, job_id, &record_result(0, "done")).unwrap();
            std::thread::sleep(Duration::from_millis(2));
        }
        {
            let _guard = journal_lock().lock().unwrap();
            cleanup_locked(&session_id, 2, Duration::from_secs(u64::MAX)).unwrap();
        }
        assert!(!job_root(&session_id, ids[0]).unwrap().exists());
        assert!(job_root(&session_id, ids[1]).unwrap().exists());
        assert!(job_root(&session_id, ids[2]).unwrap().exists());
        remove_session_for_tests(&session_id);
    }

    #[test]
    fn running_records_cannot_exceed_the_hard_session_limit() {
        let session_id = format!("journal-running-limit-{}", Uuid::new_v4());
        for index in 0..MAX_JOBS_PER_SESSION {
            create_running(
                &session_id,
                Uuid::new_v4(),
                format!("running job {index}"),
                PathBuf::from("/workspace"),
                ExecutionMode::Sandboxed,
            )
            .unwrap();
        }

        let error = create_running(
            &session_id,
            Uuid::new_v4(),
            "one job too many".to_owned(),
            PathBuf::from("/workspace"),
            ExecutionMode::Sandboxed,
        )
        .unwrap_err();

        assert!(error.to_string().contains("job journal limit reached"));
        {
            let _guard = journal_lock().lock().unwrap();
            assert_eq!(
                count_job_directories_locked(&session_id).unwrap(),
                MAX_JOBS_PER_SESSION
            );
        }
        remove_session_for_tests(&session_id);
    }
}
