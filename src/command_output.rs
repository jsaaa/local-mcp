use std::cmp::min;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};

use anyhow::{Context, Result};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use serde_json::{Value, json};
use tokio::io::{AsyncReadExt, AsyncSeekExt};
use uuid::Uuid;

use crate::{config, sandbox};

pub const DEFAULT_INLINE_OUTPUT_LIMIT: usize = 16 * 1024;
pub const MIN_INLINE_OUTPUT_LIMIT: usize = 2 * 1024;
pub const MAX_INLINE_OUTPUT_LIMIT: usize = 1024 * 1024;
pub const DEFAULT_LOG_READ_BYTES: usize = 8 * 1024;
pub const MAX_LOG_READ_BYTES: usize = 64 * 1024;
pub const MAX_JSONRPC_ID_SERIALIZED_BYTES: usize = 256;
const LOG_RETENTION_DIRECTORIES: usize = 128;
const LOG_RETENTION_AGE: Duration = Duration::from_secs(7 * 24 * 60 * 60);
const INLINE_LIMIT_ENV: &str = "LOCAL_MCP_INLINE_OUTPUT_BYTES";

#[derive(Debug)]
struct StreamPreview {
    bytes: u64,
    truncated: bool,
    head: String,
    tail: String,
}

#[derive(Debug)]
pub struct LogCapturePaths {
    stdout: PathBuf,
    stderr: PathBuf,
}

impl LogCapturePaths {
    pub fn stdout(&self) -> &Path {
        &self.stdout
    }

    pub fn stderr(&self) -> &Path {
        &self.stderr
    }
}

pub fn inline_output_limit() -> usize {
    std::env::var(INLINE_LIMIT_ENV)
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(DEFAULT_INLINE_OUTPUT_LIMIT)
        .clamp(MIN_INLINE_OUTPUT_LIMIT, MAX_INLINE_OUTPUT_LIMIT)
}

pub fn capture_preview_limit() -> usize {
    inline_output_limit()
}

pub fn jsonrpc_id_within_budget(id: &Value) -> bool {
    serde_json::to_vec(id)
        .map(|serialized| serialized.len() <= MAX_JSONRPC_ID_SERIALIZED_BYTES)
        .unwrap_or(false)
}

pub fn resource_uri(job_id: Uuid, stream: &str) -> String {
    format!("local-mcp://jobs/{job_id}/{stream}")
}

pub async fn prepare_log_capture(session_id: &str, job_id: Uuid) -> Result<LogCapturePaths> {
    cleanup_log_storage(session_id).await?;
    let directory = execution_log_dir(session_id, job_id)?;
    tokio::fs::create_dir_all(&directory).await?;
    Ok(LogCapturePaths {
        stdout: directory.join("stdout"),
        stderr: directory.join("stderr"),
    })
}

pub fn render_logged_output(job_id: Uuid, output: sandbox::LoggedOutput) -> Result<String> {
    let value = bounded_result_value(
        job_id,
        output.status,
        &output.stdout,
        &output.stderr,
        inline_output_limit(),
    );
    let text = serde_json::to_string(&value)?;
    if output.status == 0 {
        Ok(text)
    } else {
        anyhow::bail!(text)
    }
}

pub async fn read_range(
    session_id: &str,
    job_id: Uuid,
    stream: &str,
    offset: u64,
    length: usize,
) -> Result<Value> {
    anyhow::ensure!(
        matches!(stream, "stdout" | "stderr"),
        "stream must be stdout or stderr"
    );
    anyhow::ensure!(length > 0, "length must be at least 1");
    anyhow::ensure!(
        length <= MAX_LOG_READ_BYTES,
        "length must be at most {MAX_LOG_READ_BYTES}"
    );

    let path = execution_log_dir(session_id, job_id)?.join(stream);
    let metadata = tokio::fs::metadata(&path)
        .await
        .with_context(|| format!("no stored {stream} log for job {job_id}"))?;
    anyhow::ensure!(metadata.is_file(), "stored log is not a file");
    let total_bytes = metadata.len();
    let start = offset.min(total_bytes);
    let available = total_bytes.saturating_sub(start);
    let bytes_to_read = min(available, length as u64) as usize;

    let mut file = tokio::fs::File::open(&path).await?;
    file.seek(std::io::SeekFrom::Start(start)).await?;
    let mut bytes = vec![0_u8; bytes_to_read];
    file.read_exact(&mut bytes).await?;

    let (encoding, data) = match std::str::from_utf8(&bytes) {
        Ok(text) => ("utf-8", text.to_owned()),
        Err(_) => ("base64", STANDARD.encode(&bytes)),
    };
    let next_offset = start + bytes.len() as u64;
    Ok(json!({
        "job_id": job_id,
        "stream": stream,
        "resource_uri": resource_uri(job_id, stream),
        "offset": start,
        "bytes_read": bytes.len(),
        "total_bytes": total_bytes,
        "next_offset": next_offset,
        "eof": next_offset >= total_bytes,
        "encoding": encoding,
        "data": data,
    }))
}

fn session_log_root(session_id: &str) -> Result<PathBuf> {
    config::validate_session_id(session_id)?;
    Ok(config::state_dir()?.join("command-logs").join(session_id))
}

fn execution_log_dir(session_id: &str, job_id: Uuid) -> Result<PathBuf> {
    Ok(session_log_root(session_id)?.join(job_id.to_string()))
}

async fn cleanup_log_storage(session_id: &str) -> Result<()> {
    cleanup_log_storage_with_limit(session_id, LOG_RETENTION_DIRECTORIES.saturating_sub(1)).await
}

async fn cleanup_log_storage_with_limit(session_id: &str, max_existing: usize) -> Result<()> {
    let root = session_log_root(session_id)?;
    let mut entries = match tokio::fs::read_dir(&root).await {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error.into()),
    };
    let now = SystemTime::now();
    let mut retained = Vec::new();
    while let Some(entry) = entries.next_entry().await? {
        if !entry.file_type().await?.is_dir() {
            continue;
        }
        let modified = entry
            .metadata()
            .await
            .and_then(|metadata| metadata.modified())
            .unwrap_or(SystemTime::UNIX_EPOCH);
        let stale = now.duration_since(modified).unwrap_or_default() > LOG_RETENTION_AGE;
        if stale {
            let _ = tokio::fs::remove_dir_all(entry.path()).await;
        } else {
            retained.push((modified, entry.path()));
        }
    }

    retained.sort_by(|left, right| {
        left.0
            .cmp(&right.0)
            .then_with(|| left.1.as_os_str().cmp(right.1.as_os_str()))
    });
    let remove_count = retained.len().saturating_sub(max_existing);
    for (_, path) in retained.into_iter().take(remove_count) {
        let _ = tokio::fs::remove_dir_all(path).await;
    }
    Ok(())
}

fn bounded_result_value(
    job_id: Uuid,
    exit_code: i32,
    stdout: &sandbox::StreamCapture,
    stderr: &sandbox::StreamCapture,
    requested_limit: usize,
) -> Value {
    let limit = requested_limit.clamp(MIN_INLINE_OUTPUT_LIMIT, MAX_INLINE_OUTPUT_LIMIT);
    let mut preview_budget = limit;
    loop {
        let (stdout_budget, stderr_budget) = split_preview_budget(preview_budget, stdout, stderr);
        let stdout_preview = stream_preview(stdout, stdout_budget);
        let stderr_preview = stream_preview(stderr, stderr_budget);
        let value = json!({
            "job_id": job_id,
            "status": if exit_code == 0 { "completed" } else { "failed" },
            "exit_code": exit_code,
            "stdout_bytes": stdout_preview.bytes,
            "stderr_bytes": stderr_preview.bytes,
            "stdout_truncated": stdout_preview.truncated,
            "stderr_truncated": stderr_preview.truncated,
            "stdout_head": stdout_preview.head,
            "stdout_tail": stdout_preview.tail,
            "stderr_head": stderr_preview.head,
            "stderr_tail": stderr_preview.tail,
            "stdout_resource": resource_uri(job_id, "stdout"),
            "stderr_resource": resource_uri(job_id, "stderr"),
            "read_tool": "read_job_log",
        });
        let text = serde_json::to_string(&value).expect("command result metadata must serialize");
        let response_bytes = max_jsonrpc_response_bytes(&text);
        if response_bytes <= limit {
            return value;
        }
        if preview_budget == 0 {
            debug_assert!(
                response_bytes <= limit,
                "command result metadata exceeds the minimum response limit"
            );
            return value;
        }
        preview_budget =
            preview_budget.saturating_sub(response_bytes.saturating_sub(limit).max(64));
    }
}

fn max_jsonrpc_response_bytes(text: &str) -> usize {
    let reserved_id = "i".repeat(MAX_JSONRPC_ID_SERIALIZED_BYTES.saturating_sub(2));
    let success = json!({
        "jsonrpc": "2.0",
        "id": reserved_id,
        "result": {"content": [{"type": "text", "text": text}]}
    });
    let error = json!({
        "jsonrpc": "2.0",
        "id": "i".repeat(MAX_JSONRPC_ID_SERIALIZED_BYTES.saturating_sub(2)),
        "error": {"code": -32000, "message": text}
    });
    // write_message appends one newline byte after the serialized JSON-RPC
    // object, so include it in the on-wire response budget as well.
    serde_json::to_vec(&success)
        .expect("success response must serialize")
        .len()
        .max(
            serde_json::to_vec(&error)
                .expect("error response must serialize")
                .len(),
        )
        .saturating_add(1)
}

fn split_preview_budget(
    total: usize,
    stdout: &sandbox::StreamCapture,
    stderr: &sandbox::StreamCapture,
) -> (usize, usize) {
    match (stdout.bytes == 0, stderr.bytes == 0) {
        (true, true) => (0, 0),
        (false, true) => (total, 0),
        (true, false) => (0, total),
        (false, false) => {
            let stdout_budget = total / 2 + total % 2;
            (stdout_budget, total / 2)
        }
    }
}

fn stream_preview(stream: &sandbox::StreamCapture, budget: usize) -> StreamPreview {
    if stream.bytes == 0 || budget == 0 {
        return StreamPreview {
            bytes: stream.bytes,
            truncated: stream.bytes != 0,
            head: String::new(),
            tail: String::new(),
        };
    }

    if stream.bytes <= budget as u64 && stream.head.len() as u64 >= stream.bytes {
        let complete = &stream.head[..stream.bytes as usize];
        return StreamPreview {
            bytes: stream.bytes,
            truncated: false,
            head: String::from_utf8_lossy(complete).into_owned(),
            tail: String::new(),
        };
    }

    let head_budget = budget / 2 + budget % 2;
    let tail_budget = budget / 2;
    let head = String::from_utf8_lossy(&stream.head);
    let tail = String::from_utf8_lossy(&stream.tail);
    StreamPreview {
        bytes: stream.bytes,
        truncated: true,
        head: bounded_utf8_prefix(&head, head_budget),
        tail: bounded_utf8_suffix(&tail, tail_budget),
    }
}

fn bounded_utf8_prefix(text: &str, max_bytes: usize) -> String {
    let mut end = 0;
    for (index, character) in text.char_indices() {
        let next = index + character.len_utf8();
        if next > max_bytes {
            break;
        }
        end = next;
    }
    text[..end].to_owned()
}

fn bounded_utf8_suffix(text: &str, max_bytes: usize) -> String {
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
    text[start..].to_owned()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn capture(bytes: &[u8], limit: usize) -> sandbox::StreamCapture {
        sandbox::StreamCapture {
            bytes: bytes.len() as u64,
            head: bytes[..bytes.len().min(limit)].to_vec(),
            tail: bytes[bytes.len().saturating_sub(limit)..].to_vec(),
        }
    }

    #[test]
    fn large_results_are_utf8_safe_and_fit_final_jsonrpc_envelopes() {
        let stdout = "日本語🙂\\\"".repeat(10_000).into_bytes();
        let stderr = b"important failure\\n\\\\quoted\\n".repeat(2_000);
        let value = bounded_result_value(
            Uuid::new_v4(),
            7,
            &capture(&stdout, 4096),
            &capture(&stderr, 4096),
            4096,
        );
        let text = serde_json::to_string(&value).unwrap();

        assert!(
            max_jsonrpc_response_bytes(&text) <= 4096,
            "final response was {} bytes",
            max_jsonrpc_response_bytes(&text)
        );
        assert_eq!(value["stdout_bytes"], stdout.len() as u64);
        assert_eq!(value["stderr_bytes"], stderr.len() as u64);
        assert_eq!(value["exit_code"], 7);
        assert!(value["stdout_truncated"].as_bool().unwrap());
        assert!(value["stderr_truncated"].as_bool().unwrap());
        assert!(
            value["stderr_head"]
                .as_str()
                .unwrap()
                .contains("important failure")
        );
    }

    #[test]
    fn minimum_limit_bounds_both_success_and_error_jsonrpc_responses() {
        let stdout = b"\\\"\\\\\\n".repeat(100_000);
        let value = bounded_result_value(
            Uuid::new_v4(),
            1,
            &capture(&stdout, MIN_INLINE_OUTPUT_LIMIT),
            &capture(&[], MIN_INLINE_OUTPUT_LIMIT),
            MIN_INLINE_OUTPUT_LIMIT,
        );
        let text = serde_json::to_string(&value).unwrap();
        assert!(max_jsonrpc_response_bytes(&text) <= MIN_INLINE_OUTPUT_LIMIT);
    }

    #[tokio::test]
    async fn stored_logs_are_range_readable_and_session_scoped() {
        let session_id = format!("output-test-{}", Uuid::new_v4());
        let other_session_id = format!("output-test-{}", Uuid::new_v4());
        let job_id = Uuid::new_v4();
        let paths = prepare_log_capture(&session_id, job_id).await.unwrap();
        tokio::fs::write(paths.stdout(), b"0123456789")
            .await
            .unwrap();
        tokio::fs::write(paths.stderr(), [0xff, 0x00, 0x01])
            .await
            .unwrap();

        let range = read_range(&session_id, job_id, "stdout", 3, 4)
            .await
            .unwrap();
        assert_eq!(range["data"], "3456");
        assert_eq!(range["next_offset"], 7);
        assert_eq!(range["eof"], false);

        let binary = read_range(&session_id, job_id, "stderr", 0, 3)
            .await
            .unwrap();
        assert_eq!(binary["encoding"], "base64");
        assert_eq!(binary["data"], STANDARD.encode([0xff, 0x00, 0x01]));
        assert!(
            read_range(&other_session_id, job_id, "stdout", 0, 4)
                .await
                .is_err()
        );

        let _ = tokio::fs::remove_dir_all(session_log_root(&session_id).unwrap()).await;
    }

    #[tokio::test]
    async fn cleanup_keeps_only_the_newest_configured_directories() {
        let session_id = format!("output-cleanup-{}", Uuid::new_v4());
        let root = session_log_root(&session_id).unwrap();
        tokio::fs::create_dir_all(&root).await.unwrap();
        let mut paths = Vec::new();
        for index in 0..3 {
            let path = root.join(format!("entry-{index}"));
            tokio::fs::create_dir_all(&path).await.unwrap();
            tokio::fs::write(path.join("stdout"), b"test")
                .await
                .unwrap();
            paths.push(path);
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        cleanup_log_storage_with_limit(&session_id, 2)
            .await
            .unwrap();
        assert!(!paths[0].exists());
        assert!(paths[1].exists());
        assert!(paths[2].exists());
        let _ = tokio::fs::remove_dir_all(root).await;
    }
}
