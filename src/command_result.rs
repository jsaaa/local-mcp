use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use uuid::Uuid;

pub const COMMAND_RESULT_SCHEMA_VERSION: u32 = 1;

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CommandStatus {
    Running,
    Completed,
    Failed,
    Stopped,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CommandErrorKind {
    ProcessExit,
    SpawnError,
    Cancellation,
    ApprovalDenied,
    InvalidArguments,
    Internal,
}

#[derive(Clone, Debug)]
pub struct BoundedCommandOutput {
    pub job_id: Uuid,
    pub exit_code: i32,
    pub termination: String,
    pub termination_trigger: Option<String>,
    pub stdout_bytes: u64,
    pub stderr_bytes: u64,
    pub stdout_truncated: bool,
    pub stderr_truncated: bool,
    pub stdout_head: String,
    pub stdout_tail: String,
    pub stderr_head: String,
    pub stderr_tail: String,
    pub stdout_resource: String,
    pub stderr_resource: String,
    pub read_tool: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct CommandOutcome {
    pub schema_version: u32,
    pub ok: bool,
    pub status: CommandStatus,
    pub error_kind: Option<CommandErrorKind>,
    pub exit_code: Option<i32>,
    pub retryable: bool,
    pub job_id: Option<String>,
    pub message: String,
    pub termination: Option<String>,
    pub termination_trigger: Option<String>,
    pub stdout_bytes: u64,
    pub stderr_bytes: u64,
    pub stdout_truncated: bool,
    pub stderr_truncated: bool,
    pub stdout_head: String,
    pub stdout_tail: String,
    pub stderr_head: String,
    pub stderr_tail: String,
    pub stdout_resource: Option<String>,
    pub stderr_resource: Option<String>,
    pub read_tool: Option<String>,
}

impl CommandOutcome {
    pub fn running(job_id: Uuid) -> Self {
        Self::empty(
            true,
            CommandStatus::Running,
            None,
            None,
            false,
            Some(job_id),
            format!("Command is running as job {job_id}."),
        )
    }

    pub fn from_bounded_output(output: BoundedCommandOutput) -> Self {
        let (ok, status, error_kind, retryable, message) = classify_termination(
            output.exit_code,
            &output.termination,
            output.termination_trigger.as_deref(),
        );
        Self {
            schema_version: COMMAND_RESULT_SCHEMA_VERSION,
            ok,
            status,
            error_kind,
            exit_code: Some(output.exit_code),
            retryable,
            job_id: Some(output.job_id.to_string()),
            message,
            termination: Some(output.termination),
            termination_trigger: output.termination_trigger,
            stdout_bytes: output.stdout_bytes,
            stderr_bytes: output.stderr_bytes,
            stdout_truncated: output.stdout_truncated,
            stderr_truncated: output.stderr_truncated,
            stdout_head: output.stdout_head,
            stdout_tail: output.stdout_tail,
            stderr_head: output.stderr_head,
            stderr_tail: output.stderr_tail,
            stdout_resource: Some(output.stdout_resource),
            stderr_resource: Some(output.stderr_resource),
            read_tool: Some(output.read_tool),
        }
    }

    pub fn spawn_error(message: impl Into<String>) -> Self {
        Self::failure(
            CommandErrorKind::SpawnError,
            true,
            format!("Command could not be started: {}", message.into()),
        )
    }

    pub fn cancellation(job_id: Option<Uuid>, message: impl Into<String>) -> Self {
        let mut outcome = Self::empty(
            false,
            CommandStatus::Stopped,
            Some(CommandErrorKind::Cancellation),
            None,
            true,
            job_id,
            message.into(),
        );
        outcome.termination = Some("cancelled".to_owned());
        outcome
    }

    pub fn orphaned(job_id: Uuid, message: impl Into<String>) -> Self {
        let mut outcome = Self::empty(
            false,
            CommandStatus::Stopped,
            Some(CommandErrorKind::Cancellation),
            None,
            false,
            Some(job_id),
            message.into(),
        );
        outcome.termination = Some("orphaned".to_owned());
        outcome
    }

    pub fn approval_denied(message: impl Into<String>) -> Self {
        Self::failure(CommandErrorKind::ApprovalDenied, false, message)
    }

    pub fn invalid_arguments(message: impl Into<String>) -> Self {
        Self::failure(CommandErrorKind::InvalidArguments, false, message)
    }

    pub fn internal(message: impl Into<String>) -> Self {
        Self::failure(CommandErrorKind::Internal, true, message)
    }

    pub fn with_job_id(mut self, job_id: Uuid) -> Self {
        self.job_id = Some(job_id.to_string());
        self
    }

    pub fn fallback_text(&self) -> String {
        let mut text = self.message.clone();
        if let Some(exit_code) = self.exit_code {
            text.push_str(&format!(" Exit code: {exit_code}."));
        }
        if let Some(job_id) = &self.job_id
            && !text.contains(job_id)
        {
            text.push_str(&format!(" Job: {job_id}."));
        }
        if let Some(termination) = &self.termination {
            text.push_str(&format!(" Termination: {termination}."));
        }
        append_stream_preview(
            &mut text,
            "stdout",
            &self.stdout_head,
            &self.stdout_tail,
            self.stdout_truncated,
        );
        append_stream_preview(
            &mut text,
            "stderr",
            &self.stderr_head,
            &self.stderr_tail,
            self.stderr_truncated,
        );
        text
    }

    pub fn activity_summary(&self, max_bytes: usize) -> Option<String> {
        let text = self.fallback_text();
        (!text.is_empty()).then(|| bounded_utf8(&text, max_bytes))
    }

    pub fn tool_result(&self) -> Value {
        json!({
            "content": [{"type": "text", "text": self.fallback_text()}],
            "structuredContent": self,
            "isError": !self.ok,
        })
    }

    fn failure(kind: CommandErrorKind, retryable: bool, message: impl Into<String>) -> Self {
        Self::empty(
            false,
            CommandStatus::Failed,
            Some(kind),
            None,
            retryable,
            None,
            message.into(),
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn empty(
        ok: bool,
        status: CommandStatus,
        error_kind: Option<CommandErrorKind>,
        exit_code: Option<i32>,
        retryable: bool,
        job_id: Option<Uuid>,
        message: String,
    ) -> Self {
        Self {
            schema_version: COMMAND_RESULT_SCHEMA_VERSION,
            ok,
            status,
            error_kind,
            exit_code,
            retryable,
            job_id: job_id.map(|value| value.to_string()),
            message,
            termination: None,
            termination_trigger: None,
            stdout_bytes: 0,
            stderr_bytes: 0,
            stdout_truncated: false,
            stderr_truncated: false,
            stdout_head: String::new(),
            stdout_tail: String::new(),
            stderr_head: String::new(),
            stderr_tail: String::new(),
            stdout_resource: None,
            stderr_resource: None,
            read_tool: None,
        }
    }
}

fn classify_termination(
    exit_code: i32,
    termination: &str,
    trigger: Option<&str>,
) -> (bool, CommandStatus, Option<CommandErrorKind>, bool, String) {
    if termination == "exited" {
        if exit_code == 0 {
            return (
                true,
                CommandStatus::Completed,
                None,
                false,
                "Command completed successfully.".to_owned(),
            );
        }
        return (
            false,
            CommandStatus::Failed,
            Some(CommandErrorKind::ProcessExit),
            false,
            format!("Command exited with status {exit_code}."),
        );
    }

    let message = match termination {
        "stopped" => "Command was stopped by request.".to_owned(),
        "timeout" => "Command exceeded its execution deadline.".to_owned(),
        "cancelled" => "Command was cancelled.".to_owned(),
        "forced_kill" => match trigger {
            Some(trigger) => format!("Command process tree was force-killed after {trigger}."),
            None => "Command process tree was force-killed.".to_owned(),
        },
        other => format!("Command ended with lifecycle state {other}."),
    };
    (
        false,
        CommandStatus::Stopped,
        Some(CommandErrorKind::Cancellation),
        true,
        message,
    )
}

fn append_stream_preview(target: &mut String, name: &str, head: &str, tail: &str, truncated: bool) {
    if head.is_empty() && tail.is_empty() {
        return;
    }
    target.push_str(&format!("\n{name}:\n"));
    target.push_str(head);
    if truncated {
        target.push_str("\n... output truncated; use read_job_log ...\n");
        target.push_str(tail);
    }
}

fn bounded_utf8(text: &str, max_bytes: usize) -> String {
    if text.len() <= max_bytes {
        return text.to_owned();
    }
    const SUFFIX: &str = "...";
    let mut end = max_bytes.saturating_sub(SUFFIX.len()).min(text.len());
    while end > 0 && !text.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}{SUFFIX}", &text[..end])
}

pub fn output_schema() -> Value {
    json!({
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "title": "LocalMcpCommandResultV1",
        "type": "object",
        "additionalProperties": false,
        "properties": {
            "schema_version": {"type": "integer", "const": COMMAND_RESULT_SCHEMA_VERSION},
            "ok": {"type": "boolean"},
            "status": {"type": "string", "enum": ["running", "completed", "failed", "stopped"]},
            "error_kind": nullable_enum(&[
                "process_exit",
                "spawn_error",
                "cancellation",
                "approval_denied",
                "invalid_arguments",
                "internal"
            ]),
            "exit_code": {"oneOf": [{"type": "integer"}, {"type": "null"}]},
            "retryable": {"type": "boolean"},
            "job_id": {"oneOf": [{"type": "string", "format": "uuid"}, {"type": "null"}]},
            "message": {"type": "string"},
            "termination": nullable_enum(&[
                "exited", "stopped", "timeout", "cancelled", "forced_kill", "orphaned"
            ]),
            "termination_trigger": nullable_enum(&["stop", "timeout", "cancellation", "completion"]),
            "stdout_bytes": {"type": "integer", "minimum": 0},
            "stderr_bytes": {"type": "integer", "minimum": 0},
            "stdout_truncated": {"type": "boolean"},
            "stderr_truncated": {"type": "boolean"},
            "stdout_head": {"type": "string"},
            "stdout_tail": {"type": "string"},
            "stderr_head": {"type": "string"},
            "stderr_tail": {"type": "string"},
            "stdout_resource": nullable_string(),
            "stderr_resource": nullable_string(),
            "read_tool": nullable_string()
        },
        "required": [
            "schema_version",
            "ok",
            "status",
            "error_kind",
            "exit_code",
            "retryable",
            "job_id",
            "message",
            "termination",
            "termination_trigger",
            "stdout_bytes",
            "stderr_bytes",
            "stdout_truncated",
            "stderr_truncated",
            "stdout_head",
            "stdout_tail",
            "stderr_head",
            "stderr_tail",
            "stdout_resource",
            "stderr_resource",
            "read_tool"
        ]
    })
}

fn nullable_enum(values: &[&str]) -> Value {
    json!({
        "oneOf": [
            {"type": "null"},
            {"type": "string", "enum": values}
        ]
    })
}

fn nullable_string() -> Value {
    json!({"oneOf": [{"type": "string"}, {"type": "null"}]})
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bounded(exit_code: i32, termination: &str) -> CommandOutcome {
        CommandOutcome::from_bounded_output(BoundedCommandOutput {
            job_id: Uuid::parse_str("00000000-0000-4000-8000-000000000001").unwrap(),
            exit_code,
            termination: termination.to_owned(),
            termination_trigger: (termination == "forced_kill").then(|| "completion".to_owned()),
            stdout_bytes: 7,
            stderr_bytes: 0,
            stdout_truncated: false,
            stderr_truncated: false,
            stdout_head: "output\n".to_owned(),
            stdout_tail: String::new(),
            stderr_head: String::new(),
            stderr_tail: String::new(),
            stdout_resource: "local-mcp://jobs/00000000-0000-4000-8000-000000000001/stdout"
                .to_owned(),
            stderr_resource: "local-mcp://jobs/00000000-0000-4000-8000-000000000001/stderr"
                .to_owned(),
            read_tool: "read_job_log".to_owned(),
        })
    }

    fn all_variants() -> Value {
        let job_id = Uuid::parse_str("00000000-0000-4000-8000-000000000001").unwrap();
        json!([
            CommandOutcome::running(job_id),
            bounded(0, "exited"),
            bounded(2, "exited"),
            bounded(0, "forced_kill"),
            CommandOutcome::spawn_error("executable not found"),
            CommandOutcome::cancellation(Some(job_id), "Command was stopped."),
            CommandOutcome::orphaned(job_id, "The command cannot be reattached."),
            CommandOutcome::approval_denied("Approval was denied; command was not started."),
            CommandOutcome::invalid_arguments("command must contain at least one argv entry"),
            CommandOutcome::internal("command task failed unexpectedly")
        ])
    }

    #[test]
    fn outcome_variants_match_snapshot() {
        let actual = serde_json::to_string_pretty(&all_variants()).unwrap() + "\n";
        let path = concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/snapshots/command_result_variants_v1.json"
        );
        if std::env::var_os("UPDATE_COMMAND_RESULT_SNAPSHOTS").is_some() {
            std::fs::write(path, &actual).unwrap();
            return;
        }
        assert_eq!(
            actual,
            include_str!("../tests/snapshots/command_result_variants_v1.json")
        );
    }

    #[test]
    fn output_schema_matches_snapshot() {
        let actual = serde_json::to_string_pretty(&output_schema()).unwrap() + "\n";
        let path = concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/snapshots/command_result_schema_v1.json"
        );
        if std::env::var_os("UPDATE_COMMAND_RESULT_SNAPSHOTS").is_some() {
            std::fs::write(path, &actual).unwrap();
            return;
        }
        assert_eq!(
            actual,
            include_str!("../tests/snapshots/command_result_schema_v1.json")
        );
    }

    #[test]
    fn text_fallback_is_human_readable_and_preserves_bounded_output() {
        let outcome = bounded(7, "exited");
        let result = outcome.tool_result();
        let text = result["content"][0]["text"].as_str().unwrap();
        assert!(text.contains("exited with status 7"));
        assert!(text.contains("stdout:\noutput"));
        assert_eq!(result["structuredContent"]["error_kind"], "process_exit");
        assert_eq!(result["isError"], true);
    }
}
