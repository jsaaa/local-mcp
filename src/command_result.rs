use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use uuid::Uuid;

use crate::sandbox;

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
    Timeout,
    Cancellation,
    ApprovalDenied,
    InvalidArguments,
    Internal,
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
    pub stdout: String,
    pub stderr: String,
    pub message: String,
}

impl CommandOutcome {
    pub fn running(job_id: Uuid) -> Self {
        Self::new(
            true,
            CommandStatus::Running,
            None,
            None,
            false,
            Some(job_id),
            String::new(),
            String::new(),
            format!("Command is running as job {job_id}."),
        )
    }

    pub fn from_output(output: sandbox::Output) -> Self {
        if output.status == 0 {
            Self::new(
                true,
                CommandStatus::Completed,
                None,
                Some(output.status),
                false,
                None,
                output.stdout,
                output.stderr,
                "Command completed successfully.".to_owned(),
            )
        } else {
            Self::new(
                false,
                CommandStatus::Failed,
                Some(CommandErrorKind::ProcessExit),
                Some(output.status),
                false,
                None,
                output.stdout,
                output.stderr,
                format!("Command exited with status {}.", output.status),
            )
        }
    }

    pub fn spawn_error(message: impl Into<String>) -> Self {
        let message = message.into();
        Self::failure(
            CommandErrorKind::SpawnError,
            true,
            format!("Command could not be started: {message}"),
        )
    }

    pub fn timeout(message: impl Into<String>) -> Self {
        Self::failure(CommandErrorKind::Timeout, true, message)
    }

    pub fn cancellation(job_id: Option<Uuid>, message: impl Into<String>) -> Self {
        let mut outcome = Self::new(
            false,
            CommandStatus::Stopped,
            Some(CommandErrorKind::Cancellation),
            None,
            false,
            job_id,
            String::new(),
            String::new(),
            message.into(),
        );
        outcome.retryable = true;
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
        if !self.stdout.is_empty() {
            text.push_str("\nstdout:\n");
            text.push_str(&self.stdout);
        }
        if !self.stderr.is_empty() {
            text.push_str("\nstderr:\n");
            text.push_str(&self.stderr);
        }
        text
    }

    pub fn tool_result(&self) -> Value {
        json!({
            "content": [{"type": "text", "text": self.fallback_text()}],
            "structuredContent": self,
            "isError": !self.ok,
        })
    }

    fn failure(kind: CommandErrorKind, retryable: bool, message: impl Into<String>) -> Self {
        Self::new(
            false,
            CommandStatus::Failed,
            Some(kind),
            None,
            retryable,
            None,
            String::new(),
            String::new(),
            message.into(),
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn new(
        ok: bool,
        status: CommandStatus,
        error_kind: Option<CommandErrorKind>,
        exit_code: Option<i32>,
        retryable: bool,
        job_id: Option<Uuid>,
        stdout: String,
        stderr: String,
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
            stdout,
            stderr,
            message,
        }
    }
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
            "error_kind": {
                "oneOf": [
                    {"type": "null"},
                    {"type": "string", "enum": [
                        "process_exit",
                        "spawn_error",
                        "timeout",
                        "cancellation",
                        "approval_denied",
                        "invalid_arguments",
                        "internal"
                    ]}
                ]
            },
            "exit_code": {"oneOf": [{"type": "integer"}, {"type": "null"}]},
            "retryable": {"type": "boolean"},
            "job_id": {"oneOf": [{"type": "string", "format": "uuid"}, {"type": "null"}]},
            "stdout": {"type": "string"},
            "stderr": {"type": "string"},
            "message": {"type": "string"}
        },
        "required": [
            "schema_version",
            "ok",
            "status",
            "error_kind",
            "exit_code",
            "retryable",
            "job_id",
            "stdout",
            "stderr",
            "message"
        ]
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn all_variants() -> Value {
        let job_id = Uuid::parse_str("00000000-0000-4000-8000-000000000001").unwrap();
        json!([
            CommandOutcome::running(job_id),
            CommandOutcome::from_output(sandbox::Output {
                status: 0,
                stdout: "ok\n".to_owned(),
                stderr: String::new(),
            }),
            CommandOutcome::from_output(sandbox::Output {
                status: 2,
                stdout: String::new(),
                stderr: "bad input\n".to_owned(),
            }),
            CommandOutcome::spawn_error("executable not found"),
            CommandOutcome::timeout("Command exceeded its execution deadline."),
            CommandOutcome::cancellation(Some(job_id), "Command was stopped."),
            CommandOutcome::approval_denied("Approval was denied; command was not started."),
            CommandOutcome::invalid_arguments("command must contain at least one argv entry"),
            CommandOutcome::internal("command task failed unexpectedly"),
        ])
    }

    #[test]
    fn outcome_variants_match_snapshot() {
        let expected: Value = serde_json::from_str(include_str!(
            "../tests/snapshots/command_result_variants_v1.json"
        ))
        .unwrap();
        assert_eq!(all_variants(), expected);
    }

    #[test]
    fn output_schema_matches_snapshot() {
        let expected: Value = serde_json::from_str(include_str!(
            "../tests/snapshots/command_result_schema_v1.json"
        ))
        .unwrap();
        assert_eq!(output_schema(), expected);
    }

    #[test]
    fn text_fallback_is_human_readable_and_preserves_output() {
        let outcome = CommandOutcome::from_output(sandbox::Output {
            status: 7,
            stdout: "partial".to_owned(),
            stderr: "failure".to_owned(),
        });
        let result = outcome.tool_result();
        let text = result["content"][0]["text"].as_str().unwrap();
        assert!(text.contains("exited with status 7"));
        assert!(text.contains("stdout:\npartial"));
        assert!(text.contains("stderr:\nfailure"));
        assert_eq!(result["structuredContent"]["error_kind"], "process_exit");
        assert_eq!(result["isError"], true);
    }
}
