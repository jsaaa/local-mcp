use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use schemars::JsonSchema;
use serde::de::DeserializeOwned;
use serde_json::{Value, json};
use similar::{ChangeTag, TextDiff};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::task::JoinHandle;
use uuid::Uuid;

use crate::tool_args::*;
use crate::{
    approvals, command_output, command_result,
    command_result::{CommandOutcome, CommandStatus},
    config, job_journal, sandbox, workflow,
};

const FOREGROUND_TIMEOUT: Duration = Duration::from_secs(30);
const SHELL_PROGRAM: &str = "bash";
const SHELL_PREVIEW_LIMIT: usize = 160;
const REGISTERED_JOB_WAIT_POLL_INTERVAL: Duration = Duration::from_millis(5);
const HEARTBEAT_DEFAULT_NAME: &str = "default";
const IMAGE_VIEWER_URI: &str = "ui://local-mcp/image-viewer-v1.html";
const MCP_APP_MIME_TYPE: &str = "text/html;profile=mcp-app";
const IMAGE_VIEWER_HTML: &str = r#"<!doctype html>
<html>
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<style>
  html, body { margin: 0; padding: 0; background: transparent; }
  body { font-family: var(--font-sans, system-ui, sans-serif); }
  #status { padding: 12px; color: var(--color-text-secondary, #666); }
  #image { display: block; max-width: 100%; height: auto; border-radius: var(--border-radius-md, 8px); }
</style>
</head>
<body>
<div id="status">Loading image…</div>
<img id="image" alt="Image returned by local-mcp" hidden>
<script>
(() => {
  const imageEl = document.getElementById("image");
  const statusEl = document.getElementById("status");
  const pending = new Map();
  let nextId = 1;

  function post(message) {
    window.parent.postMessage(message, "*");
  }

  function request(method, params) {
    const id = nextId++;
    post({ jsonrpc: "2.0", id, method, params });
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        pending.delete(id);
        reject(new Error(`${method} timed out`));
      }, 3000);
      pending.set(id, { resolve, reject, timer });
    });
  }

  function notify(method, params = {}) {
    post({ jsonrpc: "2.0", method, params });
  }

  function notifySize() {
    const rect = document.documentElement.getBoundingClientRect();
    notify("ui/notifications/size-changed", {
      width: Math.ceil(rect.width),
      height: Math.ceil(rect.height),
    });
  }

  function renderToolResult(result) {
    const content = Array.isArray(result?.content) ? result.content : [];
    const image = content.find((item) =>
      item && item.type === "image" && typeof item.data === "string"
    );
    if (!image) return false;

    const mimeType = typeof image.mimeType === "string" ? image.mimeType : "image/png";
    imageEl.onload = notifySize;
    imageEl.src = `data:${mimeType};base64,${image.data}`;
    imageEl.hidden = false;
    statusEl.hidden = true;
    return true;
  }

  function renderFromOpenAiCompatibilityBridge() {
    const metadata = window.openai?.toolResponseMetadata;
    return renderToolResult(metadata?.mcp_tool_result) ||
      renderToolResult(metadata?.call_tool_result);
  }

  window.addEventListener("openai:set_globals", () => {
    renderFromOpenAiCompatibilityBridge();
  }, { passive: true });

  window.addEventListener("message", (event) => {
    if (event.source !== window.parent) return;
    const message = event.data;
    if (!message || message.jsonrpc !== "2.0") return;

    if (message.id !== undefined && pending.has(message.id)) {
      const entry = pending.get(message.id);
      pending.delete(message.id);
      clearTimeout(entry.timer);
      if (message.error) entry.reject(message.error);
      else entry.resolve(message.result);
      return;
    }

    if (message.method === "ui/notifications/tool-result") {
      renderToolResult(message.params);
    }
  }, { passive: true });

  async function initialize() {
    try {
      await request("ui/initialize", {
        protocolVersion: "2026-01-26",
        appInfo: { name: "local-mcp image viewer", version: "1.0.0" },
        appCapabilities: { availableDisplayModes: ["inline"] },
      });
      notify("ui/notifications/initialized");
      return;
    } catch (_) {
      // Compatibility with hosts implementing the earlier MCP Apps handshake.
    }

    try {
      await request("initialize", {
        protocolVersion: "2026-01-26",
        clientInfo: { name: "local-mcp image viewer", version: "1.0.0" },
        capabilities: {},
      });
      notify("notifications/initialized");
    } catch (error) {
      statusEl.textContent = "Image viewer initialization failed.";
    }
  }

  renderFromOpenAiCompatibilityBridge();
  initialize();
})();
</script>
</body>
</html>"#;

struct Job {
    session_id: String,
    command: String,
    handle: JoinHandle<CommandOutcome>,
    control: sandbox::CommandControl,
}

fn jobs() -> &'static Mutex<HashMap<Uuid, Job>> {
    static JOBS: OnceLock<Mutex<HashMap<Uuid, Job>>> = OnceLock::new();
    JOBS.get_or_init(|| Mutex::new(HashMap::new()))
}

struct RegisteredJobGuard {
    job_id: Uuid,
    armed: bool,
}

impl RegisteredJobGuard {
    fn new(job_id: Uuid) -> Self {
        Self {
            job_id,
            armed: true,
        }
    }

    fn id(&self) -> Uuid {
        self.job_id
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for RegisteredJobGuard {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        if let Some(job) = jobs().lock().unwrap().remove(&self.job_id) {
            job.control.request_stop();
            job.handle.abort();
            let outcome = CommandOutcome::cancellation(
                Some(self.job_id),
                "Command was cancelled before its job ID response was delivered.",
            );
            let _ = persist_command_outcome(&job.session_id, self.job_id, &outcome);
        }
    }
}

#[derive(Debug)]
struct Heartbeat {
    interval: Duration,
    next_tick: Instant,
    delivered_ticks: u64,
    skipped_ticks: u64,
    waiting: bool,
    generation: u64,
}

impl Heartbeat {
    fn new(interval: Duration, now: Instant, generation: u64) -> Self {
        Self {
            interval,
            next_tick: now + interval,
            delivered_ticks: 0,
            skipped_ticks: 0,
            waiting: false,
            generation,
        }
    }
}

fn heartbeats() -> &'static Mutex<HashMap<(String, String), Heartbeat>> {
    static HEARTBEATS: OnceLock<Mutex<HashMap<(String, String), Heartbeat>>> = OnceLock::new();
    HEARTBEATS.get_or_init(|| Mutex::new(HashMap::new()))
}

fn skip_missed_heartbeat_ticks(heartbeat: &mut Heartbeat, now: Instant) -> u64 {
    if now < heartbeat.next_tick {
        return 0;
    }

    let interval_seconds = heartbeat.interval.as_secs();
    let overdue_seconds = now.duration_since(heartbeat.next_tick).as_secs();
    let missed = overdue_seconds / interval_seconds + 1;
    heartbeat.next_tick += Duration::from_secs(interval_seconds.saturating_mul(missed));
    heartbeat.skipped_ticks = heartbeat.skipped_ticks.saturating_add(missed);
    missed
}

pub async fn serve() -> Result<()> {
    let mut lines = BufReader::new(tokio::io::stdin()).lines();
    let mut stdout = tokio::io::stdout();
    while let Some(line) = lines.next_line().await? {
        if line.trim().is_empty() {
            continue;
        }
        let request: Value = match serde_json::from_str(&line) {
            Ok(value) => value,
            Err(error) => {
                write_message(&mut stdout, &json!({"jsonrpc":"2.0","id":null,"error":{"code":-32700,"message":error.to_string()}})).await?;
                continue;
            }
        };
        if request.get("id").is_none() {
            continue;
        }
        let id = request.get("id").cloned().unwrap_or(Value::Null);
        if !command_output::jsonrpc_id_within_budget(&id) {
            write_message(
                &mut stdout,
                &json!({
                    "jsonrpc": "2.0",
                    "id": null,
                    "error": {
                        "code": -32600,
                        "message": format!(
                            "JSON-RPC request id exceeds {} serialized bytes",
                            command_output::MAX_JSONRPC_ID_SERIALIZED_BYTES
                        )
                    }
                }),
            )
            .await?;
            continue;
        }
        let response = match dispatch(&request).await {
            Ok(result) => json!({"jsonrpc":"2.0","id":id,"result":result}),
            Err(error) => {
                json!({"jsonrpc":"2.0","id":id,"error":{"code":-32000,"message":format!("{error:#}")}})
            }
        };
        write_message(&mut stdout, &response).await?;
    }
    Ok(())
}

async fn write_message(stdout: &mut tokio::io::Stdout, message: &Value) -> Result<()> {
    stdout
        .write_all(serde_json::to_string(message)?.as_bytes())
        .await?;
    stdout.write_all(b"\n").await?;
    stdout.flush().await?;
    Ok(())
}

async fn dispatch(request: &Value) -> Result<Value> {
    match request
        .get("method")
        .and_then(Value::as_str)
        .unwrap_or_default()
    {
        "initialize" => Ok(json!({
            "protocolVersion": "2025-06-18",
            "capabilities": {
                "tools": {"listChanged": false},
                "resources": {"subscribe": false, "listChanged": false}
            },
            "serverInfo": {"name": "local-mcp", "version": env!("CARGO_PKG_VERSION")},
            "instructions": "Every tool call requires the local-mcp session_id supplied by the user. Call session_info with that ID to inspect its working directory and sandbox roots."
        })),
        "ping" => Ok(json!({})),
        "tools/list" => Ok(json!({"tools": tools()})),
        "tools/call" => call_tool(request.get("params").unwrap_or(&Value::Null)).await,
        "resources/list" => Ok(json!({"resources": resources()})),
        "resources/read" => read_resource(request.get("params").unwrap_or(&Value::Null)),
        method => anyhow::bail!("method not found: {method}"),
    }
}

fn resources() -> Value {
    json!([{
        "uri": IMAGE_VIEWER_URI,
        "name": "local_mcp_image_viewer",
        "description": "Inline viewer for images returned by get_image.",
        "mimeType": MCP_APP_MIME_TYPE
    }])
}

fn read_resource(params: &Value) -> Result<Value> {
    let uri = params
        .get("uri")
        .and_then(Value::as_str)
        .context("missing resource uri")?;
    anyhow::ensure!(uri == IMAGE_VIEWER_URI, "unknown resource: {uri}");

    Ok(json!({
        "contents": [{
            "uri": IMAGE_VIEWER_URI,
            "mimeType": MCP_APP_MIME_TYPE,
            "text": IMAGE_VIEWER_HTML,
            "_meta": {
                "ui": {"prefersBorder": false},
                "openai/widgetPrefersBorder": false,
                "openai/widgetDescription": "Displays the image returned by local-mcp."
            }
        }]
    }))
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
enum ToolName {
    SessionInfo,
    ReadFile,
    GetImage,
    ListDirectory,
    WriteFile,
    Execute,
    ExecuteShell,
    StartCommand,
    RunWorkflowStep,
    PollJob,
    StopJob,
    ReadJobLog,
    ListJobs,
    HeartbeatStart,
    HeartbeatWait,
    HeartbeatStatus,
    HeartbeatStop,
    StartWithoutSandbox,
    WithoutSandbox,
    WithoutSandboxShell,
}

impl ToolName {
    const ALL: [Self; 20] = [
        Self::SessionInfo,
        Self::ReadFile,
        Self::GetImage,
        Self::ListDirectory,
        Self::WriteFile,
        Self::Execute,
        Self::ExecuteShell,
        Self::StartCommand,
        Self::RunWorkflowStep,
        Self::PollJob,
        Self::StopJob,
        Self::ReadJobLog,
        Self::ListJobs,
        Self::HeartbeatStart,
        Self::HeartbeatWait,
        Self::HeartbeatStatus,
        Self::HeartbeatStop,
        Self::StartWithoutSandbox,
        Self::WithoutSandbox,
        Self::WithoutSandboxShell,
    ];

    fn as_str(self) -> &'static str {
        match self {
            Self::SessionInfo => "session_info",
            Self::ReadFile => "read_file",
            Self::GetImage => "get_image",
            Self::ListDirectory => "list_directory",
            Self::WriteFile => "write_file",
            Self::Execute => "execute",
            Self::ExecuteShell => "execute_shell",
            Self::StartCommand => "start_command",
            Self::RunWorkflowStep => "run_workflow_step",
            Self::PollJob => "poll_job",
            Self::StopJob => "stop_job",
            Self::ReadJobLog => "read_job_log",
            Self::ListJobs => "list_jobs",
            Self::HeartbeatStart => "heartbeat_start",
            Self::HeartbeatWait => "heartbeat_wait",
            Self::HeartbeatStatus => "heartbeat_status",
            Self::HeartbeatStop => "heartbeat_stop",
            Self::StartWithoutSandbox => "start_without_sandbox",
            Self::WithoutSandbox => "without_sandbox",
            Self::WithoutSandboxShell => "without_sandbox_shell",
        }
    }

    fn parse(value: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|tool| tool.as_str() == value)
    }

    fn has_command_output_schema(self) -> bool {
        matches!(
            self,
            Self::Execute
                | Self::ExecuteShell
                | Self::StartCommand
                | Self::PollJob
                | Self::StopJob
                | Self::StartWithoutSandbox
                | Self::WithoutSandbox
                | Self::WithoutSandboxShell
        )
    }

    fn input_schema(self) -> Value {
        match self {
            Self::SessionInfo => generated_schema::<SessionInfoArgs>(),
            Self::ReadFile => generated_schema::<ReadFileArgs>(),
            Self::GetImage => generated_schema::<GetImageArgs>(),
            Self::ListDirectory => generated_schema::<ListDirectoryArgs>(),
            Self::WriteFile => generated_schema::<WriteFileArgs>(),
            Self::Execute => generated_schema::<ExecuteArgs>(),
            Self::ExecuteShell => generated_schema::<ExecuteShellArgs>(),
            Self::StartCommand => generated_schema::<StartCommandArgs>(),
            Self::RunWorkflowStep => workflow::input_schema(),
            Self::PollJob => generated_schema::<PollJobArgs>(),
            Self::StopJob => generated_schema::<StopJobArgs>(),
            Self::ReadJobLog => generated_schema::<ReadJobLogArgs>(),
            Self::ListJobs => generated_schema::<ListJobsArgs>(),
            Self::HeartbeatStart => generated_schema::<HeartbeatStartArgs>(),
            Self::HeartbeatWait => generated_schema::<HeartbeatWaitArgs>(),
            Self::HeartbeatStatus => generated_schema::<HeartbeatStatusArgs>(),
            Self::HeartbeatStop => generated_schema::<HeartbeatStopArgs>(),
            Self::StartWithoutSandbox => generated_schema::<StartWithoutSandboxArgs>(),
            Self::WithoutSandbox => generated_schema::<WithoutSandboxArgs>(),
            Self::WithoutSandboxShell => generated_schema::<WithoutSandboxShellArgs>(),
        }
    }
}

fn generated_schema<T: JsonSchema>() -> Value {
    let mut schema = serde_json::to_value(schemars::schema_for!(T))
        .expect("tool argument schemas must serialize");
    if let Some(object) = schema.as_object_mut() {
        object.remove("$schema");
        object.remove("title");
    }
    schema
}

fn tools() -> Value {
    #[cfg(not(windows))]
    let write_file_description = "Write a UTF-8 file in the Codex sandbox. Relative paths use the session working directory.";
    #[cfg(windows)]
    let write_file_description = "Write a UTF-8 file directly on the Windows host without a Codex sandbox. Relative paths use the session working directory.";
    #[cfg(not(windows))]
    let execute_description = "Execute argv without a shell in the Codex sandbox. Returns the normal result when it finishes within 30 seconds; otherwise returns a job_id for use with poll_job or stop_job. Network is disabled and approval is not required.";
    #[cfg(windows)]
    let execute_description = "Execute argv without a shell directly on the Windows host. Returns the normal result when it finishes within 30 seconds; otherwise returns a job_id for use with poll_job or stop_job. This has the user's filesystem and network access and requires approval unless the session is in yolo mode.";
    #[cfg(not(windows))]
    let start_command_description = "Start argv immediately as a background job in the Codex sandbox and return a job_id without waiting for completion. Network is disabled and approval is not required.";
    #[cfg(windows)]
    let start_command_description = "Start argv immediately as a background job directly on the Windows host and return a job_id without waiting for completion. This has the user's filesystem and network access and requires approval unless the session is in yolo mode.";

    #[cfg(not(windows))]
    let execute_shell_description = "Execute a multi-line Bash program in the Codex sandbox. Pass the program in script as a string; pipelines, heredocs, and set -euo pipefail are supported. Returns the normal result within 30 seconds or a job_id afterward. Network is disabled and approval is not required.";
    #[cfg(windows)]
    let execute_shell_description = "Shell-program execution is unsupported on Windows. Use execute with an explicit PowerShell argv command instead.";
    #[cfg(not(windows))]
    let without_sandbox_shell_description = "Execute a multi-line Bash program directly on the host with full user permissions and network access. Pass the program in script as a string. Approval is required before launch unless the session is in yolo mode; long-running work returns a job_id.";
    #[cfg(windows)]
    let without_sandbox_shell_description = "Shell-program execution is unsupported on Windows. Use without_sandbox with an explicit PowerShell argv command instead.";

    #[cfg(not(windows))]
    let workflow_description = "Run one sandboxed argv workflow step with fail-closed dependency, required-file, expected-output, idempotency, and persistent state gates. The command starts only after every prerequisite is satisfied.";
    #[cfg(windows)]
    let workflow_description = "Fail closed without starting a process: run_workflow_step is unsupported on Windows because this release cannot provide its required filesystem/network sandbox.";

    let values = ToolName::ALL
        .into_iter()
        .map(|tool| {
            let description = match tool {
                ToolName::SessionInfo => {
                    "Show a local-mcp session's ID, working directory, and allowed sandbox roots."
                }
                ToolName::ReadFile => {
                    "Read a UTF-8 file from the local machine. Relative paths use the session working directory."
                }
                ToolName::GetImage => {
                    "Read a local image and return it as MCP image content. Relative paths use the session working directory."
                }
                ToolName::ListDirectory => {
                    "List entries in a local directory. Relative paths use the session working directory."
                }
                ToolName::WriteFile => write_file_description,
                ToolName::Execute => execute_description,
                ToolName::ExecuteShell => execute_shell_description,
                ToolName::StartCommand => start_command_description,
                ToolName::RunWorkflowStep => workflow_description,
                ToolName::PollJob => {
                    "Poll a background command returned by execute or start_command. Returns running while active, or the persisted terminal result; completed, failed, stopped, and orphaned states remain queryable after restart."
                }
                ToolName::StopJob => {
                    "Stop a background command returned by execute or start_command. The command's complete process tree receives a graceful stop followed by forced termination after a bounded grace period; repeated calls return the persisted terminal result."
                }
                ToolName::ReadJobLog => {
                    "Read a bounded byte range from a command's stored stdout or stderr. Use the job_id returned in foreground/background results; access is scoped to the supplied session_id."
                }
                ToolName::ListJobs => {
                    "List persisted jobs for this session with bounded pagination and an optional lifecycle-state filter. Running records without an in-process handle are reported as orphaned."
                }
                ToolName::HeartbeatStart => {
                    "Start or reset an in-turn heartbeat schedule for this local-mcp session. After starting it, call heartbeat_wait repeatedly. A tick is delivered only while heartbeat_wait is actively waiting; ticks that occur while the agent is busy doing work are skipped instead of queued."
                }
                ToolName::HeartbeatWait => {
                    "Wait for the next heartbeat tick in short long-poll chunks. Call this repeatedly until status is tick, then do one work cycle and call it again. If a scheduled tick passes while no heartbeat_wait call is active because the agent is still working, that tick is skipped. This does not revive a ChatGPT turn after the turn has ended."
                }
                ToolName::HeartbeatStatus => {
                    "Show the current in-turn heartbeat schedule, delivered tick count, skipped tick count, and time until the next tick."
                }
                ToolName::HeartbeatStop => {
                    "Stop and remove an in-turn heartbeat schedule for this local-mcp session."
                }
                ToolName::StartWithoutSandbox => {
                    "After approval, start argv immediately as an unrestricted host background job and return a job_id. The process has full host permissions and network access; denial never starts a process or creates a job."
                }
                ToolName::WithoutSandbox => {
                    "Execute argv directly on the host with full user permissions and network access. Every call requires approval unless the session is in yolo mode."
                }
                ToolName::WithoutSandboxShell => without_sandbox_shell_description,
            };
            let mut definition = json!({
                "name": tool.as_str(),
                "description": description,
                "inputSchema": tool.input_schema(),
            });
            if tool == ToolName::GetImage {
                definition["_meta"] = json!({
                    "ui": {"resourceUri": IMAGE_VIEWER_URI, "visibility": ["model", "app"]},
                    "openai/outputTemplate": IMAGE_VIEWER_URI,
                    "openai/toolInvocation/invoking": "Reading image…",
                    "openai/toolInvocation/invoked": "Image ready"
                });
            }
            if tool.has_command_output_schema() {
                definition["outputSchema"] = command_result::output_schema();
            }
            definition
        })
        .collect();
    Value::Array(values)
}

fn parse_arguments<T: DeserializeOwned>(tool: ToolName, args: &Value) -> Result<T> {
    serde_json::from_value(args.clone())
        .with_context(|| format!("invalid arguments for {}", tool.as_str()))
}

fn parse_command_arguments<T: DeserializeOwned>(
    tool: ToolName,
    args: &Value,
) -> std::result::Result<T, Value> {
    parse_arguments(tool, args)
        .map_err(|error| CommandOutcome::invalid_arguments(format!("{error:#}")).tool_result())
}

async fn call_tool(params: &Value) -> Result<Value> {
    let name = params
        .get("name")
        .and_then(Value::as_str)
        .context("missing tool name")?;
    let tool = ToolName::parse(name).with_context(|| format!("unknown tool: {name}"))?;
    let raw_args = params
        .get("arguments")
        .cloned()
        .unwrap_or_else(|| json!({}));

    match tool {
        ToolName::SessionInfo => {
            let args: SessionInfoArgs = parse_arguments(tool, &raw_args)?;
            let session = config::load_session(args.session_id.as_str()).await?;
            approvals::activity(&session.id, "Read session info", None).await;
            text_result(serde_json::to_string_pretty(&session)?)
        }
        ToolName::GetImage => {
            let args: GetImageArgs = parse_arguments(tool, &raw_args)?;
            let session = config::load_session(args.session_id.as_str()).await?;
            let path = resolve_path(&session.cwd, PathBuf::from(&args.path));
            let result = get_image(&path).await;
            report_result(
                &session.id,
                format!("Read image {}", display_path(&path, &session.cwd)),
                &result,
            )
            .await;
            result
        }
        ToolName::ReadFile => {
            let args: ReadFileArgs = parse_arguments(tool, &raw_args)?;
            let session = config::load_session(args.session_id.as_str()).await?;
            let path = resolve_path(&session.cwd, PathBuf::from(&args.path));
            let result = tokio::fs::read_to_string(&path)
                .await
                .context("failed to read file");
            report_result(
                &session.id,
                format!("Read {}", display_path(&path, &session.cwd)),
                &result,
            )
            .await;
            text_result(result?)
        }
        ToolName::ListDirectory => {
            let args: ListDirectoryArgs = parse_arguments(tool, &raw_args)?;
            let session = config::load_session(args.session_id.as_str()).await?;
            let path = resolve_path(&session.cwd, PathBuf::from(&args.path));
            let result = list_directory(&path).await;
            report_result(
                &session.id,
                format!("Listed {}", display_path(&path, &session.cwd)),
                &result,
            )
            .await;
            text_result(result?)
        }
        ToolName::WriteFile => {
            let args: WriteFileArgs = parse_arguments(tool, &raw_args)?;
            let session = config::load_session(args.session_id.as_str()).await?;
            write_file(&args, &session).await
        }
        ToolName::Execute => {
            let args: ExecuteArgs = match parse_command_arguments(tool, &raw_args) {
                Ok(args) => args,
                Err(result) => return Ok(result),
            };
            let session = config::load_session(args.session_id.as_str()).await?;
            execute(&args, &session).await
        }
        ToolName::ExecuteShell => {
            let args: ExecuteShellArgs = match parse_command_arguments(tool, &raw_args) {
                Ok(args) => args,
                Err(result) => return Ok(result),
            };
            let session = config::load_session(args.session_id.as_str()).await?;
            execute_shell(&args, &session).await
        }
        ToolName::StartCommand => {
            let args: StartCommandArgs = match parse_command_arguments(tool, &raw_args) {
                Ok(args) => args,
                Err(result) => return Ok(result),
            };
            let session = config::load_session(args.session_id.as_str()).await?;
            start_command(&args, &session).await
        }
        ToolName::RunWorkflowStep => {
            let args: workflow::WorkflowStepArgs = parse_arguments(tool, &raw_args)?;
            let session = config::load_session(&args.session_id).await?;
            run_workflow_step(args, &session).await
        }
        ToolName::PollJob => {
            let args: PollJobArgs = match parse_command_arguments(tool, &raw_args) {
                Ok(args) => args,
                Err(result) => return Ok(result),
            };
            let session = config::load_session(args.session_id.as_str()).await?;
            poll_job(&args, &session).await
        }
        ToolName::StopJob => {
            let args: StopJobArgs = match parse_command_arguments(tool, &raw_args) {
                Ok(args) => args,
                Err(result) => return Ok(result),
            };
            let session = config::load_session(args.session_id.as_str()).await?;
            stop_job(&args, &session).await
        }
        ToolName::ReadJobLog => {
            let args: ReadJobLogArgs = parse_arguments(tool, &raw_args)?;
            let session = config::load_session(args.session_id.as_str()).await?;
            read_job_log(&args, &session).await
        }
        ToolName::ListJobs => {
            let args: ListJobsArgs = parse_arguments(tool, &raw_args)?;
            let session = config::load_session(args.session_id.as_str()).await?;
            list_jobs(&args, &session).await
        }
        ToolName::HeartbeatStart => {
            let args: HeartbeatStartArgs = parse_arguments(tool, &raw_args)?;
            let session = config::load_session(args.session_id.as_str()).await?;
            heartbeat_start(&args, &session).await
        }
        ToolName::HeartbeatWait => {
            let args: HeartbeatWaitArgs = parse_arguments(tool, &raw_args)?;
            let session = config::load_session(args.session_id.as_str()).await?;
            heartbeat_wait(&args, &session).await
        }
        ToolName::HeartbeatStatus => {
            let args: HeartbeatStatusArgs = parse_arguments(tool, &raw_args)?;
            let session = config::load_session(args.session_id.as_str()).await?;
            heartbeat_status(&args, &session).await
        }
        ToolName::HeartbeatStop => {
            let args: HeartbeatStopArgs = parse_arguments(tool, &raw_args)?;
            let session = config::load_session(args.session_id.as_str()).await?;
            heartbeat_stop(&args, &session).await
        }
        ToolName::StartWithoutSandbox => {
            let args: StartWithoutSandboxArgs = match parse_command_arguments(tool, &raw_args) {
                Ok(args) => args,
                Err(result) => return Ok(result),
            };
            let session = config::load_session(args.session_id.as_str()).await?;
            start_without_sandbox(&args, &session).await
        }
        ToolName::WithoutSandbox => {
            let args: WithoutSandboxArgs = match parse_command_arguments(tool, &raw_args) {
                Ok(args) => args,
                Err(result) => return Ok(result),
            };
            let session = config::load_session(args.session_id.as_str()).await?;
            without_sandbox(&args, &session).await
        }
        ToolName::WithoutSandboxShell => {
            let args: WithoutSandboxShellArgs = match parse_command_arguments(tool, &raw_args) {
                Ok(args) => args,
                Err(result) => return Ok(result),
            };
            let session = config::load_session(args.session_id.as_str()).await?;
            without_sandbox_shell(&args, &session).await
        }
    }
}

fn heartbeat_name(name: Option<&HeartbeatName>) -> String {
    name.map(HeartbeatName::as_str)
        .unwrap_or(HEARTBEAT_DEFAULT_NAME)
        .to_owned()
}

fn heartbeat_key(session_id: &str, name: &str) -> (String, String) {
    (session_id.to_owned(), name.to_owned())
}

fn heartbeat_interval(interval: HeartbeatInterval) -> Duration {
    Duration::from_secs(interval.seconds())
}

fn heartbeat_max_wait(wait: Option<HeartbeatWait>) -> Duration {
    Duration::from_secs(
        wait.map(HeartbeatWait::seconds)
            .unwrap_or(HEARTBEAT_MAX_WAIT_SECONDS),
    )
}

fn heartbeat_result(
    name: &str,
    status: &str,
    heartbeat: &Heartbeat,
    now: Instant,
) -> Result<Value> {
    let remaining = heartbeat.next_tick.saturating_duration_since(now);
    text_result(serde_json::to_string_pretty(&json!({
        "name": name,
        "status": status,
        "interval_seconds": heartbeat.interval.as_secs(),
        "next_tick_in_ms": remaining.as_millis().min(u64::MAX as u128) as u64,
        "delivered_ticks": heartbeat.delivered_ticks,
        "skipped_ticks": heartbeat.skipped_ticks,
    }))?)
}

async fn heartbeat_start(args: &HeartbeatStartArgs, session: &config::Session) -> Result<Value> {
    let name = heartbeat_name(args.name.as_ref());
    let interval = heartbeat_interval(args.interval_seconds);
    let key = heartbeat_key(&session.id, &name);
    let now = Instant::now();
    let result = {
        let mut all = heartbeats().lock().unwrap();
        let generation = all
            .get(&key)
            .map(|heartbeat| heartbeat.generation.saturating_add(1))
            .unwrap_or(1);
        all.insert(key.clone(), Heartbeat::new(interval, now, generation));
        heartbeat_result(&name, "started", all.get(&key).unwrap(), now)?
    };
    approvals::activity(
        &session.id,
        format!("Started heartbeat {name} every {}s", interval.as_secs()),
        None,
    )
    .await;
    Ok(result)
}

async fn heartbeat_wait(args: &HeartbeatWaitArgs, session: &config::Session) -> Result<Value> {
    let name = heartbeat_name(args.name.as_ref());
    let max_wait = heartbeat_max_wait(args.max_wait_seconds);
    let key = heartbeat_key(&session.id, &name);

    let (target, generation, wait_for) = {
        let mut all = heartbeats().lock().unwrap();
        let heartbeat = all.get_mut(&key).with_context(|| {
            format!("heartbeat {name:?} is not running; call heartbeat_start first")
        })?;
        anyhow::ensure!(
            !heartbeat.waiting,
            "heartbeat {name:?} already has an active waiter"
        );
        skip_missed_heartbeat_ticks(heartbeat, Instant::now());
        heartbeat.waiting = true;
        let target = heartbeat.next_tick;
        let wait_for = target
            .saturating_duration_since(Instant::now())
            .min(max_wait);
        (target, heartbeat.generation, wait_for)
    };

    tokio::time::sleep(wait_for).await;
    let now = Instant::now();
    let (ticked, result) = {
        let mut all = heartbeats().lock().unwrap();
        let heartbeat = all
            .get_mut(&key)
            .with_context(|| format!("heartbeat {name:?} was stopped while waiting"))?;
        anyhow::ensure!(
            heartbeat.generation == generation,
            "heartbeat {name:?} was restarted while waiting"
        );
        heartbeat.waiting = false;

        if now >= target && heartbeat.next_tick == target {
            heartbeat.delivered_ticks = heartbeat.delivered_ticks.saturating_add(1);
            heartbeat.next_tick += heartbeat.interval;
            skip_missed_heartbeat_ticks(heartbeat, now);
            (true, heartbeat_result(&name, "tick", heartbeat, now))
        } else {
            (false, heartbeat_result(&name, "waiting", heartbeat, now))
        }
    };

    if ticked {
        approvals::activity(&session.id, format!("Heartbeat {name} tick"), None).await;
    }
    result
}

async fn heartbeat_status(args: &HeartbeatStatusArgs, session: &config::Session) -> Result<Value> {
    let name = heartbeat_name(args.name.as_ref());
    let key = heartbeat_key(&session.id, &name);
    let now = Instant::now();
    let mut all = heartbeats().lock().unwrap();
    let heartbeat = all
        .get_mut(&key)
        .with_context(|| format!("heartbeat {name:?} is not running"))?;
    if !heartbeat.waiting {
        skip_missed_heartbeat_ticks(heartbeat, now);
    }
    heartbeat_result(
        &name,
        if heartbeat.waiting { "waiting" } else { "idle" },
        heartbeat,
        now,
    )
}

async fn heartbeat_stop(args: &HeartbeatStopArgs, session: &config::Session) -> Result<Value> {
    let name = heartbeat_name(args.name.as_ref());
    let key = heartbeat_key(&session.id, &name);
    let heartbeat = heartbeats()
        .lock()
        .unwrap()
        .remove(&key)
        .with_context(|| format!("heartbeat {name:?} is not running"))?;
    approvals::activity(&session.id, format!("Stopped heartbeat {name}"), None).await;
    text_result(serde_json::to_string_pretty(&json!({
        "name": name,
        "status": "stopped",
        "delivered_ticks": heartbeat.delivered_ticks,
        "skipped_ticks": heartbeat.skipped_ticks,
    }))?)
}

async fn report_result<T>(session_id: &str, title: String, result: &Result<T>) {
    let detail = result
        .as_ref()
        .err()
        .map(|error| format!("└ Error: {error:#}"));
    approvals::activity(session_id, title, detail).await;
}

fn display_path<'a>(path: &'a Path, session_cwd: &Path) -> std::borrow::Cow<'a, str> {
    path.strip_prefix(session_cwd)
        .unwrap_or(path)
        .to_string_lossy()
}

fn text_result(text: String) -> Result<Value> {
    Ok(json!({"content":[{"type":"text","text":text}]}))
}

async fn get_image(path: &Path) -> Result<Value> {
    let path = tokio::fs::canonicalize(&path)
        .await
        .with_context(|| format!("cannot resolve image {}", path.display()))?;
    let metadata = tokio::fs::metadata(&path).await?;
    anyhow::ensure!(
        metadata.is_file(),
        "image path is not a file: {}",
        path.display()
    );
    let bytes = tokio::fs::read(&path)
        .await
        .with_context(|| format!("cannot read image {}", path.display()))?;
    let mime_type = image_mime_type(&bytes)
        .with_context(|| format!("unsupported image format: {}", path.display()))?;
    Ok(json!({
        "content": [{
            "type": "image",
            "data": STANDARD.encode(bytes),
            "mimeType": mime_type
        }]
    }))
}

fn image_mime_type(bytes: &[u8]) -> Option<&'static str> {
    if bytes.starts_with(b"\x89PNG\r\n\x1a\n") {
        Some("image/png")
    } else if bytes.starts_with(b"\xff\xd8\xff") {
        Some("image/jpeg")
    } else if bytes.starts_with(b"GIF87a") || bytes.starts_with(b"GIF89a") {
        Some("image/gif")
    } else if bytes.len() >= 12 && &bytes[..4] == b"RIFF" && &bytes[8..12] == b"WEBP" {
        Some("image/webp")
    } else if bytes.starts_with(b"BM") {
        Some("image/bmp")
    } else if bytes.starts_with(b"II*\0") || bytes.starts_with(b"MM\0*") {
        Some("image/tiff")
    } else if bytes.len() >= 12
        && &bytes[4..8] == b"ftyp"
        && matches!(&bytes[8..12], b"avif" | b"avis")
    {
        Some("image/avif")
    } else {
        None
    }
}

fn resolve_path(session_cwd: &Path, path: PathBuf) -> PathBuf {
    if path.is_absolute() {
        path
    } else {
        session_cwd.join(path)
    }
}

fn cwd(requested: Option<&str>, session_cwd: &Path) -> Result<PathBuf> {
    let path = requested
        .map(PathBuf::from)
        .map(|path| resolve_path(session_cwd, path))
        .unwrap_or_else(|| session_cwd.to_owned());
    std::fs::canonicalize(&path).with_context(|| format!("cannot resolve cwd {}", path.display()))
}

async fn list_directory(path: &Path) -> Result<String> {
    let mut entries = tokio::fs::read_dir(path).await?;
    let mut names = Vec::new();
    while let Some(entry) = entries.next_entry().await? {
        let suffix = if entry.file_type().await?.is_dir() {
            "/"
        } else {
            ""
        };
        names.push(format!("{}{}", entry.file_name().to_string_lossy(), suffix));
    }
    names.sort();
    Ok(names.join("\n"))
}

async fn write_file(args: &WriteFileArgs, session: &config::Session) -> Result<Value> {
    let absolute = resolve_path(&session.cwd, PathBuf::from(&args.path));
    let parent = absolute.parent().context("file has no parent directory")?;
    let parent = std::fs::canonicalize(parent)
        .with_context(|| format!("parent does not exist: {}", parent.display()))?;
    let content = args.content.as_str();
    let previous = tokio::fs::read_to_string(&absolute)
        .await
        .unwrap_or_default();
    #[cfg(unix)]
    let output = {
        let command = vec![
            "sh".to_owned(),
            "-c".to_owned(),
            "cat > \"$1\"".to_owned(),
            "local-mcp-write".to_owned(),
            absolute.display().to_string(),
        ];
        sandbox::run(
            &command,
            &parent,
            std::slice::from_ref(&parent),
            Some(content.as_bytes()),
        )
        .await?
    };
    #[cfg(windows)]
    let output = {
        // Windows has no application sandbox here, so avoid depending on a
        // shell utility for the file-edit operation.
        tokio::fs::write(&absolute, content).await?;
        sandbox::Output {
            status: 0,
            stdout: String::new(),
            stderr: String::new(),
            termination: sandbox::Termination::Exited,
        }
    };
    let result = render_output(output);
    let (added, removed, diff) = render_diff(&previous, content);
    let title = format!(
        "Edited {} (+{added} -{removed})",
        display_path(&absolute, &session.cwd)
    );
    let detail = match &result {
        Ok(_) => (!diff.is_empty()).then_some(diff),
        Err(error) => Some(format!("└ Error: {error:#}")),
    };
    approvals::activity(&session.id, title, detail).await;
    text_result(result?)
}

fn argv_execution_mode() -> job_journal::ExecutionMode {
    #[cfg(windows)]
    {
        job_journal::ExecutionMode::Unrestricted
    }
    #[cfg(not(windows))]
    {
        job_journal::ExecutionMode::Sandboxed
    }
}

async fn execute(args: &ExecuteArgs, session: &config::Session) -> Result<Value> {
    let (job_id, rendered_command, mut handle, control) = match spawn_sandboxed_command(
        "execute",
        args.command.as_slice(),
        args.cwd.as_deref(),
        session,
    )
    .await
    {
        Ok(job) => job,
        Err(outcome) => return Ok(outcome.tool_result()),
    };

    match tokio::time::timeout(FOREGROUND_TIMEOUT, &mut handle).await {
        Ok(joined) => {
            drop(control);
            let outcome = settle_joined_outcome(&session.id, job_id, joined);
            Ok(outcome.tool_result())
        }
        Err(_) => {
            store_job(
                job_id,
                session,
                rendered_command,
                handle,
                control,
                "Backgrounded",
            )
            .await
        }
    }
}

async fn run_workflow_step(
    args: workflow::WorkflowStepArgs,
    session: &config::Session,
) -> Result<Value> {
    let step_id = args.step_id.clone();
    let result = workflow::run(args, session).await?;
    approvals::activity(
        &session.id,
        format!("Workflow step {step_id}: {}", result.status.as_str()),
        result.error.as_ref().map(|error| format!("└ {error}")),
    )
    .await;
    text_result(serde_json::to_string_pretty(&result)?)
}

async fn execute_shell(args: &ExecuteShellArgs, session: &config::Session) -> Result<Value> {
    let command = match shell_command(&args.script) {
        Ok(command) => command,
        Err(error) => {
            return Ok(CommandOutcome::invalid_arguments(format!("{error:#}")).tool_result());
        }
    };
    let rendered_command = shell_activity_label(&args.script);
    let approval_detail = format!("shell: {SHELL_PROGRAM}\n{rendered_command}");
    let (job_id, rendered_command, mut handle, control) = match spawn_sandboxed_command_named(
        "execute_shell",
        command,
        args.cwd.as_deref(),
        rendered_command,
        approval_detail,
        session,
    )
    .await
    {
        Ok(job) => job,
        Err(outcome) => return Ok(outcome.tool_result()),
    };

    match tokio::time::timeout(FOREGROUND_TIMEOUT, &mut handle).await {
        Ok(joined) => {
            drop(control);
            let outcome = settle_joined_outcome(&session.id, job_id, joined);
            Ok(outcome.tool_result())
        }
        Err(_) => {
            store_job(
                job_id,
                session,
                rendered_command,
                handle,
                control,
                "Backgrounded",
            )
            .await
        }
    }
}

async fn start_command(args: &StartCommandArgs, session: &config::Session) -> Result<Value> {
    let (job_id, rendered_command, handle, control) = match spawn_sandboxed_command(
        "start_command",
        args.command.as_slice(),
        args.cwd.as_deref(),
        session,
    )
    .await
    {
        Ok(job) => job,
        Err(outcome) => return Ok(outcome.tool_result()),
    };
    store_job(
        job_id,
        session,
        rendered_command,
        handle,
        control,
        "Started",
    )
    .await
}

async fn spawn_sandboxed_command(
    operation: &str,
    command: &[String],
    requested_cwd: Option<&str>,
    session: &config::Session,
) -> std::result::Result<
    (
        Uuid,
        String,
        JoinHandle<CommandOutcome>,
        sandbox::CommandControl,
    ),
    CommandOutcome,
> {
    let command = command.to_vec();
    let rendered_command = render_command(&command);
    let approval_detail = format!("argv preview: {rendered_command}");
    spawn_sandboxed_command_named(
        operation,
        command,
        requested_cwd,
        rendered_command,
        approval_detail,
        session,
    )
    .await
}

async fn spawn_sandboxed_command_named(
    operation: &str,
    command: Vec<String>,
    requested_cwd: Option<&str>,
    rendered_command: String,
    approval_detail: String,
    session: &config::Session,
) -> std::result::Result<
    (
        Uuid,
        String,
        JoinHandle<CommandOutcome>,
        sandbox::CommandControl,
    ),
    CommandOutcome,
> {
    let cwd = cwd(requested_cwd, &session.cwd)
        .map_err(|error| CommandOutcome::invalid_arguments(format!("{error:#}")))?;
    #[cfg(windows)]
    {
        let approved = approvals::request(&session.id, operation, approval_detail, cwd.clone())
            .await
            .map_err(|error| {
                CommandOutcome::internal(format!("Approval request failed: {error:#}"))
            })?;
        if !approved {
            return Err(CommandOutcome::approval_denied(format!(
                "Approval was denied for {operation}; command was not started."
            )));
        }
    }
    #[cfg(not(windows))]
    let _ = (operation, approval_detail);
    let mut roots = session.permitted_directories.clone();
    if !roots.iter().any(|root| cwd.starts_with(root)) {
        roots.push(cwd.clone());
    }
    let job_id = Uuid::new_v4();
    job_journal::create_running(
        &session.id,
        job_id,
        rendered_command.clone(),
        cwd.clone(),
        argv_execution_mode(),
    )
    .map_err(|error| {
        CommandOutcome::internal(format!("Could not reserve job journal state: {error:#}"))
            .with_job_id(job_id)
    })?;
    approvals::activity(&session.id, format!("Running {rendered_command}"), None).await;
    let log_session_id = session.id.clone();
    let report_session_id = session.id.clone();
    let task_command = rendered_command.clone();
    let (control, cancellation) = sandbox::command_control();
    let handle = tokio::spawn(async move {
        let outcome = match command_output::prepare_log_capture(&log_session_id, job_id).await {
            Ok(logs) => match sandbox::run_logged_controlled(
                &command,
                &cwd,
                &roots,
                None,
                cancellation,
                None,
                logs.stdout(),
                logs.stderr(),
                command_output::capture_preview_limit(),
            )
            .await
            {
                Ok(output) => command_output::outcome_from_logged_output(job_id, output),
                Err(error) => CommandOutcome::spawn_error(format!("{error:#}")).with_job_id(job_id),
            },
            Err(error) => CommandOutcome::internal(format!(
                "Could not prepare command log capture: {error:#}"
            ))
            .with_job_id(job_id),
        };
        let final_outcome = match persist_command_outcome(&report_session_id, job_id, &outcome) {
            Ok(_) => outcome,
            Err(error) => CommandOutcome::internal(format!(
                "Command completed but its result could not be persisted: {error:#}"
            ))
            .with_job_id(job_id),
        };
        report_command_finished(report_session_id, &task_command, &final_outcome).await;
        final_outcome
    });
    Ok((job_id, rendered_command, handle, control))
}

async fn store_job(
    job_id: Uuid,
    session: &config::Session,
    rendered_command: String,
    handle: JoinHandle<CommandOutcome>,
    control: sandbox::CommandControl,
    activity: &str,
) -> Result<Value> {
    jobs().lock().unwrap().insert(
        job_id,
        Job {
            session_id: session.id.clone(),
            command: rendered_command.clone(),
            handle,
            control,
        },
    );
    approvals::activity(
        &session.id,
        format!("{activity} {rendered_command}"),
        Some(format!("└ job {job_id}")),
    )
    .await;
    Ok(CommandOutcome::running(job_id).tool_result())
}

async fn poll_job(args: &PollJobArgs, session: &config::Session) -> Result<Value> {
    let job_id = args.job_id;
    let in_memory = {
        let jobs = jobs().lock().unwrap();
        match jobs.get(&job_id) {
            Some(job) if job.session_id == session.id => Some(job.handle.is_finished()),
            Some(_) => {
                return Ok(CommandOutcome::invalid_arguments(
                    "job does not belong to this session",
                )
                .with_job_id(job_id)
                .tool_result());
            }
            None => None,
        }
    };

    let outcome = match in_memory {
        None => match job_journal::load(&session.id, job_id, false) {
            Ok(record) => persisted_job_outcome(record),
            Err(error) => {
                CommandOutcome::invalid_arguments(format!("{error:#}")).with_job_id(job_id)
            }
        },
        Some(false) => CommandOutcome::running(job_id),
        Some(true) => {
            let job = jobs().lock().unwrap().remove(&job_id).unwrap();
            settle_joined_outcome(&session.id, job_id, job.handle.await)
        }
    };
    Ok(outcome.tool_result())
}

async fn stop_job(args: &StopJobArgs, session: &config::Session) -> Result<Value> {
    let job_id = args.job_id;
    let job = {
        let mut all_jobs = jobs().lock().unwrap();
        match all_jobs.get(&job_id) {
            Some(job) if job.session_id == session.id => all_jobs.remove(&job_id),
            Some(_) => {
                return Ok(CommandOutcome::invalid_arguments(
                    "job does not belong to this session",
                )
                .with_job_id(job_id)
                .tool_result());
            }
            None => None,
        }
    };

    let Some(job) = job else {
        let outcome = match job_journal::load(&session.id, job_id, false) {
            Ok(record) => persisted_job_outcome(record),
            Err(error) => {
                CommandOutcome::invalid_arguments(format!("{error:#}")).with_job_id(job_id)
            }
        };
        return Ok(outcome.tool_result());
    };

    let command = job.command.clone();
    if !job.handle.is_finished() {
        job.control.request_stop();
    }
    let outcome = settle_joined_outcome(&session.id, job_id, job.handle.await);
    approvals::activity(
        &session.id,
        if outcome.status == CommandStatus::Stopped {
            format!("Stopped {command}")
        } else {
            format!("Stop skipped for finished {command}")
        },
        Some(format!(
            "└ job {job_id}; status {:?}; termination {}",
            outcome.status,
            outcome.termination.as_deref().unwrap_or("none")
        )),
    )
    .await;
    Ok(outcome.tool_result())
}

fn persist_command_outcome(
    session_id: &str,
    job_id: Uuid,
    outcome: &CommandOutcome,
) -> Result<job_journal::JobRecord> {
    let serialized = serde_json::to_string(outcome)?;
    let journal_result: Result<String> = if outcome.status == CommandStatus::Failed {
        Err(anyhow::anyhow!(serialized))
    } else {
        Ok(serialized)
    };
    job_journal::finish(session_id, job_id, &journal_result)
}

fn settle_joined_outcome(
    session_id: &str,
    job_id: Uuid,
    joined: std::result::Result<CommandOutcome, tokio::task::JoinError>,
) -> CommandOutcome {
    let outcome = match joined {
        Ok(outcome) => outcome,
        Err(error) if error.is_cancelled() => {
            CommandOutcome::cancellation(Some(job_id), "Command task was cancelled.")
        }
        Err(error) => {
            CommandOutcome::internal(format!("Command task failed: {error}")).with_job_id(job_id)
        }
    };
    match persist_command_outcome(session_id, job_id, &outcome) {
        Ok(_) => outcome,
        Err(error) => {
            CommandOutcome::internal(format!("Command outcome could not be persisted: {error:#}"))
                .with_job_id(job_id)
        }
    }
}

fn persisted_job_outcome(record: job_journal::JobRecord) -> CommandOutcome {
    let job_id = match Uuid::parse_str(&record.job_id) {
        Ok(job_id) => job_id,
        Err(error) => {
            return CommandOutcome::internal(format!("Persisted job has an invalid ID: {error}"));
        }
    };
    match record.state {
        job_journal::JobState::Running => CommandOutcome::running(job_id),
        job_journal::JobState::Orphaned => CommandOutcome::orphaned(
            job_id,
            record
                .error
                .unwrap_or_else(|| "The command cannot be reattached.".to_owned()),
        ),
        job_journal::JobState::Stopped
        | job_journal::JobState::Completed
        | job_journal::JobState::Failed => {
            let payload = record.result.or(record.error);
            match payload {
                Some(payload) => {
                    serde_json::from_str::<CommandOutcome>(&payload).unwrap_or_else(|error| {
                        CommandOutcome::internal(format!(
                            "Persisted command outcome is unavailable or corrupt: {error}"
                        ))
                        .with_job_id(job_id)
                    })
                }
                None if record.state == job_journal::JobState::Stopped => {
                    CommandOutcome::cancellation(Some(job_id), "Command was stopped.")
                }
                None => CommandOutcome::internal("Persisted job has no result payload")
                    .with_job_id(job_id),
            }
        }
    }
}

fn job_state_filter(state: JobStateFilter) -> job_journal::JobState {
    match state {
        JobStateFilter::Running => job_journal::JobState::Running,
        JobStateFilter::Completed => job_journal::JobState::Completed,
        JobStateFilter::Failed => job_journal::JobState::Failed,
        JobStateFilter::Stopped => job_journal::JobState::Stopped,
        JobStateFilter::Orphaned => job_journal::JobState::Orphaned,
    }
}

async fn list_jobs(args: &ListJobsArgs, session: &config::Session) -> Result<Value> {
    let state = args.state.map(job_state_filter);
    let offset = args.offset.unwrap_or(0);
    anyhow::ensure!(offset <= usize::MAX as u64, "offset is too large");
    let limit = args.limit.map(JobListLimit::value).unwrap_or(50);
    let active_jobs = {
        let jobs = jobs().lock().unwrap();
        jobs.iter()
            .filter_map(|(job_id, job)| (job.session_id == session.id).then_some(*job_id))
            .collect::<HashSet<_>>()
    };
    let page = job_journal::list(&session.id, &active_jobs, state, offset as usize, limit)?;
    approvals::activity(
        &session.id,
        "Listed persisted jobs",
        Some(format!("└ {} matching jobs", page.total)),
    )
    .await;
    text_result(serde_json::to_string_pretty(&page)?)
}

async fn read_job_log(args: &ReadJobLogArgs, session: &config::Session) -> Result<Value> {
    let job_id = args.job_id;
    let stream = args.stream.as_str();
    let offset = args.offset.unwrap_or(0);
    let length = args
        .length
        .map(JobLogLength::bytes)
        .unwrap_or(command_output::DEFAULT_LOG_READ_BYTES);
    let result = command_output::read_range(&session.id, job_id, stream, offset, length).await?;
    approvals::activity(
        &session.id,
        format!("Read {stream} for job {job_id}"),
        Some(format!("└ offset {offset}, length {length}")),
    )
    .await;
    text_result(serde_json::to_string_pretty(&result)?)
}

fn shell_command(script: &str) -> Result<Vec<String>> {
    #[cfg(unix)]
    {
        Ok(vec![
            SHELL_PROGRAM.to_owned(),
            "-c".to_owned(),
            script.to_owned(),
        ])
    }
    #[cfg(windows)]
    {
        let _ = script;
        anyhow::bail!(
            "shell-program tools are unsupported on Windows; invoke PowerShell explicitly with an argv tool"
        )
    }
    #[cfg(not(any(unix, windows)))]
    {
        let _ = script;
        anyhow::bail!("shell-program tools are unsupported on this platform")
    }
}

fn shell_activity_label(script: &str) -> String {
    format!(
        "{SHELL_PROGRAM} script ({} bytes; preview: {})",
        script.len(),
        shell_script_preview(script)
    )
}

fn shell_script_preview(script: &str) -> String {
    let preview = script
        .split('\n')
        .filter_map(|line| {
            let line = line.trim();
            if line.is_empty() {
                return None;
            }
            Some(if shell_line_is_sensitive(line) {
                "[redacted sensitive line]".to_owned()
            } else {
                escape_terminal_controls(line)
            })
        })
        .take(3)
        .collect::<Vec<_>>()
        .join(" ⏎ ");
    let preview = if preview.is_empty() {
        "<empty>".to_owned()
    } else {
        preview
    };
    bound_utf8_preview(&preview, SHELL_PREVIEW_LIMIT)
}

fn shell_line_is_sensitive(line: &str) -> bool {
    let normalized = line
        .chars()
        .filter(|character| character.is_ascii_alphanumeric())
        .map(|character| character.to_ascii_lowercase())
        .collect::<String>();
    [
        "authorization",
        "bearer",
        "cookie",
        "credential",
        "password",
        "passwd",
        "secret",
        "token",
        "apikey",
        "accesskey",
        "clientsecret",
        "privatekey",
    ]
    .iter()
    .any(|marker| normalized.contains(marker))
}

fn escape_terminal_controls(text: &str) -> String {
    let mut escaped = String::with_capacity(text.len());
    for character in text.chars() {
        match character {
            '\n' => escaped.push_str("\\n"),
            '\r' => escaped.push_str("\\r"),
            '\t' => escaped.push_str("\\t"),
            '\u{1b}' => escaped.push_str("\\x1b"),
            character if terminal_unsafe_character(character) => {
                let code = character as u32;
                if code <= 0xff {
                    escaped.push_str(&format!("\\x{code:02x}"));
                } else {
                    escaped.push_str(&format!("\\u{{{code:x}}}"));
                }
            }
            character => escaped.push(character),
        }
    }
    escaped
}

fn terminal_unsafe_character(character: char) -> bool {
    character.is_control()
        || matches!(
            character,
            '\u{200b}'..='\u{200f}'
                | '\u{202a}'..='\u{202e}'
                | '\u{2066}'..='\u{2069}'
                | '\u{feff}'
        )
}

fn bound_utf8_preview(text: &str, max_bytes: usize) -> String {
    if text.len() <= max_bytes {
        return text.to_owned();
    }
    const ELLIPSIS: &str = "...";
    let mut end = max_bytes.saturating_sub(ELLIPSIS.len()).min(text.len());
    while end > 0 && !text.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}{ELLIPSIS}", &text[..end])
}

async fn without_sandbox_shell(
    args: &WithoutSandboxShellArgs,
    session: &config::Session,
) -> Result<Value> {
    let command = match shell_command(&args.script) {
        Ok(command) => command,
        Err(error) => {
            return Ok(CommandOutcome::invalid_arguments(format!("{error:#}")).tool_result());
        }
    };
    without_sandbox_shell_with_timeout(
        command,
        args.cwd.as_deref(),
        shell_activity_label(&args.script),
        session,
        FOREGROUND_TIMEOUT,
    )
    .await
}

async fn without_sandbox_shell_with_timeout(
    command: Vec<String>,
    requested_cwd: Option<&str>,
    rendered_command: String,
    session: &config::Session,
    foreground_timeout: Duration,
) -> Result<Value> {
    let approval_detail = format!("shell: {SHELL_PROGRAM}\n{rendered_command}");
    let (mut registration, rendered_command) = match spawn_unrestricted_job_named(
        "without_sandbox_shell",
        command,
        requested_cwd,
        rendered_command,
        approval_detail,
        session,
    )
    .await
    {
        Ok(job) => job,
        Err(outcome) => return Ok(outcome.tool_result()),
    };
    let job_id = registration.id();

    match wait_for_registered_job(job_id, session, foreground_timeout).await {
        Ok(Some(outcome)) => {
            registration.disarm();
            Ok(outcome.tool_result())
        }
        Ok(None) => {
            let result =
                running_registered_job_result(session, &rendered_command, job_id, "Backgrounded")
                    .await;
            registration.disarm();
            Ok(result)
        }
        Err(outcome) => Ok(outcome.tool_result()),
    }
}

async fn without_sandbox(args: &WithoutSandboxArgs, session: &config::Session) -> Result<Value> {
    without_sandbox_with_timeout(
        args.command.as_slice(),
        args.cwd.as_deref(),
        session,
        FOREGROUND_TIMEOUT,
    )
    .await
}

async fn without_sandbox_with_timeout(
    command: &[String],
    requested_cwd: Option<&str>,
    session: &config::Session,
    foreground_timeout: Duration,
) -> Result<Value> {
    let (mut registration, rendered_command) =
        match spawn_unrestricted_job("without_sandbox", command, requested_cwd, session).await {
            Ok(job) => job,
            Err(outcome) => return Ok(outcome.tool_result()),
        };
    let job_id = registration.id();

    match wait_for_registered_job(job_id, session, foreground_timeout).await {
        Ok(Some(outcome)) => {
            registration.disarm();
            Ok(outcome.tool_result())
        }
        Ok(None) => {
            let result =
                running_registered_job_result(session, &rendered_command, job_id, "Backgrounded")
                    .await;
            registration.disarm();
            Ok(result)
        }
        Err(outcome) => Ok(outcome.tool_result()),
    }
}

async fn start_without_sandbox(
    args: &StartWithoutSandboxArgs,
    session: &config::Session,
) -> Result<Value> {
    let (mut registration, rendered_command) = match spawn_unrestricted_job(
        "start_without_sandbox",
        args.command.as_slice(),
        args.cwd.as_deref(),
        session,
    )
    .await
    {
        Ok(job) => job,
        Err(outcome) => return Ok(outcome.tool_result()),
    };
    let result =
        running_registered_job_result(session, &rendered_command, registration.id(), "Started")
            .await;
    registration.disarm();
    Ok(result)
}

async fn spawn_unrestricted_job(
    operation: &str,
    command: &[String],
    requested_cwd: Option<&str>,
    session: &config::Session,
) -> std::result::Result<(RegisteredJobGuard, String), CommandOutcome> {
    let command = command.to_vec();
    let rendered_command = render_command(&command);
    let approval_detail = format!("argv preview: {rendered_command}");
    spawn_unrestricted_job_named(
        operation,
        command,
        requested_cwd,
        rendered_command,
        approval_detail,
        session,
    )
    .await
}

async fn spawn_unrestricted_job_named(
    operation: &str,
    command: Vec<String>,
    requested_cwd: Option<&str>,
    rendered_command: String,
    approval_detail: String,
    session: &config::Session,
) -> std::result::Result<(RegisteredJobGuard, String), CommandOutcome> {
    let cwd = cwd(requested_cwd, &session.cwd)
        .map_err(|error| CommandOutcome::invalid_arguments(format!("{error:#}")))?;
    let approved = approvals::request(&session.id, operation, approval_detail, cwd.clone())
        .await
        .map_err(|error| CommandOutcome::internal(format!("Approval request failed: {error:#}")))?;
    if !approved {
        return Err(CommandOutcome::approval_denied(format!(
            "Approval was denied for {operation}; command was not started."
        )));
    }

    let job_id = Uuid::new_v4();
    job_journal::create_running(
        &session.id,
        job_id,
        rendered_command.clone(),
        cwd.clone(),
        job_journal::ExecutionMode::Unrestricted,
    )
    .map_err(|error| {
        CommandOutcome::internal(format!("Could not reserve job journal state: {error:#}"))
            .with_job_id(job_id)
    })?;
    approvals::activity(&session.id, format!("Running {rendered_command}"), None).await;

    let log_session_id = session.id.clone();
    let report_session_id = session.id.clone();
    let task_command = rendered_command.clone();
    let (control, cancellation) = sandbox::command_control();
    let task_control = control.clone();
    let handle = tokio::spawn(async move {
        let outcome = match command_output::prepare_log_capture(&log_session_id, job_id).await {
            Ok(logs) => match sandbox::run_unrestricted_logged_controlled(
                &command,
                &cwd,
                None,
                cancellation,
                None,
                logs.stdout(),
                logs.stderr(),
                command_output::capture_preview_limit(),
            )
            .await
            {
                Ok(output) => command_output::outcome_from_logged_output(job_id, output),
                Err(error) => CommandOutcome::spawn_error(format!("{error:#}")).with_job_id(job_id),
            },
            Err(error) => CommandOutcome::internal(format!(
                "Could not prepare command log capture: {error:#}"
            ))
            .with_job_id(job_id),
        };
        let final_outcome = match persist_command_outcome(&report_session_id, job_id, &outcome) {
            Ok(_) => outcome,
            Err(error) => CommandOutcome::internal(format!(
                "Command completed but its result could not be persisted: {error:#}"
            ))
            .with_job_id(job_id),
        };
        report_command_finished(report_session_id, &task_command, &final_outcome).await;
        final_outcome
    });

    // There is deliberately no await between spawning and registration. Once
    // the host process can start, cancellation of the MCP call cannot leave an
    // untracked process outside the session-owned job registry.
    jobs().lock().unwrap().insert(
        job_id,
        Job {
            session_id: session.id.clone(),
            command: rendered_command.clone(),
            handle,
            control: task_control,
        },
    );
    Ok((RegisteredJobGuard::new(job_id), rendered_command))
}

async fn wait_for_registered_job(
    job_id: Uuid,
    session: &config::Session,
    foreground_timeout: Duration,
) -> std::result::Result<Option<CommandOutcome>, CommandOutcome> {
    let wait_until_finished = async {
        loop {
            let finished = {
                let all_jobs = jobs().lock().unwrap();
                let Some(job) = all_jobs.get(&job_id) else {
                    return Err(CommandOutcome::internal(format!(
                        "Registered job {job_id} disappeared before completion"
                    ))
                    .with_job_id(job_id));
                };
                if job.session_id != session.id {
                    return Err(CommandOutcome::invalid_arguments(
                        "job does not belong to this session",
                    )
                    .with_job_id(job_id));
                }
                job.handle.is_finished()
            };
            if finished {
                return Ok(());
            }
            tokio::time::sleep(REGISTERED_JOB_WAIT_POLL_INTERVAL).await;
        }
    };

    match tokio::time::timeout(foreground_timeout, wait_until_finished).await {
        Ok(result) => result?,
        Err(_) => return Ok(None),
    }

    let job = {
        let mut all_jobs = jobs().lock().unwrap();
        let Some(job) = all_jobs.get(&job_id) else {
            return Err(CommandOutcome::internal(format!(
                "Registered job {job_id} disappeared after completion"
            ))
            .with_job_id(job_id));
        };
        if job.session_id != session.id {
            return Err(
                CommandOutcome::invalid_arguments("job does not belong to this session")
                    .with_job_id(job_id),
            );
        }
        all_jobs.remove(&job_id).unwrap()
    };
    Ok(Some(settle_joined_outcome(
        &session.id,
        job_id,
        job.handle.await,
    )))
}

async fn running_registered_job_result(
    session: &config::Session,
    rendered_command: &str,
    job_id: Uuid,
    activity: &str,
) -> Value {
    approvals::activity(
        &session.id,
        format!("{activity} {rendered_command}"),
        Some(format!("└ job {job_id}")),
    )
    .await;
    CommandOutcome::running(job_id).tool_result()
}

#[cfg(test)]
async fn run_and_report(
    session_id: String,
    command: Vec<String>,
    cwd: PathBuf,
    unrestricted: bool,
    roots: &[PathBuf],
) -> CommandOutcome {
    let rendered_command = render_command(&command);
    let job_id = Uuid::new_v4();
    let execution_mode = if unrestricted {
        job_journal::ExecutionMode::Unrestricted
    } else {
        job_journal::ExecutionMode::Sandboxed
    };
    if let Err(error) = job_journal::create_running(
        &session_id,
        job_id,
        rendered_command.clone(),
        cwd.clone(),
        execution_mode,
    ) {
        return CommandOutcome::internal(format!("Could not reserve job journal state: {error:#}"))
            .with_job_id(job_id);
    }
    approvals::activity(&session_id, format!("Running {rendered_command}"), None).await;
    let outcome = match command_output::prepare_log_capture(&session_id, job_id).await {
        Ok(logs) => {
            let output = if unrestricted {
                sandbox::run_unrestricted_logged(
                    &command,
                    &cwd,
                    None,
                    logs.stdout(),
                    logs.stderr(),
                    command_output::capture_preview_limit(),
                )
                .await
            } else {
                sandbox::run_logged(
                    &command,
                    &cwd,
                    roots,
                    None,
                    logs.stdout(),
                    logs.stderr(),
                    command_output::capture_preview_limit(),
                )
                .await
            };
            match output {
                Ok(output) => command_output::outcome_from_logged_output(job_id, output),
                Err(error) => CommandOutcome::spawn_error(format!("{error:#}")).with_job_id(job_id),
            }
        }
        Err(error) => {
            CommandOutcome::internal(format!("Could not prepare command log capture: {error:#}"))
                .with_job_id(job_id)
        }
    };
    let outcome = match persist_command_outcome(&session_id, job_id, &outcome) {
        Ok(_) => outcome,
        Err(error) => CommandOutcome::internal(format!(
            "Command completed but its result could not be persisted: {error:#}"
        ))
        .with_job_id(job_id),
    };
    report_command_finished(session_id, &rendered_command, &outcome).await;
    outcome
}

fn render_command(command: &[String]) -> String {
    let rendered = command
        .iter()
        .map(|arg| shell_word(arg))
        .collect::<Vec<_>>()
        .join(" ");
    bounded_text(&rendered, 512)
}

fn bounded_text(text: &str, max_bytes: usize) -> String {
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

async fn report_command_finished(session_id: String, command: &str, outcome: &CommandOutcome) {
    approvals::activity(
        &session_id,
        format!("Ran {command}"),
        outcome.activity_summary(2048),
    )
    .await;
}

fn shell_word(value: &str) -> String {
    if value
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || "-_./:=+".contains(c))
    {
        value.to_owned()
    } else {
        format!("{:?}", value)
    }
}

fn render_diff(old: &str, new: &str) -> (usize, usize, String) {
    let diff = TextDiff::from_lines(old, new);
    let mut added = 0;
    let mut removed = 0;
    for change in diff.iter_all_changes() {
        match change.tag() {
            ChangeTag::Insert => added += 1,
            ChangeTag::Delete => removed += 1,
            ChangeTag::Equal => {}
        }
    }
    let rendered = diff.unified_diff().context_radius(3).to_string();
    (added, removed, rendered.trim_end().to_owned())
}

fn render_output(output: sandbox::Output) -> Result<String> {
    let termination = output.termination;
    let text = json!({
        "exit_code": output.status,
        "stdout": output.stdout,
        "stderr": output.stderr,
        "termination": termination.as_str(),
        "termination_trigger": termination.trigger().map(sandbox::StopTrigger::as_str),
    })
    .to_string();
    if termination == sandbox::Termination::Exited && output.status != 0 {
        anyhow::bail!(text)
    } else {
        Ok(text)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn detects_supported_image_types() {
        assert_eq!(image_mime_type(b"\x89PNG\r\n\x1a\n"), Some("image/png"));
        assert_eq!(image_mime_type(b"\xff\xd8\xff\xe0"), Some("image/jpeg"));
        assert_eq!(image_mime_type(b"GIF89a"), Some("image/gif"));
        assert_eq!(image_mime_type(b"RIFF\0\0\0\0WEBP"), Some("image/webp"));
        assert_eq!(image_mime_type(b"not an image"), None);
    }

    #[test]
    fn renders_edit_counts_and_unified_diff() {
        let (added, removed, diff) = render_diff("one\ntwo\n", "one\nchanged\nthree\n");

        assert_eq!((added, removed), (2, 1));
        assert!(diff.contains("-two"));
        assert!(diff.contains("+changed"));
        assert!(diff.contains("+three"));
    }

    #[test]
    fn renders_forced_completion_cleanup_as_an_inspectable_result() {
        let rendered = render_output(sandbox::Output {
            status: 0,
            stdout: String::new(),
            stderr: String::new(),
            termination: sandbox::Termination::ForcedKill(sandbox::StopTrigger::Completion),
        })
        .unwrap();
        assert!(rendered.contains("\"termination\":\"forced_kill\""));
        assert!(rendered.contains("\"termination_trigger\":\"completion\""));
    }

    #[test]
    fn quotes_command_arguments_for_activity_display() {
        assert_eq!(shell_word("README.md"), "README.md");
        assert_eq!(shell_word("hello world"), "\"hello world\"");
    }

    #[cfg(windows)]
    #[test]
    fn describes_windows_command_execution_as_approved_host_access() {
        let tools = tools();
        for name in ["execute", "start_command"] {
            let description = tools
                .as_array()
                .unwrap()
                .iter()
                .find(|tool| tool["name"] == name)
                .unwrap()["description"]
                .as_str()
                .unwrap();
            assert!(description.contains("Windows host"));
            assert!(description.contains("requires approval"));
            assert!(description.contains("filesystem and network access"));
        }

        let write_file_description = tools
            .as_array()
            .unwrap()
            .iter()
            .find(|tool| tool["name"] == "write_file")
            .unwrap()["description"]
            .as_str()
            .unwrap();
        assert!(write_file_description.contains("Windows host"));
        assert!(write_file_description.contains("without a Codex sandbox"));
    }

    #[tokio::test]
    async fn get_image_returns_mcp_image_content() {
        let path = std::env::temp_dir().join(format!("local-mcp-{}.png", uuid::Uuid::new_v4()));
        let bytes = b"\x89PNG\r\n\x1a\nexample";
        tokio::fs::write(&path, bytes).await.unwrap();

        let result = get_image(&path).await.unwrap();
        tokio::fs::remove_file(path).await.unwrap();

        assert_eq!(result["content"][0]["type"], "image");
        assert_eq!(result["content"][0]["mimeType"], "image/png");
        assert_eq!(result["content"][0]["data"], STANDARD.encode(bytes));
    }

    #[tokio::test]
    async fn get_image_resolves_relative_paths_from_session_cwd() {
        let directory = std::env::temp_dir().join(format!("local-mcp-{}", uuid::Uuid::new_v4()));
        tokio::fs::create_dir(&directory).await.unwrap();
        let path = directory.join("image.gif");
        tokio::fs::write(&path, b"GIF89a").await.unwrap();

        let result = get_image(&resolve_path(&directory, PathBuf::from("image.gif")))
            .await
            .unwrap();
        tokio::fs::remove_dir_all(directory).await.unwrap();

        assert_eq!(result["content"][0]["mimeType"], "image/gif");
    }

    #[test]
    fn get_image_tool_declares_image_viewer_ui() {
        let tools = tools();
        let get_image = tools
            .as_array()
            .unwrap()
            .iter()
            .find(|tool| tool["name"] == "get_image")
            .unwrap();

        assert_eq!(
            get_image
                .pointer("/_meta/ui/resourceUri")
                .and_then(Value::as_str),
            Some(IMAGE_VIEWER_URI)
        );
        assert_eq!(
            get_image
                .pointer("/_meta/openai~1outputTemplate")
                .and_then(Value::as_str),
            Some(IMAGE_VIEWER_URI)
        );
        assert_eq!(
            get_image
                .pointer("/_meta/ui/visibility")
                .and_then(Value::as_array)
                .unwrap(),
            &vec![json!("model"), json!("app")]
        );
    }

    #[test]
    fn image_viewer_resource_is_valid_mcp_app_html() {
        let listed = resources();
        let resource = &listed.as_array().unwrap()[0];
        assert_eq!(resource["uri"], IMAGE_VIEWER_URI);
        assert_eq!(resource["mimeType"], MCP_APP_MIME_TYPE);

        let result = read_resource(&json!({"uri": IMAGE_VIEWER_URI})).unwrap();
        let content = &result["contents"][0];
        assert_eq!(content["uri"], IMAGE_VIEWER_URI);
        assert_eq!(content["mimeType"], MCP_APP_MIME_TYPE);
        let html = content["text"].as_str().unwrap();
        assert!(html.starts_with("<!doctype html>"));
        assert!(html.contains("ui/notifications/tool-result"));
        assert!(html.contains("data:${mimeType};base64,${image.data}"));
        assert!(html.contains("toolResponseMetadata"));
    }

    #[test]
    fn rejects_unknown_ui_resource() {
        let error = read_resource(&json!({"uri": "ui://local-mcp/unknown.html"})).unwrap_err();
        assert!(error.to_string().contains("unknown resource"));
    }

    #[cfg(unix)]
    async fn start_unrestricted_approval_responder(
        session_id: &str,
        operation: &'static str,
        response: &'static str,
    ) -> tokio::task::JoinHandle<()> {
        use tokio::net::UnixListener;

        let path = config::socket_path(session_id).unwrap();
        tokio::fs::create_dir_all(path.parent().unwrap())
            .await
            .unwrap();
        let _ = tokio::fs::remove_file(&path).await;
        let listener = UnixListener::bind(&path).unwrap();
        tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut line = String::new();
            BufReader::new(&mut stream)
                .read_line(&mut line)
                .await
                .unwrap();
            assert!(line.contains(operation));
            stream
                .write_all(format!("{response}\n").as_bytes())
                .await
                .unwrap();
        })
    }

    #[cfg(unix)]
    fn unrestricted_test_session(directory: &Path) -> config::Session {
        config::Session {
            id: format!("unrestricted-test-{}", Uuid::new_v4()),
            cwd: directory.to_owned(),
            permitted_directories: vec![directory.to_owned()],
        }
    }

    fn structured_job_id(result: &Value) -> Uuid {
        Uuid::parse_str(result["structuredContent"]["job_id"].as_str().unwrap()).unwrap()
    }

    #[cfg(unix)]
    async fn wait_for_structured_job(job_id: Uuid, session: &config::Session) -> Value {
        let args: PollJobArgs = serde_json::from_value(json!({
            "session_id": session.id,
            "job_id": job_id
        }))
        .unwrap();
        for _ in 0..200 {
            let result = poll_job(&args, session).await.unwrap();
            if result["structuredContent"]["status"] != "running" {
                return result;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("job did not finish");
    }

    #[cfg(unix)]
    async fn cleanup_unrestricted_test(session: &config::Session, directory: &Path) {
        let _ = tokio::fs::remove_file(config::socket_path(&session.id).unwrap()).await;
        job_journal::remove_session_for_tests(&session.id);
        let _ = tokio::fs::remove_dir_all(
            config::state_dir()
                .unwrap()
                .join("command-logs")
                .join(&session.id),
        )
        .await;
        let _ = tokio::fs::remove_dir_all(directory).await;
    }

    #[test]
    fn command_tools_publish_the_versioned_output_schema() {
        let tools = tools();
        for name in [
            "execute",
            "execute_shell",
            "start_command",
            "poll_job",
            "stop_job",
            "start_without_sandbox",
            "without_sandbox",
            "without_sandbox_shell",
        ] {
            let tool = tools
                .as_array()
                .unwrap()
                .iter()
                .find(|tool| tool["name"] == name)
                .unwrap();
            assert_eq!(tool["outputSchema"], command_result::output_schema());
        }
        for name in ["read_file", "read_job_log", "list_jobs"] {
            let tool = tools
                .as_array()
                .unwrap()
                .iter()
                .find(|tool| tool["name"] == name)
                .unwrap();
            assert!(tool.get("outputSchema").is_none());
        }
    }

    #[test]
    fn shell_tools_use_string_schemas_without_changing_argv_tools() {
        let tools = tools();
        let find = |name: &str| {
            tools
                .as_array()
                .unwrap()
                .iter()
                .find(|tool| tool["name"] == name)
                .unwrap()
        };

        for name in ["execute_shell", "without_sandbox_shell"] {
            let tool = find(name);
            assert_eq!(
                tool["inputSchema"]["properties"]["script"]["type"],
                "string"
            );
            assert_eq!(tool["inputSchema"]["additionalProperties"], false);
            assert!(tool["inputSchema"]["properties"].get("command").is_none());
            assert!(
                tool["inputSchema"]["required"]
                    .as_array()
                    .unwrap()
                    .contains(&json!("script"))
            );
            assert_eq!(tool["outputSchema"], command_result::output_schema());
        }

        assert_eq!(
            find("execute")["inputSchema"]["properties"]["command"]["type"],
            "array"
        );
        assert_eq!(
            find("without_sandbox")["inputSchema"]["properties"]["command"]["type"],
            "array"
        );
    }

    #[cfg(unix)]
    #[test]
    fn shell_command_uses_bash_and_preserves_multiline_programs() {
        let script = "set -euo pipefail\nprintf '%s\\n' hello | sed 's/hello/world/'\ncat <<'EOF'\ndone\nEOF";
        let command = shell_command(script).unwrap();
        assert_eq!(command, vec!["bash", "-c", script]);
    }

    #[test]
    fn shell_activity_preview_is_terminal_safe_bounded_and_redacted() {
        let script = format!(
            "echo safe\x1b[2J\rforged\x07\nX-API-Key: {}\necho {}\nfourth line",
            "x".repeat(200),
            "日本語".repeat(300)
        );
        let preview = shell_script_preview(&script);
        assert!(preview.contains("echo safe\\x1b[2J\\rforged\\x07"));
        assert!(preview.contains("[redacted sensitive line]"));
        assert!(!preview.contains('\x1b'));
        assert!(!preview.contains('\r'));
        assert!(!preview.contains('\x07'));
        assert!(!preview.contains(&"x".repeat(20)));
        assert!(!preview.contains("fourth line"));
        assert!(preview.matches(" ⏎ ").count() <= 2);
        assert!(preview.len() <= SHELL_PREVIEW_LIMIT);
    }

    #[test]
    fn shell_activity_preview_escapes_c1_and_bidi_controls() {
        let preview = shell_script_preview("printf '\u{009b}31mspoof\u{202e}'");
        assert!(preview.contains("\\x9b"));
        assert!(preview.contains("\\u{202e}"));
        assert!(!preview.contains('\u{009b}'));
        assert!(!preview.contains('\u{202e}'));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn denied_shell_approval_never_starts_the_process() {
        let directory = std::env::temp_dir().join(format!("local-mcp-shell-{}", Uuid::new_v4()));
        tokio::fs::create_dir_all(&directory).await.unwrap();
        let session = unrestricted_test_session(&directory);
        let marker = directory.join("should-not-exist");
        let responder =
            start_unrestricted_approval_responder(&session.id, "without_sandbox_shell", "deny")
                .await;
        let args: WithoutSandboxShellArgs = serde_json::from_value(json!({
            "session_id": session.id,
            "script": format!("printf started > {}", marker.display())
        }))
        .unwrap();

        let result = without_sandbox_shell(&args, &session).await.unwrap();
        responder.await.unwrap();

        assert_eq!(result["structuredContent"]["error_kind"], "approval_denied");
        assert!(!marker.exists());
        assert_eq!(
            jobs()
                .lock()
                .unwrap()
                .values()
                .filter(|job| job.session_id == session.id)
                .count(),
            0
        );
        cleanup_unrestricted_test(&session, &directory).await;
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn approved_shell_executes_multiline_pipeline_and_heredoc() {
        let directory = std::env::temp_dir().join(format!("local-mcp-shell-{}", Uuid::new_v4()));
        tokio::fs::create_dir_all(&directory).await.unwrap();
        let session = unrestricted_test_session(&directory);
        let responder =
            start_unrestricted_approval_responder(&session.id, "without_sandbox_shell", "allow")
                .await;
        let script =
            "set -euo pipefail\ncat <<'EOF' | sed 's/hello/world/' > result.txt\nhello\nEOF";
        let args: WithoutSandboxShellArgs = serde_json::from_value(json!({
            "session_id": session.id,
            "script": script
        }))
        .unwrap();

        let result = without_sandbox_shell(&args, &session).await.unwrap();
        responder.await.unwrap();

        assert_eq!(result["structuredContent"]["status"], "completed");
        assert_eq!(
            tokio::fs::read_to_string(directory.join("result.txt"))
                .await
                .unwrap()
                .trim(),
            "world"
        );
        cleanup_unrestricted_test(&session, &directory).await;
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn approved_shell_honors_cwd_and_reports_nonzero_exit() {
        let directory = std::env::temp_dir().join(format!("local-mcp-shell-{}", Uuid::new_v4()));
        tokio::fs::create_dir_all(&directory).await.unwrap();
        let session = unrestricted_test_session(&directory);
        let responder =
            start_unrestricted_approval_responder(&session.id, "without_sandbox_shell", "allow")
                .await;
        let args: WithoutSandboxShellArgs = serde_json::from_value(json!({
            "session_id": session.id,
            "script": "pwd > cwd.txt\nexit 7"
        }))
        .unwrap();

        let result = without_sandbox_shell(&args, &session).await.unwrap();
        responder.await.unwrap();

        assert_eq!(result["structuredContent"]["error_kind"], "process_exit");
        assert_eq!(result["structuredContent"]["exit_code"], 7);
        let recorded = tokio::fs::read_to_string(directory.join("cwd.txt"))
            .await
            .unwrap();
        assert_eq!(recorded.trim(), directory.to_string_lossy());
        cleanup_unrestricted_test(&session, &directory).await;
    }

    #[test]
    fn unrestricted_background_tool_is_declared() {
        let tools = tools();
        let tool = tools
            .as_array()
            .unwrap()
            .iter()
            .find(|tool| tool["name"] == "start_without_sandbox")
            .unwrap();
        assert_eq!(
            tool["inputSchema"]["properties"]["command"]["type"],
            "array"
        );
        assert_eq!(tool["outputSchema"], command_result::output_schema());
        assert!(
            tool["description"]
                .as_str()
                .unwrap()
                .contains("denial never starts")
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn denied_unrestricted_approval_does_not_start_or_create_a_job() {
        let directory =
            std::env::temp_dir().join(format!("local-mcp-unrestricted-{}", Uuid::new_v4()));
        tokio::fs::create_dir_all(&directory).await.unwrap();
        let session = unrestricted_test_session(&directory);
        let marker = directory.join("denied-marker");
        let responder =
            start_unrestricted_approval_responder(&session.id, "start_without_sandbox", "deny")
                .await;
        let args: StartWithoutSandboxArgs = serde_json::from_value(json!({
            "session_id": session.id,
            "command": ["sh", "-c", format!("printf started > {}", marker.display())]
        }))
        .unwrap();

        let result = start_without_sandbox(&args, &session).await.unwrap();
        responder.await.unwrap();

        assert_eq!(result["structuredContent"]["error_kind"], "approval_denied");
        assert_eq!(result["isError"], true);
        assert!(!marker.exists());
        assert_eq!(
            jobs()
                .lock()
                .unwrap()
                .values()
                .filter(|job| job.session_id == session.id)
                .count(),
            0
        );
        let page = job_journal::list(&session.id, &HashSet::new(), None, 0, 100).unwrap();
        assert!(page.jobs.is_empty());
        cleanup_unrestricted_test(&session, &directory).await;
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn unrestricted_foreground_command_returns_structured_result() {
        let directory =
            std::env::temp_dir().join(format!("local-mcp-unrestricted-{}", Uuid::new_v4()));
        tokio::fs::create_dir_all(&directory).await.unwrap();
        let session = unrestricted_test_session(&directory);
        let responder =
            start_unrestricted_approval_responder(&session.id, "without_sandbox", "allow").await;
        let command = vec![
            "sh".to_owned(),
            "-c".to_owned(),
            "printf complete > foreground.txt".to_owned(),
        ];

        let result = without_sandbox_with_timeout(&command, None, &session, Duration::from_secs(1))
            .await
            .unwrap();
        responder.await.unwrap();

        assert_eq!(result["structuredContent"]["status"], "completed");
        assert_eq!(result["structuredContent"]["exit_code"], 0);
        assert!(directory.join("foreground.txt").is_file());
        cleanup_unrestricted_test(&session, &directory).await;
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn unrestricted_foreground_timeout_auto_backgrounds() {
        let directory =
            std::env::temp_dir().join(format!("local-mcp-unrestricted-{}", Uuid::new_v4()));
        tokio::fs::create_dir_all(&directory).await.unwrap();
        let session = unrestricted_test_session(&directory);
        let responder =
            start_unrestricted_approval_responder(&session.id, "without_sandbox", "allow").await;
        let command = vec![
            "sh".to_owned(),
            "-c".to_owned(),
            "sleep 0.1; printf complete > auto.txt".to_owned(),
        ];

        let running =
            without_sandbox_with_timeout(&command, None, &session, Duration::from_millis(5))
                .await
                .unwrap();
        responder.await.unwrap();
        assert_eq!(running["structuredContent"]["status"], "running");
        let job_id = structured_job_id(&running);
        let completed = wait_for_structured_job(job_id, &session).await;

        assert_eq!(completed["structuredContent"]["status"], "completed");
        assert_eq!(completed["structuredContent"]["exit_code"], 0);
        assert!(directory.join("auto.txt").is_file());
        cleanup_unrestricted_test(&session, &directory).await;
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn cancelled_foreground_call_terminates_registered_unrestricted_job() {
        let directory =
            std::env::temp_dir().join(format!("local-mcp-unrestricted-{}", Uuid::new_v4()));
        tokio::fs::create_dir_all(&directory).await.unwrap();
        let session = unrestricted_test_session(&directory);
        let started = directory.join("cancel-started.txt");
        let completed = directory.join("cancel-completed.txt");
        let responder =
            start_unrestricted_approval_responder(&session.id, "without_sandbox", "allow").await;
        let command = vec![
            "sh".to_owned(),
            "-c".to_owned(),
            format!(
                "printf started > {}; sleep 0.5; printf completed > {}",
                started.display(),
                completed.display()
            ),
        ];
        let call_session = session.clone();
        let call = tokio::spawn(async move {
            without_sandbox_with_timeout(&command, None, &call_session, Duration::from_secs(5))
                .await
        });
        responder.await.unwrap();

        for _ in 0..100 {
            if started.exists() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        assert!(started.exists(), "the unrestricted process never started");
        call.abort();
        assert!(call.await.unwrap_err().is_cancelled());

        tokio::time::sleep(Duration::from_millis(600)).await;
        assert!(
            !completed.exists(),
            "cancelling before a job_id response must terminate the command"
        );
        assert_eq!(
            jobs()
                .lock()
                .unwrap()
                .values()
                .filter(|job| job.session_id == session.id)
                .count(),
            0,
            "cancelled call leaked a registered job"
        );
        let stopped = job_journal::list(
            &session.id,
            &HashSet::new(),
            Some(job_journal::JobState::Stopped),
            0,
            100,
        )
        .unwrap();
        assert_eq!(stopped.jobs.len(), 1);
        cleanup_unrestricted_test(&session, &directory).await;
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn explicit_unrestricted_background_job_can_be_polled() {
        let directory =
            std::env::temp_dir().join(format!("local-mcp-unrestricted-{}", Uuid::new_v4()));
        tokio::fs::create_dir_all(&directory).await.unwrap();
        let session = unrestricted_test_session(&directory);
        let responder =
            start_unrestricted_approval_responder(&session.id, "start_without_sandbox", "allow")
                .await;
        let args: StartWithoutSandboxArgs = serde_json::from_value(json!({
            "session_id": session.id,
            "command": ["sh", "-c", "sleep 0.05; printf complete > explicit.txt"]
        }))
        .unwrap();

        let running = start_without_sandbox(&args, &session).await.unwrap();
        responder.await.unwrap();
        assert_eq!(running["structuredContent"]["status"], "running");
        let job_id = structured_job_id(&running);
        let completed = wait_for_structured_job(job_id, &session).await;

        assert_eq!(completed["structuredContent"]["status"], "completed");
        assert_eq!(completed["structuredContent"]["exit_code"], 0);
        assert!(directory.join("explicit.txt").is_file());
        cleanup_unrestricted_test(&session, &directory).await;
    }

    #[tokio::test]
    async fn invalid_command_arguments_are_structured_tool_outcomes() {
        for arguments in [
            json!({"session_id": "schema-contract", "command": "true"}),
            json!({"session_id": "schema-contract", "command": ["true"], "cwd": 1}),
        ] {
            let result = call_tool(&json!({
                "name": "execute",
                "arguments": arguments
            }))
            .await
            .unwrap();
            assert_eq!(
                result["structuredContent"]["error_kind"],
                "invalid_arguments"
            );
            assert_eq!(result["isError"], true);
        }
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn nonzero_process_exit_is_a_structured_result_not_a_transport_error() {
        let directory =
            std::env::temp_dir().join(format!("local-mcp-structured-{}", Uuid::new_v4()));
        tokio::fs::create_dir_all(&directory).await.unwrap();
        let session_id = format!("structured-result-{}", Uuid::new_v4());
        let outcome = run_and_report(
            session_id.clone(),
            vec![
                "sh".to_owned(),
                "-c".to_owned(),
                "printf output; printf failure >&2; exit 2".to_owned(),
            ],
            directory.clone(),
            true,
            &[],
        )
        .await;
        let result = outcome.tool_result();
        assert_eq!(result["structuredContent"]["error_kind"], "process_exit");
        assert_eq!(result["structuredContent"]["exit_code"], 2);
        assert_eq!(result["isError"], true);
        assert!(
            result["content"][0]["text"]
                .as_str()
                .unwrap()
                .contains("failure")
        );

        job_journal::remove_session_for_tests(&session_id);
        let _ = tokio::fs::remove_dir_all(
            config::state_dir()
                .unwrap()
                .join("command-logs")
                .join(&session_id),
        )
        .await;
        tokio::fs::remove_dir_all(directory).await.unwrap();
    }

    #[test]
    fn jsonrpc_request_ids_are_bounded_before_tool_dispatch() {
        assert!(command_output::jsonrpc_id_within_budget(&json!(1)));
        assert!(command_output::jsonrpc_id_within_budget(&json!(
            "request-1"
        )));
        assert!(!command_output::jsonrpc_id_within_budget(&json!(
            "x".repeat(command_output::MAX_JSONRPC_ID_SERIALIZED_BYTES)
        )));
    }

    #[test]
    fn read_job_log_tool_declares_bounded_range_schema() {
        let tool = tools()
            .as_array()
            .unwrap()
            .iter()
            .find(|tool| tool["name"] == "read_job_log")
            .unwrap()
            .clone();
        assert_eq!(
            tool.pointer("/inputSchema/properties/length/maximum"),
            Some(&json!(JOB_LOG_MAX_READ_BYTES as f64))
        );
        assert_eq!(
            tool.pointer("/inputSchema/definitions/JobLogStream/enum"),
            Some(&json!(["stdout", "stderr"]))
        );
        assert_eq!(tool["inputSchema"]["additionalProperties"], false);
    }

    #[test]
    fn workflow_tool_schema_is_fail_closed_and_bounded() {
        let tools = tools();
        let tool = tools
            .as_array()
            .unwrap()
            .iter()
            .find(|tool| tool["name"] == "run_workflow_step")
            .unwrap();
        let schema = &tool["inputSchema"];
        assert_eq!(schema["additionalProperties"], false);
        assert_eq!(schema["properties"]["depends_on"]["maxItems"], 128);
        assert_eq!(schema["properties"]["required_files"]["maxItems"], 256);
        assert_eq!(schema["properties"]["expected_outputs"]["maxItems"], 256);
        assert_eq!(schema["properties"]["command"]["minItems"], 1);
        assert_eq!(schema["properties"]["command"]["maxItems"], 4096);
        assert_eq!(
            schema["properties"]["command"]["prefixItems"][0]["minLength"],
            1
        );
        assert!(tool.get("outputSchema").is_none());
    }

    #[test]
    fn list_jobs_tool_has_bounded_filterable_schema() {
        let tool = tools()
            .as_array()
            .unwrap()
            .iter()
            .find(|tool| tool["name"] == "list_jobs")
            .unwrap()
            .clone();
        assert_eq!(
            tool.pointer("/inputSchema/properties/limit/maximum"),
            Some(&json!(JOB_LIST_MAX_LIMIT as f64))
        );
        assert_eq!(
            tool.pointer("/inputSchema/definitions/JobStateFilter/enum"),
            Some(&json!([
                "running",
                "completed",
                "failed",
                "stopped",
                "orphaned"
            ]))
        );
        assert_eq!(tool["inputSchema"]["additionalProperties"], false);
    }

    #[test]
    fn command_activity_summary_uses_bounded_head_and_tail() {
        let outcome = CommandOutcome::from_bounded_output(command_result::BoundedCommandOutput {
            job_id: Uuid::new_v4(),
            exit_code: 0,
            termination: "exited".to_owned(),
            termination_trigger: None,
            stdout_bytes: 20,
            stderr_bytes: 0,
            stdout_truncated: true,
            stderr_truncated: false,
            stdout_head: "first line\n".to_owned(),
            stdout_tail: "last line\n".to_owned(),
            stderr_head: String::new(),
            stderr_tail: String::new(),
            stdout_resource: "local-mcp://jobs/test/stdout".to_owned(),
            stderr_resource: "local-mcp://jobs/test/stderr".to_owned(),
            read_tool: "read_job_log".to_owned(),
        });
        let summary = outcome.activity_summary(2048).unwrap();
        assert!(summary.contains("first line"));
        assert!(summary.contains("output truncated"));
        assert!(summary.contains("last line"));
    }

    #[test]
    fn rendered_activity_command_is_utf8_safe_and_bounded() {
        let rendered = render_command(&["echo".into(), "🙂".repeat(1000)]);
        assert!(rendered.len() <= 512);
        assert!(rendered.ends_with("..."));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn repeated_stop_job_calls_return_the_same_terminal_result() {
        let directory = std::env::temp_dir().join(format!("local-mcp-stop-{}", Uuid::new_v4()));
        tokio::fs::create_dir_all(&directory).await.unwrap();
        let session = config::Session {
            id: format!("stop-test-{}", Uuid::new_v4()),
            cwd: directory.clone(),
            permitted_directories: vec![directory.clone()],
        };
        let job_id = Uuid::new_v4();
        let command = vec!["sh".to_owned(), "-c".to_owned(), "sleep 30".to_owned()];
        let (control, cancellation) = sandbox::command_control();
        let task_directory = directory.clone();
        let task_job_id = job_id;
        let handle = tokio::spawn(async move {
            match sandbox::run_unrestricted_controlled(
                &command,
                &task_directory,
                None,
                cancellation,
                None,
            )
            .await
            {
                Ok(output) => {
                    CommandOutcome::from_bounded_output(command_result::BoundedCommandOutput {
                        job_id: task_job_id,
                        exit_code: output.status,
                        termination: output.termination.as_str().to_owned(),
                        termination_trigger: output
                            .termination
                            .trigger()
                            .map(sandbox::StopTrigger::as_str)
                            .map(str::to_owned),
                        stdout_bytes: output.stdout.len() as u64,
                        stderr_bytes: output.stderr.len() as u64,
                        stdout_truncated: false,
                        stderr_truncated: false,
                        stdout_head: output.stdout,
                        stdout_tail: String::new(),
                        stderr_head: output.stderr,
                        stderr_tail: String::new(),
                        stdout_resource: command_output::resource_uri(task_job_id, "stdout"),
                        stderr_resource: command_output::resource_uri(task_job_id, "stderr"),
                        read_tool: "read_job_log".to_owned(),
                    })
                }
                Err(error) => {
                    CommandOutcome::spawn_error(format!("{error:#}")).with_job_id(task_job_id)
                }
            }
        });
        job_journal::create_running(
            &session.id,
            job_id,
            "sh -c sleep".to_owned(),
            directory.clone(),
            job_journal::ExecutionMode::Unrestricted,
        )
        .unwrap();
        jobs().lock().unwrap().insert(
            job_id,
            Job {
                session_id: session.id.clone(),
                command: "sh -c sleep".to_owned(),
                handle,
                control,
            },
        );

        let args: StopJobArgs = serde_json::from_value(json!({
            "session_id": session.id,
            "job_id": job_id.to_string()
        }))
        .unwrap();
        let first = stop_job(&args, &session).await.unwrap();
        let second = stop_job(&args, &session).await.unwrap();
        assert_eq!(first, second);
        assert_eq!(first["structuredContent"]["termination"], "stopped");
        assert_eq!(first["structuredContent"]["termination_trigger"], "stop");
        assert_eq!(first["structuredContent"]["status"], "stopped");

        job_journal::remove_session_for_tests(&session.id);
        tokio::fs::remove_dir_all(directory).await.unwrap();
    }

    #[tokio::test]
    async fn poll_recovers_completed_result_after_in_memory_state_is_lost() {
        let directory =
            std::env::temp_dir().join(format!("local-mcp-journal-poll-{}", Uuid::new_v4()));
        tokio::fs::create_dir_all(&directory).await.unwrap();
        let session = config::Session {
            id: format!("journal-poll-{}", Uuid::new_v4()),
            cwd: directory.clone(),
            permitted_directories: vec![directory.clone()],
        };
        let job_id = Uuid::new_v4();
        job_journal::create_running(
            &session.id,
            job_id,
            "completed command".to_owned(),
            directory.clone(),
            argv_execution_mode(),
        )
        .unwrap();
        let completed = CommandOutcome::from_bounded_output(command_result::BoundedCommandOutput {
            job_id,
            exit_code: 0,
            termination: "exited".to_owned(),
            termination_trigger: None,
            stdout_bytes: 16,
            stderr_bytes: 0,
            stdout_truncated: false,
            stderr_truncated: false,
            stdout_head: "persisted marker".to_owned(),
            stdout_tail: String::new(),
            stderr_head: String::new(),
            stderr_tail: String::new(),
            stdout_resource: command_output::resource_uri(job_id, "stdout"),
            stderr_resource: command_output::resource_uri(job_id, "stderr"),
            read_tool: "read_job_log".to_owned(),
        });
        persist_command_outcome(&session.id, job_id, &completed).unwrap();

        let args: PollJobArgs = serde_json::from_value(json!({
            "session_id": session.id,
            "job_id": job_id
        }))
        .unwrap();
        let result = poll_job(&args, &session).await.unwrap();
        assert!(
            result["content"][0]["text"]
                .as_str()
                .unwrap()
                .contains("persisted marker")
        );

        job_journal::remove_session_for_tests(&session.id);
        tokio::fs::remove_dir_all(directory).await.unwrap();
    }

    #[tokio::test]
    async fn poll_reports_unrecoverable_running_job_as_orphaned() {
        let directory =
            std::env::temp_dir().join(format!("local-mcp-journal-orphan-{}", Uuid::new_v4()));
        tokio::fs::create_dir_all(&directory).await.unwrap();
        let session = config::Session {
            id: format!("journal-orphan-{}", Uuid::new_v4()),
            cwd: directory.clone(),
            permitted_directories: vec![directory.clone()],
        };
        let job_id = Uuid::new_v4();
        job_journal::create_running(
            &session.id,
            job_id,
            "lost command".to_owned(),
            directory.clone(),
            argv_execution_mode(),
        )
        .unwrap();

        let args: PollJobArgs = serde_json::from_value(json!({
            "session_id": session.id,
            "job_id": job_id
        }))
        .unwrap();
        let result = poll_job(&args, &session).await.unwrap();
        assert_eq!(result["structuredContent"]["status"], "stopped");
        assert_eq!(result["structuredContent"]["termination"], "orphaned");
        assert!(
            result["structuredContent"]["message"]
                .as_str()
                .unwrap()
                .contains("cannot be reattached")
        );

        job_journal::remove_session_for_tests(&session.id);
        tokio::fs::remove_dir_all(directory).await.unwrap();
    }

    #[test]
    fn heartbeat_tools_are_declared() {
        let tools = tools();
        for name in [
            "heartbeat_start",
            "heartbeat_wait",
            "heartbeat_status",
            "heartbeat_stop",
        ] {
            assert!(
                tools
                    .as_array()
                    .unwrap()
                    .iter()
                    .any(|tool| tool["name"] == name),
                "missing tool {name}"
            );
        }
    }

    #[test]
    fn heartbeat_skips_ticks_missed_while_agent_is_busy() {
        let start = Instant::now();
        let interval = Duration::from_secs(300);
        let mut heartbeat = Heartbeat::new(interval, start, 1);

        // The 5-minute tick was delivered while the agent was waiting.
        heartbeat.delivered_ticks += 1;
        heartbeat.next_tick += interval;

        // The work triggered at 5 minutes runs until 12 minutes. The 10-minute
        // tick must be skipped instead of being queued for immediate delivery.
        let missed = skip_missed_heartbeat_ticks(&mut heartbeat, start + Duration::from_secs(720));
        assert_eq!(missed, 1);
        assert_eq!(heartbeat.skipped_ticks, 1);
        assert_eq!(heartbeat.delivered_ticks, 1);
        assert_eq!(heartbeat.next_tick, start + Duration::from_secs(900));
    }

    #[test]
    fn heartbeat_skips_multiple_overdue_ticks_without_catching_up() {
        let start = Instant::now();
        let mut heartbeat = Heartbeat::new(Duration::from_secs(60), start, 1);

        let missed = skip_missed_heartbeat_ticks(&mut heartbeat, start + Duration::from_secs(305));
        assert_eq!(missed, 5);
        assert_eq!(heartbeat.skipped_ticks, 5);
        assert_eq!(heartbeat.next_tick, start + Duration::from_secs(360));
    }

    fn minimal_fixture(tool: ToolName) -> Value {
        let session_id = "schema-contract";
        match tool {
            ToolName::SessionInfo => json!({"session_id": session_id}),
            ToolName::ReadFile | ToolName::GetImage | ToolName::ListDirectory => {
                json!({"session_id": session_id, "path": "README.md"})
            }
            ToolName::WriteFile => {
                json!({"session_id": session_id, "path": "output.txt", "content": "ok"})
            }
            ToolName::Execute
            | ToolName::StartCommand
            | ToolName::StartWithoutSandbox
            | ToolName::WithoutSandbox => {
                json!({"session_id": session_id, "command": ["true"]})
            }
            ToolName::ExecuteShell | ToolName::WithoutSandboxShell => {
                json!({"session_id": session_id, "script": "true"})
            }
            ToolName::RunWorkflowStep => json!({
                "session_id": session_id,
                "step_id": "focused-tests",
                "idempotency_key": "focused-tests-v1",
                "command": ["true"]
            }),
            ToolName::PollJob | ToolName::StopJob => json!({
                "session_id": session_id,
                "job_id": "00000000-0000-4000-8000-000000000001"
            }),
            ToolName::ReadJobLog => json!({
                "session_id": session_id,
                "job_id": "00000000-0000-4000-8000-000000000001",
                "stream": "stdout"
            }),
            ToolName::ListJobs => json!({"session_id": session_id}),
            ToolName::HeartbeatStart => {
                json!({"session_id": session_id, "interval_seconds": 1})
            }
            ToolName::HeartbeatWait | ToolName::HeartbeatStatus | ToolName::HeartbeatStop => {
                json!({"session_id": session_id})
            }
        }
    }

    fn deserialize_tool_arguments(tool: ToolName, value: &Value) -> Result<()> {
        match tool {
            ToolName::SessionInfo => {
                let _: SessionInfoArgs = parse_arguments(tool, value)?;
            }
            ToolName::ReadFile => {
                let _: ReadFileArgs = parse_arguments(tool, value)?;
            }
            ToolName::GetImage => {
                let _: GetImageArgs = parse_arguments(tool, value)?;
            }
            ToolName::ListDirectory => {
                let _: ListDirectoryArgs = parse_arguments(tool, value)?;
            }
            ToolName::WriteFile => {
                let _: WriteFileArgs = parse_arguments(tool, value)?;
            }
            ToolName::Execute => {
                let _: ExecuteArgs = parse_arguments(tool, value)?;
            }
            ToolName::ExecuteShell => {
                let _: ExecuteShellArgs = parse_arguments(tool, value)?;
            }
            ToolName::StartCommand => {
                let _: StartCommandArgs = parse_arguments(tool, value)?;
            }
            ToolName::RunWorkflowStep => {
                let _: workflow::WorkflowStepArgs = parse_arguments(tool, value)?;
            }
            ToolName::PollJob => {
                let _: PollJobArgs = parse_arguments(tool, value)?;
            }
            ToolName::StopJob => {
                let _: StopJobArgs = parse_arguments(tool, value)?;
            }
            ToolName::ReadJobLog => {
                let _: ReadJobLogArgs = parse_arguments(tool, value)?;
            }
            ToolName::ListJobs => {
                let _: ListJobsArgs = parse_arguments(tool, value)?;
            }
            ToolName::HeartbeatStart => {
                let _: HeartbeatStartArgs = parse_arguments(tool, value)?;
            }
            ToolName::HeartbeatWait => {
                let _: HeartbeatWaitArgs = parse_arguments(tool, value)?;
            }
            ToolName::HeartbeatStatus => {
                let _: HeartbeatStatusArgs = parse_arguments(tool, value)?;
            }
            ToolName::HeartbeatStop => {
                let _: HeartbeatStopArgs = parse_arguments(tool, value)?;
            }
            ToolName::StartWithoutSandbox => {
                let _: StartWithoutSandboxArgs = parse_arguments(tool, value)?;
            }
            ToolName::WithoutSandbox => {
                let _: WithoutSandboxArgs = parse_arguments(tool, value)?;
            }
            ToolName::WithoutSandboxShell => {
                let _: WithoutSandboxShellArgs = parse_arguments(tool, value)?;
            }
        }
        Ok(())
    }

    fn schema_snapshot() -> String {
        let schemas = ToolName::ALL
            .into_iter()
            .map(|tool| {
                json!({
                    "name": tool.as_str(),
                    "inputSchema": tool.input_schema(),
                })
            })
            .collect::<Vec<_>>();
        format!("{}\n", serde_json::to_string_pretty(&schemas).unwrap())
    }

    #[test]
    fn every_declared_tool_deserializes_a_minimal_schema_valid_fixture() {
        for tool in ToolName::ALL {
            deserialize_tool_arguments(tool, &minimal_fixture(tool))
                .unwrap_or_else(|error| panic!("{} fixture failed: {error:#}", tool.as_str()));
        }
    }

    #[test]
    fn typed_arguments_preserve_legacy_unknown_and_null_behavior() {
        assert!(
            deserialize_tool_arguments(ToolName::SessionInfo, &json!({})).is_err(),
            "missing required session_id must fail"
        );
        assert!(
            deserialize_tool_arguments(
                ToolName::Execute,
                &json!({"session_id": "schema-contract", "command": "cargo test"}),
            )
            .is_err(),
            "a scalar command must fail"
        );
        assert!(
            deserialize_tool_arguments(
                ToolName::Execute,
                &json!({"session_id": "schema-contract", "command": ["cargo", 1]}),
            )
            .is_err(),
            "non-string argv entries must fail"
        );
        assert!(
            deserialize_tool_arguments(
                ToolName::Execute,
                &json!({
                    "session_id": "schema-contract",
                    "command": ["true"],
                    "unexpected": true
                }),
            )
            .is_ok(),
            "execute ignored unknown fields before typed argument migration"
        );
        assert!(
            deserialize_tool_arguments(
                ToolName::SessionInfo,
                &json!({"session_id": "schema-contract", "unexpected": true}),
            )
            .is_err(),
            "session_info already rejected unknown fields"
        );
        assert!(
            deserialize_tool_arguments(
                ToolName::Execute,
                &json!({"session_id": "schema-contract", "command": ["true"], "cwd": null}),
            )
            .is_ok(),
            "explicit null cwd remains equivalent to omission"
        );
        assert!(
            deserialize_tool_arguments(
                ToolName::HeartbeatStatus,
                &json!({"session_id": "schema-contract", "name": null}),
            )
            .is_ok(),
            "explicit null heartbeat name remains equivalent to omission"
        );
    }

    fn assert_schema_and_serde_agree(tool: ToolName, value: Value, expected: bool) {
        let schema = tool.input_schema();
        let validator = jsonschema::validator_for(&schema)
            .unwrap_or_else(|error| panic!("{} schema did not compile: {error}", tool.as_str()));
        let schema_accepts = validator.is_valid(&value);
        let serde_accepts = deserialize_tool_arguments(tool, &value).is_ok();
        assert_eq!(
            schema_accepts,
            serde_accepts,
            "schema/Serde mismatch for {} with {value}",
            tool.as_str()
        );
        assert_eq!(
            schema_accepts,
            expected,
            "unexpected contract result for {} with {value}",
            tool.as_str()
        );
    }

    #[test]
    fn generated_schemas_and_serde_agree_on_boundaries() {
        let valid_job_id = "00000000-0000-4000-8000-000000000001";
        let cases = [
            (
                ToolName::SessionInfo,
                json!({"session_id": "schema-contract"}),
                true,
            ),
            (ToolName::SessionInfo, json!({}), false),
            (ToolName::SessionInfo, json!({"session_id": "."}), false),
            (ToolName::SessionInfo, json!({"session_id": ".."}), false),
            (
                ToolName::SessionInfo,
                json!({"session_id": "x".repeat(64)}),
                true,
            ),
            (
                ToolName::SessionInfo,
                json!({"session_id": "x".repeat(65)}),
                false,
            ),
            (
                ToolName::SessionInfo,
                json!({"session_id": "contains spaces"}),
                false,
            ),
            (
                ToolName::SessionInfo,
                json!({"session_id": "schema-contract", "extra": true}),
                false,
            ),
            (
                ToolName::ReadFile,
                json!({"session_id": "schema-contract", "path": "README.md", "extra": true}),
                true,
            ),
            (
                ToolName::GetImage,
                json!({"session_id": "schema-contract", "path": "image.png", "extra": true}),
                false,
            ),
            (
                ToolName::Execute,
                json!({"session_id": "schema-contract", "command": ["true"]}),
                true,
            ),
            (
                ToolName::Execute,
                json!({"session_id": "schema-contract", "command": ["true"], "cwd": null}),
                true,
            ),
            (
                ToolName::Execute,
                json!({"session_id": "schema-contract", "command": ["true"], "cwd": "."}),
                true,
            ),
            (
                ToolName::Execute,
                json!({"session_id": "schema-contract", "command": ["true"], "cwd": 1}),
                false,
            ),
            (
                ToolName::Execute,
                json!({"session_id": "schema-contract", "command": []}),
                false,
            ),
            (
                ToolName::Execute,
                json!({"session_id": "schema-contract", "command": "true"}),
                false,
            ),
            (
                ToolName::Execute,
                json!({"session_id": "schema-contract", "command": ["true"], "extra": true}),
                true,
            ),
            (
                ToolName::PollJob,
                json!({"session_id": "schema-contract", "job_id": valid_job_id}),
                true,
            ),
            (
                ToolName::HeartbeatStart,
                json!({"session_id": "schema-contract", "interval_seconds": 1}),
                true,
            ),
            (
                ToolName::HeartbeatStart,
                json!({"session_id": "schema-contract", "interval_seconds": 86400}),
                true,
            ),
            (
                ToolName::HeartbeatStart,
                json!({"session_id": "schema-contract", "interval_seconds": 0}),
                false,
            ),
            (
                ToolName::HeartbeatStart,
                json!({"session_id": "schema-contract", "interval_seconds": 86401}),
                false,
            ),
            (
                ToolName::HeartbeatStart,
                json!({"session_id": "schema-contract", "interval_seconds": 1, "name": null}),
                true,
            ),
            (
                ToolName::HeartbeatStart,
                json!({"session_id": "schema-contract", "interval_seconds": 1, "name": ""}),
                false,
            ),
            (
                ToolName::HeartbeatStart,
                json!({"session_id": "schema-contract", "interval_seconds": 1, "name": "x".repeat(64)}),
                true,
            ),
            (
                ToolName::HeartbeatStart,
                json!({"session_id": "schema-contract", "interval_seconds": 1, "name": "x".repeat(65)}),
                false,
            ),
            (
                ToolName::HeartbeatStart,
                json!({"session_id": "schema-contract", "interval_seconds": 1, "extra": true}),
                false,
            ),
            (
                ToolName::HeartbeatWait,
                json!({"session_id": "schema-contract", "name": null, "max_wait_seconds": null}),
                true,
            ),
            (
                ToolName::HeartbeatWait,
                json!({"session_id": "schema-contract", "max_wait_seconds": 1}),
                true,
            ),
            (
                ToolName::HeartbeatWait,
                json!({"session_id": "schema-contract", "max_wait_seconds": 25}),
                true,
            ),
            (
                ToolName::HeartbeatWait,
                json!({"session_id": "schema-contract", "max_wait_seconds": 0}),
                false,
            ),
            (
                ToolName::HeartbeatWait,
                json!({"session_id": "schema-contract", "max_wait_seconds": 26}),
                false,
            ),
            (
                ToolName::WithoutSandbox,
                json!({"session_id": "schema-contract", "command": ["true"], "cwd": null, "extra": true}),
                true,
            ),
        ];

        for (tool, value, expected) in cases {
            assert_schema_and_serde_agree(tool, value, expected);
        }
    }

    #[test]
    fn generated_schema_bounds_match_runtime_validation() {
        let execute = ToolName::Execute.input_schema();
        assert_eq!(
            execute
                .pointer("/properties/command/minItems")
                .and_then(Value::as_u64),
            Some(1)
        );
        assert_eq!(
            execute
                .pointer("/properties/session_id/minLength")
                .and_then(Value::as_u64),
            Some(1)
        );
        assert_eq!(
            execute
                .pointer("/properties/session_id/maxLength")
                .and_then(Value::as_u64),
            Some(64)
        );
        assert_eq!(
            execute
                .pointer("/properties/session_id/pattern")
                .and_then(Value::as_str),
            Some("^[A-Za-z0-9._-]+$")
        );
        assert_eq!(
            execute.pointer("/properties/session_id/not/enum"),
            Some(&json!([".", ".."]))
        );

        let heartbeat_start = ToolName::HeartbeatStart.input_schema();
        assert_eq!(
            heartbeat_start
                .pointer("/properties/interval_seconds/minimum")
                .and_then(Value::as_f64),
            Some(1.0)
        );
        assert_eq!(
            heartbeat_start
                .pointer("/properties/interval_seconds/maximum")
                .and_then(Value::as_f64),
            Some(HEARTBEAT_MAX_INTERVAL_SECONDS as f64)
        );

        let heartbeat_wait = ToolName::HeartbeatWait.input_schema();
        assert_eq!(
            heartbeat_wait
                .pointer("/properties/max_wait_seconds/minimum")
                .and_then(Value::as_f64),
            Some(1.0)
        );
        assert_eq!(
            heartbeat_wait
                .pointer("/properties/max_wait_seconds/maximum")
                .and_then(Value::as_f64),
            Some(HEARTBEAT_MAX_WAIT_SECONDS as f64)
        );
    }

    #[test]
    fn declared_tool_names_and_dispatch_handlers_are_bijective() {
        let declared = tools()
            .as_array()
            .unwrap()
            .iter()
            .map(|tool| tool["name"].as_str().unwrap().to_owned())
            .collect::<Vec<_>>();
        let handled = ToolName::ALL
            .into_iter()
            .map(|tool| tool.as_str().to_owned())
            .collect::<Vec<_>>();

        assert_eq!(declared, handled);
        let mut unique = handled.clone();
        unique.sort();
        unique.dedup();
        assert_eq!(unique.len(), ToolName::ALL.len());
        for name in declared {
            assert!(
                ToolName::parse(&name).is_some(),
                "missing handler for {name}"
            );
        }
    }

    #[test]
    fn generated_schemas_preserve_legacy_additional_property_policy() {
        let strict = [
            ToolName::SessionInfo,
            ToolName::GetImage,
            ToolName::PollJob,
            ToolName::StopJob,
            ToolName::ReadJobLog,
            ToolName::ListJobs,
            ToolName::HeartbeatStart,
            ToolName::HeartbeatWait,
            ToolName::HeartbeatStatus,
            ToolName::HeartbeatStop,
            ToolName::ExecuteShell,
            ToolName::WithoutSandboxShell,
            ToolName::RunWorkflowStep,
        ];
        for tool in ToolName::ALL {
            let schema = tool.input_schema();
            let expected = strict.contains(&tool).then_some(&Value::Bool(false));
            assert_eq!(
                schema.get("additionalProperties"),
                expected,
                "{} must preserve its pre-refactor unknown-field behavior",
                tool.as_str()
            );
        }
    }

    #[test]
    fn tool_schemas_match_snapshot() {
        let actual = schema_snapshot();
        if std::env::var_os("UPDATE_TOOL_SCHEMA_SNAPSHOT").is_some() {
            std::fs::write(
                concat!(
                    env!("CARGO_MANIFEST_DIR"),
                    "/tests/snapshots/tool_schemas.json"
                ),
                &actual,
            )
            .unwrap();
            return;
        }
        assert_eq!(actual, include_str!("../tests/snapshots/tool_schemas.json"));
    }
}
