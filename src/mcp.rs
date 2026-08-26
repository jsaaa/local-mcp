use std::collections::HashMap;
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
use crate::{approvals, command_output, config, sandbox};

const FOREGROUND_TIMEOUT: Duration = Duration::from_secs(30);
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
    handle: JoinHandle<Result<String>>,
    control: sandbox::CommandControl,
}

#[derive(Clone)]
enum TerminalOutcome {
    Success(String),
    Failure(String),
}

#[derive(Clone)]
struct TerminalJob {
    session_id: String,
    outcome: TerminalOutcome,
}

fn jobs() -> &'static Mutex<HashMap<Uuid, Job>> {
    static JOBS: OnceLock<Mutex<HashMap<Uuid, Job>>> = OnceLock::new();
    JOBS.get_or_init(|| Mutex::new(HashMap::new()))
}

fn terminal_jobs() -> &'static Mutex<HashMap<Uuid, TerminalJob>> {
    static TERMINAL_JOBS: OnceLock<Mutex<HashMap<Uuid, TerminalJob>>> = OnceLock::new();
    TERMINAL_JOBS.get_or_init(|| Mutex::new(HashMap::new()))
}

fn remember_terminal_job(job_id: Uuid, session_id: &str, result: &Result<String>) {
    const MAX_TERMINAL_JOBS: usize = 256;
    let outcome = match result {
        Ok(text) => TerminalOutcome::Success(text.clone()),
        Err(error) => TerminalOutcome::Failure(format!("{error:#}")),
    };
    let mut terminal = terminal_jobs().lock().unwrap();
    if terminal.len() >= MAX_TERMINAL_JOBS
        && !terminal.contains_key(&job_id)
        && let Some(oldest) = terminal.keys().next().copied()
    {
        terminal.remove(&oldest);
    }
    terminal.insert(
        job_id,
        TerminalJob {
            session_id: session_id.to_owned(),
            outcome,
        },
    );
}

fn cached_terminal_result(job_id: Uuid, session_id: &str) -> Result<Option<Value>> {
    let terminal = terminal_jobs().lock().unwrap();
    let Some(job) = terminal.get(&job_id) else {
        return Ok(None);
    };
    anyhow::ensure!(
        job.session_id == session_id,
        "job does not belong to this session"
    );
    match &job.outcome {
        TerminalOutcome::Success(text) => text_result(text.clone()).map(Some),
        TerminalOutcome::Failure(error) => anyhow::bail!(error.clone()),
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
    StartCommand,
    PollJob,
    StopJob,
    ReadJobLog,
    HeartbeatStart,
    HeartbeatWait,
    HeartbeatStatus,
    HeartbeatStop,
    WithoutSandbox,
}

impl ToolName {
    const ALL: [Self; 15] = [
        Self::SessionInfo,
        Self::ReadFile,
        Self::GetImage,
        Self::ListDirectory,
        Self::WriteFile,
        Self::Execute,
        Self::StartCommand,
        Self::PollJob,
        Self::StopJob,
        Self::ReadJobLog,
        Self::HeartbeatStart,
        Self::HeartbeatWait,
        Self::HeartbeatStatus,
        Self::HeartbeatStop,
        Self::WithoutSandbox,
    ];

    fn as_str(self) -> &'static str {
        match self {
            Self::SessionInfo => "session_info",
            Self::ReadFile => "read_file",
            Self::GetImage => "get_image",
            Self::ListDirectory => "list_directory",
            Self::WriteFile => "write_file",
            Self::Execute => "execute",
            Self::StartCommand => "start_command",
            Self::PollJob => "poll_job",
            Self::StopJob => "stop_job",
            Self::ReadJobLog => "read_job_log",
            Self::HeartbeatStart => "heartbeat_start",
            Self::HeartbeatWait => "heartbeat_wait",
            Self::HeartbeatStatus => "heartbeat_status",
            Self::HeartbeatStop => "heartbeat_stop",
            Self::WithoutSandbox => "without_sandbox",
        }
    }

    fn parse(value: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|tool| tool.as_str() == value)
    }

    fn input_schema(self) -> Value {
        match self {
            Self::SessionInfo => generated_schema::<SessionInfoArgs>(),
            Self::ReadFile => generated_schema::<ReadFileArgs>(),
            Self::GetImage => generated_schema::<GetImageArgs>(),
            Self::ListDirectory => generated_schema::<ListDirectoryArgs>(),
            Self::WriteFile => generated_schema::<WriteFileArgs>(),
            Self::Execute => generated_schema::<ExecuteArgs>(),
            Self::StartCommand => generated_schema::<StartCommandArgs>(),
            Self::PollJob => generated_schema::<PollJobArgs>(),
            Self::StopJob => generated_schema::<StopJobArgs>(),
            Self::ReadJobLog => generated_schema::<ReadJobLogArgs>(),
            Self::HeartbeatStart => generated_schema::<HeartbeatStartArgs>(),
            Self::HeartbeatWait => generated_schema::<HeartbeatWaitArgs>(),
            Self::HeartbeatStatus => generated_schema::<HeartbeatStatusArgs>(),
            Self::HeartbeatStop => generated_schema::<HeartbeatStopArgs>(),
            Self::WithoutSandbox => generated_schema::<WithoutSandboxArgs>(),
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
                ToolName::StartCommand => start_command_description,
                ToolName::PollJob => {
                    "Poll a background command returned by execute or start_command. Returns running while active, or the command result once completed."
                }
                ToolName::StopJob => {
                    "Stop a background command returned by execute or start_command. The command's complete process tree receives a graceful stop followed by forced termination after a bounded grace period; repeated calls return the cached terminal result."
                }
                ToolName::ReadJobLog => {
                    "Read a bounded byte range from a command's stored stdout or stderr. Use the job_id returned in foreground/background results; access is scoped to the supplied session_id."
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
                ToolName::WithoutSandbox => {
                    "Execute argv directly on the host with full user permissions and network access. Every call requires approval unless the session is in yolo mode."
                }
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
            definition
        })
        .collect();
    Value::Array(values)
}

fn parse_arguments<T: DeserializeOwned>(tool: ToolName, args: &Value) -> Result<T> {
    serde_json::from_value(args.clone())
        .with_context(|| format!("invalid arguments for {}", tool.as_str()))
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
            let args: ExecuteArgs = parse_arguments(tool, &raw_args)?;
            let session = config::load_session(args.session_id.as_str()).await?;
            execute(&args, &session).await
        }
        ToolName::StartCommand => {
            let args: StartCommandArgs = parse_arguments(tool, &raw_args)?;
            let session = config::load_session(args.session_id.as_str()).await?;
            start_command(&args, &session).await
        }
        ToolName::PollJob => {
            let args: PollJobArgs = parse_arguments(tool, &raw_args)?;
            let session = config::load_session(args.session_id.as_str()).await?;
            poll_job(&args, &session).await
        }
        ToolName::StopJob => {
            let args: StopJobArgs = parse_arguments(tool, &raw_args)?;
            let session = config::load_session(args.session_id.as_str()).await?;
            stop_job(&args, &session).await
        }
        ToolName::ReadJobLog => {
            let args: ReadJobLogArgs = parse_arguments(tool, &raw_args)?;
            let session = config::load_session(args.session_id.as_str()).await?;
            read_job_log(&args, &session).await
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
        ToolName::WithoutSandbox => {
            let args: WithoutSandboxArgs = parse_arguments(tool, &raw_args)?;
            let session = config::load_session(args.session_id.as_str()).await?;
            without_sandbox(&args, &session).await
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

async fn execute(args: &ExecuteArgs, session: &config::Session) -> Result<Value> {
    let (job_id, rendered_command, mut handle, control) = spawn_sandboxed_command(
        "execute",
        args.command.as_slice(),
        args.cwd.as_deref(),
        session,
    )
    .await?;

    match tokio::time::timeout(FOREGROUND_TIMEOUT, &mut handle).await {
        Ok(joined) => {
            drop(control);
            text_result(joined.context("command task failed")??)
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
    let (job_id, rendered_command, handle, control) = spawn_sandboxed_command(
        "start_command",
        args.command.as_slice(),
        args.cwd.as_deref(),
        session,
    )
    .await?;
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
) -> Result<(
    Uuid,
    String,
    JoinHandle<Result<String>>,
    sandbox::CommandControl,
)> {
    let command = command.to_vec();
    let cwd = cwd(requested_cwd, &session.cwd)?;
    let rendered_command = render_command(&command);
    #[cfg(windows)]
    if !approvals::request(
        &session.id,
        operation,
        format!("argv preview: {rendered_command}"),
        cwd.clone(),
    )
    .await?
    {
        anyhow::bail!("user denied {operation}")
    }
    #[cfg(not(windows))]
    let _ = operation;
    let mut roots = session.permitted_directories.clone();
    if !roots.iter().any(|root| cwd.starts_with(root)) {
        roots.push(cwd.clone());
    }
    approvals::activity(&session.id, format!("Running {rendered_command}"), None).await;
    let job_id = Uuid::new_v4();
    let log_session_id = session.id.clone();
    let report_session_id = session.id.clone();
    let task_command = rendered_command.clone();
    let (control, cancellation) = sandbox::command_control();
    let handle = tokio::spawn(async move {
        let result = async {
            let logs = command_output::prepare_log_capture(&log_session_id, job_id).await?;
            let output = sandbox::run_logged_controlled(
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
            .await?;
            command_output::render_logged_output(job_id, output)
        }
        .await;
        report_command_finished(report_session_id, &task_command, &result).await;
        result
    });
    Ok((job_id, rendered_command, handle, control))
}

async fn store_job(
    job_id: Uuid,
    session: &config::Session,
    rendered_command: String,
    handle: JoinHandle<Result<String>>,
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
    text_result(json!({"status":"running","job_id":job_id}).to_string())
}

async fn poll_job(args: &PollJobArgs, session: &config::Session) -> Result<Value> {
    let job_id = args.job_id;
    let finished = {
        let jobs = jobs().lock().unwrap();
        match jobs.get(&job_id) {
            Some(job) => {
                anyhow::ensure!(
                    job.session_id == session.id,
                    "job does not belong to this session"
                );
                Some(job.handle.is_finished())
            }
            None => None,
        }
    };
    match finished {
        None => cached_terminal_result(job_id, &session.id)?
            .with_context(|| format!("unknown job_id: {job_id}")),
        Some(false) => text_result(json!({"status":"running","job_id":job_id}).to_string()),
        Some(true) => {
            let job = jobs().lock().unwrap().remove(&job_id).unwrap();
            let result = match job.handle.await {
                Ok(result) => result,
                Err(error) => Err(anyhow::anyhow!("background command task failed: {error}")),
            };
            remember_terminal_job(job_id, &session.id, &result);
            text_result(result?)
        }
    }
}

async fn stop_job(args: &StopJobArgs, session: &config::Session) -> Result<Value> {
    let job_id = args.job_id;
    let job = {
        let mut jobs = jobs().lock().unwrap();
        if let Some(job) = jobs.get(&job_id) {
            anyhow::ensure!(
                job.session_id == session.id,
                "job does not belong to this session"
            );
        }
        jobs.remove(&job_id)
    };

    let Some(job) = job else {
        return cached_terminal_result(job_id, &session.id)?
            .with_context(|| format!("unknown job_id: {job_id}"));
    };

    job.control.request_stop();
    let result = match job.handle.await {
        Ok(result) => result,
        Err(error) => Err(anyhow::anyhow!("background command task failed: {error}")),
    };
    remember_terminal_job(job_id, &session.id, &result);
    let termination = result
        .as_ref()
        .ok()
        .and_then(|text| serde_json::from_str::<Value>(text).ok())
        .and_then(|value| {
            value
                .get("termination")
                .and_then(Value::as_str)
                .map(str::to_owned)
        })
        .unwrap_or_else(|| "failed".to_owned());
    approvals::activity(
        &session.id,
        format!("Stopped {}", job.command),
        Some(format!("└ job {job_id}; termination {termination}")),
    )
    .await;
    text_result(result?)
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

async fn without_sandbox(args: &WithoutSandboxArgs, session: &config::Session) -> Result<Value> {
    let command = args.command.as_slice().to_vec();
    let cwd = cwd(args.cwd.as_deref(), &session.cwd)?;
    if !approvals::request(
        &session.id,
        "without_sandbox",
        format!("argv preview: {}", render_command(&command)),
        cwd.clone(),
    )
    .await?
    {
        anyhow::bail!("user denied without_sandbox")
    }
    run_and_report(session.id.clone(), command, cwd, true, &[]).await
}

async fn run_and_report(
    session_id: String,
    command: Vec<String>,
    cwd: PathBuf,
    unrestricted: bool,
    roots: &[PathBuf],
) -> Result<Value> {
    let rendered_command = render_command(&command);
    approvals::activity(&session_id, format!("Running {rendered_command}"), None).await;
    let job_id = Uuid::new_v4();
    let result = async {
        let logs = command_output::prepare_log_capture(&session_id, job_id).await?;
        let output = if unrestricted {
            sandbox::run_unrestricted_logged(
                &command,
                &cwd,
                None,
                logs.stdout(),
                logs.stderr(),
                command_output::capture_preview_limit(),
            )
            .await?
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
            .await?
        };
        command_output::render_logged_output(job_id, output)
    }
    .await;
    report_command_finished(session_id, &rendered_command, &result).await;
    text_result(result?)
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

async fn report_command_finished(session_id: String, command: &str, result: &Result<String>) {
    let detail = match result {
        Ok(text) => command_summary(text),
        Err(error) => command_summary(&error.to_string())
            .or_else(|| Some(bounded_text(&format!("└ Error: {error:#}"), 2048))),
    };
    approvals::activity(&session_id, format!("Ran {command}"), detail).await;
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

fn command_summary(text: &str) -> Option<String> {
    let value: Value = serde_json::from_str(text).ok()?;
    let has_bounded_streams = ["stdout_head", "stdout_tail", "stderr_head", "stderr_tail"]
        .iter()
        .any(|key| value.get(*key).is_some());

    let output = if has_bounded_streams {
        let stream_summary = |name: &str| {
            let head_key = format!("{name}_head");
            let tail_key = format!("{name}_tail");
            let truncated_key = format!("{name}_truncated");
            let head = value
                .get(head_key.as_str())
                .and_then(Value::as_str)
                .unwrap_or("")
                .trim_end();
            let tail = value
                .get(tail_key.as_str())
                .and_then(Value::as_str)
                .unwrap_or("")
                .trim_end();
            let truncated = value
                .get(truncated_key.as_str())
                .and_then(Value::as_bool)
                .unwrap_or(false);
            if head.is_empty() && tail.is_empty() {
                String::new()
            } else if truncated && !tail.is_empty() {
                format!("{head}\n... output truncated; use read_job_log ...\n{tail}")
            } else {
                head.to_owned()
            }
        };
        let stdout = stream_summary("stdout");
        let stderr = stream_summary("stderr");
        if stdout.is_empty() { stderr } else { stdout }
    } else {
        let stdout = value
            .get("stdout")
            .and_then(Value::as_str)
            .unwrap_or("")
            .trim_end();
        let stderr = value
            .get("stderr")
            .and_then(Value::as_str)
            .unwrap_or("")
            .trim_end();
        if stdout.is_empty() {
            stderr.to_owned()
        } else {
            stdout.to_owned()
        }
    };

    if output.is_empty() {
        None
    } else {
        Some(
            output
                .lines()
                .map(|line| format!("└ {line}"))
                .collect::<Vec<_>>()
                .join("\n"),
        )
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
    fn command_activity_summary_uses_bounded_head_and_tail() {
        let text = json!({
            "stdout_head": "first line\n",
            "stdout_tail": "last line\n",
            "stdout_truncated": true,
            "stderr_head": "",
            "stderr_tail": "",
            "stderr_truncated": false
        })
        .to_string();
        let summary = command_summary(&text).unwrap();
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
        let command = vec!["sh".to_owned(), "-c".to_owned(), "sleep 30".to_owned()];
        let (control, cancellation) = sandbox::command_control();
        let task_directory = directory.clone();
        let handle = tokio::spawn(async move {
            sandbox::run_unrestricted_controlled(
                &command,
                &task_directory,
                None,
                cancellation,
                None,
            )
            .await
            .and_then(render_output)
        });
        let job_id = Uuid::new_v4();
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
        let text = first["content"][0]["text"].as_str().unwrap();
        assert!(text.contains("\"termination\":\"stopped\""));
        assert!(text.contains("\"termination_trigger\":\"stop\""));

        terminal_jobs().lock().unwrap().remove(&job_id);
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
            ToolName::Execute | ToolName::StartCommand | ToolName::WithoutSandbox => {
                json!({"session_id": session_id, "command": ["true"]})
            }
            ToolName::PollJob | ToolName::StopJob => json!({
                "session_id": session_id,
                "job_id": "00000000-0000-4000-8000-000000000001"
            }),
            ToolName::ReadJobLog => json!({
                "session_id": session_id,
                "job_id": "00000000-0000-4000-8000-000000000001",
                "stream": "stdout"
            }),
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
            ToolName::StartCommand => {
                let _: StartCommandArgs = parse_arguments(tool, value)?;
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
            ToolName::WithoutSandbox => {
                let _: WithoutSandboxArgs = parse_arguments(tool, value)?;
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
            ToolName::HeartbeatStart,
            ToolName::HeartbeatWait,
            ToolName::HeartbeatStatus,
            ToolName::HeartbeatStop,
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
