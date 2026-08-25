use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use serde_json::{Value, json};
use similar::{ChangeTag, TextDiff};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::task::JoinHandle;
use uuid::Uuid;

use crate::{approvals, config, sandbox};

const FOREGROUND_TIMEOUT: Duration = Duration::from_secs(30);
const HEARTBEAT_DEFAULT_NAME: &str = "default";
const HEARTBEAT_MAX_WAIT: Duration = Duration::from_secs(25);
const HEARTBEAT_MAX_INTERVAL: Duration = Duration::from_secs(24 * 60 * 60);
const IMAGE_VIEWER_URI: &str = "ui://local-mcp/image-viewer-v1.html";
const MCP_APP_MIME_TYPE: &str = "text/html;profile=mcp-app";
const SHELL_PROGRAM: &str = "bash";
const SHELL_PREVIEW_LIMIT: usize = 160;
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
}

fn jobs() -> &'static Mutex<HashMap<Uuid, Job>> {
    static JOBS: OnceLock<Mutex<HashMap<Uuid, Job>>> = OnceLock::new();
    JOBS.get_or_init(|| Mutex::new(HashMap::new()))
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
    let without_sandbox_shell_description = "Execute a multi-line Bash program directly on the host with full user permissions and network access. Pass the program in script as a string. Approval is required before launch unless the session is in yolo mode.";
    #[cfg(windows)]
    let without_sandbox_shell_description = "Shell-program execution is unsupported on Windows. Use without_sandbox with an explicit PowerShell argv command instead.";

    let mut tools = json!([
        {"name":"session_info","description":"Show a local-mcp session's ID, working directory, and allowed sandbox roots.","inputSchema":{"type":"object","properties":{"session_id":{"type":"string","format":"uuid"}},"required":["session_id"],"additionalProperties":false}},
        {"name":"read_file","description":"Read a UTF-8 file from the local machine. Relative paths use the session working directory.","inputSchema":{"type":"object","properties":{"session_id":{"type":"string","format":"uuid"},"path":{"type":"string"}},"required":["session_id","path"]}},
        {"name":"get_image","description":"Read a local image and return it as MCP image content. Relative paths use the session working directory.","inputSchema":{"type":"object","properties":{"session_id":{"type":"string","format":"uuid"},"path":{"type":"string","description":"Path to a PNG, JPEG, GIF, WebP, BMP, TIFF, or AVIF image."}},"required":["session_id","path"],"additionalProperties":false},"_meta":{"ui":{"resourceUri":IMAGE_VIEWER_URI,"visibility":["model","app"]},"openai/outputTemplate":IMAGE_VIEWER_URI,"openai/toolInvocation/invoking":"Reading image…","openai/toolInvocation/invoked":"Image ready"}},
        {"name":"list_directory","description":"List entries in a local directory. Relative paths use the session working directory.","inputSchema":{"type":"object","properties":{"session_id":{"type":"string","format":"uuid"},"path":{"type":"string"}},"required":["session_id","path"]}},
        {"name":"write_file","description":write_file_description,"inputSchema":{"type":"object","properties":{"session_id":{"type":"string","format":"uuid"},"path":{"type":"string"},"content":{"type":"string"}},"required":["session_id","path","content"]}},
        {"name":"execute","description":execute_description,"inputSchema":{"type":"object","properties":{"session_id":{"type":"string","format":"uuid"},"command":{"type":"array","items":{"type":"string"},"minItems":1},"cwd":{"type":"string"}},"required":["session_id","command"]}},
        {"name":"execute_shell","description":execute_shell_description,"inputSchema":{"type":"object","properties":{"session_id":{"type":"string","format":"uuid"},"script":{"type":"string"},"cwd":{"type":"string"}},"required":["session_id","script"],"additionalProperties":false}},
        {"name":"start_command","description":start_command_description,"inputSchema":{"type":"object","properties":{"session_id":{"type":"string","format":"uuid"},"command":{"type":"array","items":{"type":"string"},"minItems":1},"cwd":{"type":"string"}},"required":["session_id","command"]}},
        {"name":"poll_job","description":"Poll a background command returned by execute or start_command. Returns running while active, or the command result once completed.","inputSchema":{"type":"object","properties":{"session_id":{"type":"string","format":"uuid"},"job_id":{"type":"string","format":"uuid"}},"required":["session_id","job_id"],"additionalProperties":false}},
        {"name":"stop_job","description":"Stop a background command returned by execute or start_command.","inputSchema":{"type":"object","properties":{"session_id":{"type":"string","format":"uuid"},"job_id":{"type":"string","format":"uuid"}},"required":["session_id","job_id"],"additionalProperties":false}},
        {"name":"heartbeat_start","description":"Start or reset an in-turn heartbeat schedule for this local-mcp session. After starting it, call heartbeat_wait repeatedly. A tick is delivered only while heartbeat_wait is actively waiting; ticks that occur while the agent is busy doing work are skipped instead of queued.","inputSchema":{"type":"object","properties":{"session_id":{"type":"string"},"interval_seconds":{"type":"integer","minimum":1,"maximum":86400},"name":{"type":"string","minLength":1,"maxLength":64}},"required":["session_id","interval_seconds"],"additionalProperties":false}},
        {"name":"heartbeat_wait","description":"Wait for the next heartbeat tick in short long-poll chunks. Call this repeatedly until status is tick, then do one work cycle and call it again. If a scheduled tick passes while no heartbeat_wait call is active because the agent is still working, that tick is skipped. This does not revive a ChatGPT turn after the turn has ended.","inputSchema":{"type":"object","properties":{"session_id":{"type":"string"},"name":{"type":"string","minLength":1,"maxLength":64},"max_wait_seconds":{"type":"integer","minimum":1,"maximum":25}},"required":["session_id"],"additionalProperties":false}},
        {"name":"heartbeat_status","description":"Show the current in-turn heartbeat schedule, delivered tick count, skipped tick count, and time until the next tick.","inputSchema":{"type":"object","properties":{"session_id":{"type":"string"},"name":{"type":"string","minLength":1,"maxLength":64}},"required":["session_id"],"additionalProperties":false}},
        {"name":"heartbeat_stop","description":"Stop and remove an in-turn heartbeat schedule for this local-mcp session.","inputSchema":{"type":"object","properties":{"session_id":{"type":"string"},"name":{"type":"string","minLength":1,"maxLength":64}},"required":["session_id"],"additionalProperties":false}},
        {"name":"without_sandbox","description":"Execute argv directly on the host with full user permissions and network access. Every call requires approval unless the session is in yolo mode.","inputSchema":{"type":"object","properties":{"session_id":{"type":"string","format":"uuid"},"command":{"type":"array","items":{"type":"string"},"minItems":1},"cwd":{"type":"string"}},"required":["session_id","command"]}},
        {"name":"without_sandbox_shell","description":without_sandbox_shell_description,"inputSchema":{"type":"object","properties":{"session_id":{"type":"string","format":"uuid"},"script":{"type":"string"},"cwd":{"type":"string"}},"required":["session_id","script"],"additionalProperties":false}}
    ]);
    for tool in tools.as_array_mut().unwrap() {
        if let Some(session_id) = tool
            .pointer_mut("/inputSchema/properties/session_id")
            .and_then(Value::as_object_mut)
        {
            session_id.remove("format");
        }
    }
    tools
}

async fn call_tool(params: &Value) -> Result<Value> {
    let name = params
        .get("name")
        .and_then(Value::as_str)
        .context("missing tool name")?;
    let args = params
        .get("arguments")
        .cloned()
        .unwrap_or_else(|| json!({}));
    let session_id = required_session_id(&args)?;
    let session = config::load_session(&session_id).await?;
    match name {
        "session_info" => {
            approvals::activity(&session.id, "Read session info", None).await;
            text_result(serde_json::to_string_pretty(&session)?)
        }
        "get_image" => {
            let path = resolve_path(&session.cwd, required_path(&args, "path")?);
            let result = get_image(&path).await;
            report_result(
                &session.id,
                format!("Read image {}", display_path(&path, &session.cwd)),
                &result,
            )
            .await;
            result
        }
        "read_file" => {
            let path = resolve_path(&session.cwd, required_path(&args, "path")?);
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
        "list_directory" => {
            let path = resolve_path(&session.cwd, required_path(&args, "path")?);
            let result = list_directory(&path).await;
            report_result(
                &session.id,
                format!("Listed {}", display_path(&path, &session.cwd)),
                &result,
            )
            .await;
            text_result(result?)
        }
        "write_file" => write_file(&args, &session).await,
        "execute" => execute(&args, &session).await,
        "execute_shell" => execute_shell(&args, &session).await,
        "start_command" => start_command(&args, &session).await,
        "poll_job" => poll_job(&args, &session).await,
        "stop_job" => stop_job(&args, &session).await,
        "heartbeat_start" => heartbeat_start(&args, &session).await,
        "heartbeat_wait" => heartbeat_wait(&args, &session).await,
        "heartbeat_status" => heartbeat_status(&args, &session).await,
        "heartbeat_stop" => heartbeat_stop(&args, &session).await,
        "without_sandbox" => without_sandbox(&args, &session).await,
        "without_sandbox_shell" => without_sandbox_shell(&args, &session).await,
        _ => anyhow::bail!("unknown tool: {name}"),
    }
}

fn heartbeat_name(args: &Value) -> Result<String> {
    let name = args
        .get("name")
        .and_then(Value::as_str)
        .unwrap_or(HEARTBEAT_DEFAULT_NAME);
    anyhow::ensure!(!name.is_empty(), "heartbeat name must not be empty");
    anyhow::ensure!(name.len() <= 64, "heartbeat name must be at most 64 bytes");
    anyhow::ensure!(
        name.bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.')),
        "heartbeat name may contain only ASCII letters, digits, '.', '_' and '-'"
    );
    Ok(name.to_owned())
}

fn heartbeat_key(session_id: &str, name: &str) -> (String, String) {
    (session_id.to_owned(), name.to_owned())
}

fn heartbeat_interval(args: &Value) -> Result<Duration> {
    let seconds = args
        .get("interval_seconds")
        .and_then(Value::as_u64)
        .context("missing interval_seconds")?;
    let interval = Duration::from_secs(seconds);
    anyhow::ensure!(!interval.is_zero(), "interval_seconds must be at least 1");
    anyhow::ensure!(
        interval <= HEARTBEAT_MAX_INTERVAL,
        "interval_seconds must be at most {}",
        HEARTBEAT_MAX_INTERVAL.as_secs()
    );
    Ok(interval)
}

fn heartbeat_max_wait(args: &Value) -> Result<Duration> {
    let seconds = args
        .get("max_wait_seconds")
        .and_then(Value::as_u64)
        .unwrap_or(HEARTBEAT_MAX_WAIT.as_secs());
    let wait = Duration::from_secs(seconds);
    anyhow::ensure!(!wait.is_zero(), "max_wait_seconds must be at least 1");
    anyhow::ensure!(
        wait <= HEARTBEAT_MAX_WAIT,
        "max_wait_seconds must be at most {}",
        HEARTBEAT_MAX_WAIT.as_secs()
    );
    Ok(wait)
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

async fn heartbeat_start(args: &Value, session: &config::Session) -> Result<Value> {
    let name = heartbeat_name(args)?;
    let interval = heartbeat_interval(args)?;
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

async fn heartbeat_wait(args: &Value, session: &config::Session) -> Result<Value> {
    let name = heartbeat_name(args)?;
    let max_wait = heartbeat_max_wait(args)?;
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
        let result = heartbeat_result(&name, "tick", heartbeat, now);
        drop(all);
        approvals::activity(&session.id, format!("Heartbeat {name} tick"), None).await;
        return result;
    }

    heartbeat_result(&name, "waiting", heartbeat, now)
}

async fn heartbeat_status(args: &Value, session: &config::Session) -> Result<Value> {
    let name = heartbeat_name(args)?;
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

async fn heartbeat_stop(args: &Value, session: &config::Session) -> Result<Value> {
    let name = heartbeat_name(args)?;
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

fn required_path(args: &Value, name: &str) -> Result<PathBuf> {
    args.get(name)
        .and_then(Value::as_str)
        .map(PathBuf::from)
        .context(format!("missing {name}"))
}

fn required_session_id(args: &Value) -> Result<String> {
    let value = args
        .get("session_id")
        .and_then(Value::as_str)
        .context("missing session_id; ask the user to run `local-mcp start` and provide its ID")?;
    config::validate_session_id(value)?;
    Ok(value.to_owned())
}

fn resolve_path(session_cwd: &Path, path: PathBuf) -> PathBuf {
    if path.is_absolute() {
        path
    } else {
        session_cwd.join(path)
    }
}

fn cwd(args: &Value, session_cwd: &Path) -> Result<PathBuf> {
    let path = args
        .get("cwd")
        .and_then(Value::as_str)
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

async fn write_file(args: &Value, session: &config::Session) -> Result<Value> {
    let absolute = resolve_path(&session.cwd, required_path(args, "path")?);
    let parent = absolute.parent().context("file has no parent directory")?;
    let parent = std::fs::canonicalize(parent)
        .with_context(|| format!("parent does not exist: {}", parent.display()))?;
    let content = args
        .get("content")
        .and_then(Value::as_str)
        .context("missing content")?;
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

async fn execute(args: &Value, session: &config::Session) -> Result<Value> {
    let (rendered_command, mut handle) = spawn_sandboxed_command("execute", args, session).await?;

    match tokio::time::timeout(FOREGROUND_TIMEOUT, &mut handle).await {
        Ok(joined) => text_result(joined.context("command task failed")??),
        Err(_) => store_job(session, rendered_command, handle, "Backgrounded").await,
    }
}

async fn execute_shell(args: &Value, session: &config::Session) -> Result<Value> {
    validate_shell_args(args)?;
    let script = required_script(args)?;
    let command = shell_command(&script)?;
    let cwd = cwd(args, &session.cwd)?;
    let mut roots = session.permitted_directories.clone();
    if !roots.iter().any(|root| cwd.starts_with(root)) {
        roots.push(cwd.clone());
    }
    let display = shell_activity_label(&script);
    approvals::activity(&session.id, format!("Running {display}"), None).await;
    let session_id = session.id.clone();
    let task_display = display.clone();
    let handle = tokio::spawn(async move {
        let result = sandbox::run(&command, &cwd, &roots, None)
            .await
            .and_then(render_output);
        report_command_finished(session_id, &task_display, &result).await;
        result
    });

    let mut handle = handle;
    match tokio::time::timeout(FOREGROUND_TIMEOUT, &mut handle).await {
        Ok(joined) => text_result(joined.context("shell command task failed")??),
        Err(_) => store_job(session, display, handle, "Backgrounded").await,
    }
}

async fn start_command(args: &Value, session: &config::Session) -> Result<Value> {
    let (rendered_command, handle) =
        spawn_sandboxed_command("start_command", args, session).await?;
    store_job(session, rendered_command, handle, "Started").await
}

async fn spawn_sandboxed_command(
    operation: &str,
    args: &Value,
    session: &config::Session,
) -> Result<(String, JoinHandle<Result<String>>)> {
    let command = required_command(args)?;
    let cwd = cwd(args, &session.cwd)?;
    #[cfg(windows)]
    if !approvals::request(
        &session.id,
        operation,
        format!("argv: {command:?}"),
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
    let rendered_command = render_command(&command);
    approvals::activity(&session.id, format!("Running {rendered_command}"), None).await;
    let session_id = session.id.clone();
    let task_command = rendered_command.clone();
    let handle = tokio::spawn(async move {
        let result = sandbox::run(&command, &cwd, &roots, None)
            .await
            .and_then(render_output);
        report_command_finished(session_id, &task_command, &result).await;
        result
    });
    Ok((rendered_command, handle))
}

async fn store_job(
    session: &config::Session,
    rendered_command: String,
    handle: JoinHandle<Result<String>>,
    activity: &str,
) -> Result<Value> {
    let job_id = Uuid::new_v4();
    jobs().lock().unwrap().insert(
        job_id,
        Job {
            session_id: session.id.clone(),
            command: rendered_command.clone(),
            handle,
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

async fn poll_job(args: &Value, session: &config::Session) -> Result<Value> {
    let job_id = required_job_id(args)?;
    let finished = {
        let jobs = jobs().lock().unwrap();
        let job = jobs.get(&job_id).context("unknown job_id")?;
        anyhow::ensure!(
            job.session_id == session.id,
            "job does not belong to this session"
        );
        job.handle.is_finished()
    };
    if !finished {
        return text_result(json!({"status":"running","job_id":job_id}).to_string());
    }

    let job = jobs().lock().unwrap().remove(&job_id).unwrap();
    let result = job.handle.await.context("background command task failed")?;
    text_result(result?)
}

async fn stop_job(args: &Value, session: &config::Session) -> Result<Value> {
    let job_id = required_job_id(args)?;
    let job = {
        let mut jobs = jobs().lock().unwrap();
        let job = jobs.get(&job_id).context("unknown job_id")?;
        anyhow::ensure!(
            job.session_id == session.id,
            "job does not belong to this session"
        );
        jobs.remove(&job_id).unwrap()
    };
    job.handle.abort();
    let _ = job.handle.await;
    approvals::activity(
        &session.id,
        format!("Stopped {}", job.command),
        Some(format!("└ job {job_id}")),
    )
    .await;
    text_result(json!({"status":"stopped","job_id":job_id}).to_string())
}

fn required_job_id(args: &Value) -> Result<Uuid> {
    let value = args
        .get("job_id")
        .and_then(Value::as_str)
        .context("missing job_id")?;
    Uuid::parse_str(value).context("invalid job_id")
}

fn required_command(args: &Value) -> Result<Vec<String>> {
    args.get("command")
        .and_then(Value::as_array)
        .context("missing command")?
        .iter()
        .map(|item| {
            item.as_str()
                .map(str::to_owned)
                .context("command entries must be strings")
        })
        .collect()
}

fn validate_shell_args(args: &Value) -> Result<()> {
    let object = args
        .as_object()
        .context("tool arguments must be an object")?;
    for key in object.keys() {
        anyhow::ensure!(
            matches!(key.as_str(), "session_id" | "script" | "cwd"),
            "unknown shell-tool argument: {key}"
        );
    }
    Ok(())
}

fn required_script(args: &Value) -> Result<String> {
    args.get("script")
        .and_then(Value::as_str)
        .map(str::to_owned)
        .context("missing script; shell tools require script to be a string")
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
    let mut preview = script
        .lines()
        .filter_map(|line| {
            let line = line.trim();
            if line.is_empty() {
                return None;
            }
            let lower = line.to_ascii_lowercase();
            let sensitive = [
                "authorization",
                "cookie",
                "password",
                "passwd",
                "secret",
                "token",
                "api_key",
                "apikey",
                "private_key",
            ]
            .iter()
            .any(|marker| lower.contains(marker));
            Some(if sensitive {
                "[redacted sensitive line]".to_owned()
            } else {
                line.to_owned()
            })
        })
        .take(3)
        .collect::<Vec<_>>()
        .join(" ⏎ ");
    if preview.is_empty() {
        preview = "<empty>".to_owned();
    }

    let mut chars = preview.chars();
    let bounded = chars.by_ref().take(SHELL_PREVIEW_LIMIT).collect::<String>();
    if chars.next().is_some() {
        format!("{bounded}…")
    } else {
        bounded
    }
}

async fn without_sandbox(args: &Value, session: &config::Session) -> Result<Value> {
    let command = required_command(args)?;
    let cwd = cwd(args, &session.cwd)?;
    if !approvals::request(
        &session.id,
        "without_sandbox",
        format!("argv: {command:?}"),
        cwd.clone(),
    )
    .await?
    {
        anyhow::bail!("user denied without_sandbox")
    }
    run_and_report(session.id.clone(), command, cwd, true, &[]).await
}

async fn without_sandbox_shell(args: &Value, session: &config::Session) -> Result<Value> {
    validate_shell_args(args)?;
    let script = required_script(args)?;
    let command = shell_command(&script)?;
    let cwd = cwd(args, &session.cwd)?;
    let display = shell_activity_label(&script);
    if !approvals::request(
        &session.id,
        "without_sandbox_shell",
        format!("shell: {SHELL_PROGRAM}\n{display}"),
        cwd.clone(),
    )
    .await?
    {
        anyhow::bail!("user denied without_sandbox_shell")
    }
    run_and_report_named(session.id.clone(), command, cwd, true, &[], display).await
}

async fn run_and_report(
    session_id: String,
    command: Vec<String>,
    cwd: PathBuf,
    unrestricted: bool,
    roots: &[PathBuf],
) -> Result<Value> {
    let rendered_command = render_command(&command);
    run_and_report_named(
        session_id,
        command,
        cwd,
        unrestricted,
        roots,
        rendered_command,
    )
    .await
}

async fn run_and_report_named(
    session_id: String,
    command: Vec<String>,
    cwd: PathBuf,
    unrestricted: bool,
    roots: &[PathBuf],
    rendered_command: String,
) -> Result<Value> {
    approvals::activity(&session_id, format!("Running {rendered_command}"), None).await;
    let output = if unrestricted {
        sandbox::run_unrestricted(&command, &cwd, None).await
    } else {
        sandbox::run(&command, &cwd, roots, None).await
    };
    let result = output.and_then(render_output);
    report_command_finished(session_id, &rendered_command, &result).await;
    text_result(result?)
}

fn render_command(command: &[String]) -> String {
    command
        .iter()
        .map(|arg| shell_word(arg))
        .collect::<Vec<_>>()
        .join(" ")
}

async fn report_command_finished(session_id: String, command: &str, result: &Result<String>) {
    let detail = match result {
        Ok(text) => command_summary(text),
        Err(error) => Some(format!("└ Error: {error:#}")),
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
    let output = if stdout.is_empty() { stderr } else { stdout };
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
    let text = json!({"exit_code":output.status,"stdout":output.stdout,"stderr":output.stderr})
        .to_string();
    if output.status == 0 {
        Ok(text)
    } else {
        anyhow::bail!(text)
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

    #[test]
    fn shell_argument_validation_rejects_wrong_types_and_unknown_fields() {
        assert!(required_script(&json!({"script": ["echo", "hello"]})).is_err());
        assert!(
            validate_shell_args(&json!({
                "session_id": "example",
                "script": "true",
                "extra": true
            }))
            .is_err()
        );
    }

    #[cfg(unix)]
    #[test]
    fn shell_command_uses_bash_and_preserves_multiline_programs() {
        let script =
            "set -euo pipefail\nprintf '%s\n' hello | sed 's/hello/world/'\ncat <<'EOF'\ndone\nEOF";
        let command = shell_command(script).unwrap();
        assert_eq!(command, vec!["bash", "-c", script]);
    }

    #[test]
    fn shell_activity_preview_is_bounded_and_redacts_sensitive_lines() {
        let script = format!(
            "echo safe\nTOKEN={}\necho {}",
            "x".repeat(200),
            "y".repeat(300)
        );
        let preview = shell_script_preview(&script);
        assert!(preview.contains("echo safe"));
        assert!(preview.contains("[redacted sensitive line]"));
        assert!(preview.chars().count() <= SHELL_PREVIEW_LIMIT + 1);
        assert!(!preview.contains(&"x".repeat(20)));
    }

    #[cfg(unix)]
    async fn start_approval_responder(
        session_id: &str,
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
            assert!(line.contains("without_sandbox_shell"));
            stream
                .write_all(format!("{response}\n").as_bytes())
                .await
                .unwrap();
        })
    }

    #[cfg(unix)]
    fn shell_test_session(directory: &Path) -> config::Session {
        config::Session {
            id: format!("shell-test-{}", Uuid::new_v4()),
            cwd: directory.to_owned(),
            permitted_directories: vec![directory.to_owned()],
        }
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn denied_shell_approval_never_starts_the_process() {
        let directory = std::env::temp_dir().join(format!("local-mcp-shell-{}", Uuid::new_v4()));
        tokio::fs::create_dir_all(&directory).await.unwrap();
        let session = shell_test_session(&directory);
        let marker = directory.join("should-not-exist");
        let responder = start_approval_responder(&session.id, "deny").await;

        let error = without_sandbox_shell(
            &json!({"script": format!("printf started > {}", marker.display())}),
            &session,
        )
        .await
        .unwrap_err();
        responder.await.unwrap();

        assert!(error.to_string().contains("user denied"));
        assert!(!marker.exists());
        let _ = tokio::fs::remove_file(config::socket_path(&session.id).unwrap()).await;
        tokio::fs::remove_dir_all(directory).await.unwrap();
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn approved_shell_honors_cwd_and_reports_nonzero_exit() {
        let directory = std::env::temp_dir().join(format!("local-mcp-shell-{}", Uuid::new_v4()));
        tokio::fs::create_dir_all(&directory).await.unwrap();
        let session = shell_test_session(&directory);
        let responder = start_approval_responder(&session.id, "allow").await;

        let error = without_sandbox_shell(&json!({"script": "pwd > cwd.txt\nexit 7"}), &session)
            .await
            .unwrap_err();
        responder.await.unwrap();

        assert!(error.to_string().contains("\"exit_code\":7"));
        let recorded = tokio::fs::read_to_string(directory.join("cwd.txt"))
            .await
            .unwrap();
        assert_eq!(recorded.trim(), directory.to_string_lossy());
        let _ = tokio::fs::remove_file(config::socket_path(&session.id).unwrap()).await;
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
}
