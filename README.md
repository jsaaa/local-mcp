# local-mcp

`local-mcp` exposes basic local-machine capabilities as MCP tools: file reads,
image reads, directory listings, sandboxed file writes and commands, plus explicitly approved
unsandboxed command execution. It intentionally does not provide web search or a
dedicated network-request tool.

Commands are isolated with OpenAI Codex's `codex-rs/sandboxing`: Landlock and the
Linux sandbox helper on Linux, and Seatbelt (`sandbox-exec`) on macOS. Network
access is denied for ordinary commands.

## Usage

```sh
cargo build --release

# Run one persistent MCP server (for example through a tunnel):
local-mcp mcp

# In another terminal, start a session in the project directory:
cd ./some-project
local-mcp start

# Or choose a stable session ID (letters, numbers, "-", "_", and "."):
local-mcp start my-project

# Start directly in yolo mode when all unsandboxed calls should be allowed:
local-mcp start my-project --yolo

# Give the printed session ID to the agent in your prompt. The agent includes it
# in each local-mcp tool call.

# In the approvals UI, allow every unsandboxed call for the session:
/permissions yolo

# Manage the current session from the start screen:
/permission ask
/permission yolo
/permission allow ../another-project
/permission revoke ../another-project
/permission list
/permission status
```

With Nix, `curl` and `bash` are included in the runtime environment. Linux builds
also include `bwrap`:

```sh
nix run github:OWNER/local-mcp
nix develop
nix build
```

The session working directory is the directory where `local-mcp start` was run;
there is no separate persistent cwd setting. Sandboxed calls are always allowed
and have no network access. `without_sandbox`
runs with the service user's full host permissions and network access, so it asks
the approvals process before every call. `/permissions yolo` disables those
prompts only for the lifetime of that session; `local-mcp start --yolo` starts in
the same mode immediately. `/permissions ask` turns prompts back on. The singular
`/permission ...` spelling is also accepted.
Every tool takes a `session_id`. The agent can call `session_info` with the ID
from the prompt to confirm the working directory and sandbox roots. One
`local-mcp mcp` process can therefore serve multiple independently configured
sessions.
`get_image` returns PNG, JPEG, GIF, WebP, BMP, TIFF, and AVIF files as native MCP
image content. Relative image paths are resolved from the session working directory.

For long-running ChatGPT Web turns, `heartbeat_start` and `heartbeat_wait` provide
an in-turn heartbeat similar to a `/heartbeat every 5m` loop. Start a schedule,
then call `heartbeat_wait` repeatedly while idle. The wait call uses short long-poll
chunks (at most 25 seconds) so the agent should immediately call it again when it
returns `status: "waiting"`. A scheduled tick is delivered only when
`heartbeat_wait` is actively waiting. If the agent is still doing work when a tick
passes, that tick is counted as skipped and is not queued for catch-up; the next
future tick remains scheduled. `heartbeat_status` reports delivered/skipped counts,
and `heartbeat_stop` removes the schedule.

This heartbeat keeps an already-running ChatGPT turn alive through repeated tool
calls; MCP does not provide a way for local-mcp to start a new ChatGPT turn after
that turn has ended. For example, an agent emulating `every 5m` should use
`interval_seconds: 300`, wait until `status: "tick"`, do one work cycle, and then
resume calling `heartbeat_wait`.

Each session uses its own local IPC endpoint: an explicitly permission-restricted
Unix domain socket on Unix, or a named pipe using Windows' default security
descriptor. Both the MCP server and the start UI block on I/O, so idle operation
and pending approvals do not use polling timers.

The `start` screen also receives live activity from MCP calls. It shows file and
image reads, directory listings, file edits with unified diffs and line counts,
and command start/completion with output, in a compact Codex-style timeline.
`execute` returns its normal result for commands that finish within 30 seconds.
Longer commands continue in the background and return a `job_id`; use `poll_job`
to check for completion or `stop_job` to terminate them. Use `start_command`
when a command should run in the background immediately without the 30-second
foreground wait.

### Process-tree lifecycle

Every command is launched with an owned process-tree lifecycle. On Unix,
local-mcp creates a dedicated process group and signals the entire group. On
Windows, it uses the documented `taskkill /T` process-tree operation. A requested
stop first attempts graceful tree termination, waits for a bounded 500 ms grace
period, and then escalates to a forced tree kill. The direct child is always
waited and reaped before `stop_job` returns.

The same lifecycle primitive is used by explicit stop requests, internal
execution timeouts, and cancellation cleanup. If an execution future is dropped
or its Tokio task is aborted, a synchronous Drop guard force-terminates the tree
instead of relying only on `kill_on_drop` for the direct child. On Windows this
requires the system `taskkill.exe`, which is part of supported Windows releases.

Command result JSON includes `termination` and `termination_trigger` fields. The
possible lifecycle results are `exited`, `stopped`, `timeout`, `cancelled`, and
`forced_kill`; a forced result also identifies whether stop, timeout, or
cancellation triggered the escalation. Normal non-zero process exits remain
command errors, while lifecycle termination is returned as an inspectable
terminal outcome. Repeating `stop_job` for a recently stopped in-memory job is
idempotent and returns the cached terminal result. The cache is bounded to 256
entries per MCP process.

On Linux, the build produces `local-mcp` and its sibling `codex-linux-sandbox`;
install or copy both into the same directory, and ensure `bwrap` (bubblewrap) is
available in `PATH`. On macOS, only `local-mcp` is needed; sandboxed commands use
the system `/usr/bin/sandbox-exec`. Windows uses named-pipe IPC and direct argv
execution; it does not currently provide the filesystem/network sandbox enforced
by Linux and macOS. Consequently, `execute` and `start_command` require approval
on Windows unless the session is in yolo mode, while `write_file` writes directly
to the requested host path. The Windows named pipe uses the
[default security descriptor](https://learn.microsoft.com/en-us/windows/win32/ipc/named-pipe-security-and-access-rights),
which grants full control to LocalSystem, administrators, and the creator owner,
and read access to Everyone and anonymous users; unlike Unix, `local-mcp` does not
install an explicit per-user ACL. Windows builds use the MSVC Rust target and
require Visual Studio Build Tools with the "Desktop development with C++"
workload. Build from a Developer PowerShell with
`cargo build --locked --release`.
