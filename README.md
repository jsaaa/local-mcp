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

### Argv commands and shell programs

Use `execute`, `start_command`, and `without_sandbox` for direct process execution.
Their `command` field is always an argv array, so no shell parsing or implicit
quoting occurs:

```json
{"session_id":"...","command":["cargo","test","--locked"],"cwd":"."}
```

On Unix, use `execute_shell` for a sandboxed multi-line Bash program and
`without_sandbox_shell` for an approved host Bash program. Their `script` field is
a string and supports pipelines, heredocs, conditionals, and
`set -euo pipefail`:

```json
{
  "session_id": "...",
  "script": "set -euo pipefail\nprintf '%s\n' hello | sed 's/hello/world/'",
  "cwd": "."
}
```

`execute_shell` has the same filesystem sandbox and denied network access as
`execute`. `without_sandbox_shell` has full host permissions and network access,
so approval is requested before Bash starts unless the session is in yolo mode.
Activity output names Bash and includes only a bounded preview with common
credential-bearing lines redacted and terminal control characters escaped. The
complete script is not dumped into the activity timeline. `execute_shell` follows
the same 30-second foreground/background lifecycle as `execute`, while
`without_sandbox_shell` follows the approved unrestricted lifecycle described
below.

The shell-program tools are intentionally unsupported on Windows because silently
mapping Bash semantics to PowerShell would be unsafe. On Windows, invoke
PowerShell explicitly through an argv tool instead.
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
`execute` returns its normal result for sandboxed commands that finish within 30
seconds. Longer commands continue in the background and return a `job_id`; use
`poll_job` to check for completion or `stop_job` to stop them. Use `start_command`
when a sandboxed command should run in the background immediately.

Unrestricted execution has the same lifecycle. `without_sandbox` requests
approval before launching, returns a normal result within 30 seconds, and
automatically returns a `job_id` if the approved process is still running.
`start_without_sandbox` requests approval and then starts the unrestricted job in
the background immediately. Approval is always completed before process spawn;
immediately after spawn the process is registered as a session-owned job before
any foreground wait begins. If the MCP call is cancelled before a `job_id`
response is delivered, that registered job is removed, its complete process tree
is terminated, and a cancelled terminal result is persisted. A denied request
creates no process and no job. Both kinds of unrestricted jobs
remain owned by the originating session and are polled or stopped with the same
`poll_job` and `stop_job` tools. Yolo mode skips the prompt but does not change
job behavior or session ownership. Unrestricted background jobs retain the
service user's full filesystem and network permissions for their entire lifetime.

### Bounded command output and full logs

Every foreground and background command receives a job ID. While the process is
running, stdout and stderr are streamed directly to separate files under
local-mcp's state directory. RAM retains only bounded head/tail previews, so log
volume cannot grow the server's capture buffers without limit. The inline result
contains UTF-8-safe previews, original byte counts, truncation flags,
`termination` metadata, and `local-mcp://jobs/<job-id>/<stream>` identifiers.
Non-zero exits and bounded stderr previews remain visible for very large output.

The complete serialized JSON-RPC tool-result envelope, including both `content`
and `structuredContent`, string re-escaping, and a 256-byte serialized request-ID
budget, is capped by
`LOCAL_MCP_INLINE_OUTPUT_BYTES`. Oversized request IDs are rejected before tool
dispatch. The default is 16384 bytes; configured values are clamped to 2048
through 1048576 bytes. Command and approval activity previews are bounded
separately so a large argv or command log cannot flood the permission timeline.

Use `read_job_log` to read a stored stream in bounded ranges:

```json
{
  "session_id": "...",
  "job_id": "...",
  "stream": "stdout",
  "offset": 0,
  "length": 8192
}
```

`length` is limited to 65536 bytes per call. Valid UTF-8 ranges are returned as
text; arbitrary binary ranges are returned as base64. Log paths are derived from
the owning session, so another session cannot read the same job ID. Active log
directories are protected by OS file locks. Completed entries older than seven
days are removed, and at most 128 command-log directories are retained per
session. If all retained entries are active, starting another capture fails
closed rather than deleting a running command's logs.

### Persistent job journal

Command lifecycle metadata is persisted below local-mcp's state
directory in a session-scoped journal. Each update is written to a unique
temporary file, flushed, and atomically renamed to a versioned JSON snapshot. A
partial temporary write is ignored and cleaned up; a corrupt latest snapshot
fails closed for that job without preventing the MCP server from starting.

`poll_job` first checks the live in-memory handle and otherwise loads the journal.
Completed, failed, and stopped results therefore remain available after restart.
Calling `stop_job` after a job has already reached a terminal state returns that
state and result instead of rewriting it. A record still marked `running` with no
live handle is explicitly changed to `orphaned`; the first journal format does
not claim that an arbitrary process can be reattached.

Use `list_jobs` to recover IDs or inspect state after a lost response:

```json
{
  "session_id": "...",
  "state": "completed",
  "offset": 0,
  "limit": 50
}
```

The state filter accepts `running`, `completed`, `failed`, `stopped`, and
`orphaned`; `limit` is capped at 100. Terminal records older than 30 days are
removed, and each session has a hard limit of 256 records. Running records are
not deleted merely to satisfy the count limit; if no terminal record can be
removed, creation of another job fails closed. Persisted result/error fields are
capped at 64 KiB of serialized JSON and replaced by a UTF-8-safe head/tail
envelope when truncated. Rendered commands are bounded, and each complete
journal snapshot is capped at 128 KiB. Corrupt entries are omitted from list
results and counted in `corrupt_entries`. On Windows, argv jobs are recorded as
`unrestricted` because they execute directly on the host after approval.

### Structured command results

`execute`, `start_command`, `poll_job`, `stop_job`, `without_sandbox`, and
`start_without_sandbox` publish a
versioned `outputSchema` and return the same command-result contract in MCP
`structuredContent`. The current schema version is `1`. Its core fields identify
`running`, `completed`, `failed`, or `stopped` status; distinguish
`process_exit`, `spawn_error`, `cancellation`, `approval_denied`,
`invalid_arguments`, and `internal` failures; and carry the exit code,
retryability, and job ID. The same object also contains process-tree termination
metadata, bounded stdout/stderr head and tail previews, original byte counts,
truncation flags, and full-log resource identifiers.

A non-zero child exit is an expected tool outcome with `isError: true`, not a
JSON-RPC transport failure or a JSON string nested inside an error message.
Argument validation and approval denial are likewise returned as typed tool
outcomes when they belong to a command lifecycle. Protocol framing failures,
unknown tool names, missing sessions, and faults outside that lifecycle may still
use MCP/JSON-RPC errors.

The `content` array remains present with a concise human-readable fallback, so
clients that ignore `structuredContent` continue to display useful output.
Consumers should branch on `schema_version` before relying on fields. Additive,
backward-compatible clarifications may retain the current version; removing
fields, changing their types or meanings, or changing enum semantics requires a
new version and output schema. The inline-output budget described above applies
to the complete result, including both the fallback and `structuredContent`.

### Process-tree lifecycle

Every command is launched with an owned process-tree lifecycle. On Unix,
local-mcp creates a dedicated process group and signals the entire group. On
Windows, it starts the process suspended, assigns it to a Job Object configured
with `JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE`, and only then resumes execution. A
requested stop first attempts graceful tree termination, waits for a bounded
500 ms grace period, and then escalates to a forced tree kill. Direct-child exit
does not release ownership: local-mcp verifies that the Unix process group or
Windows Job Object has no active descendants before returning. The direct child
is always waited and reaped before `stop_job` returns.

The same lifecycle primitive is used by explicit stop requests, internal
execution timeouts, and cancellation cleanup. If an execution future is dropped
or its Tokio task is aborted, a synchronous Drop guard terminates the Unix group;
on Windows, closing the owned Job Object triggers kill-on-close for every
associated process. Cleanup/query failures are returned as errors rather than
being reported as normal command exits.

Command result JSON includes `termination` and `termination_trigger` fields. The
possible lifecycle results are `exited`, `stopped`, `timeout`, `cancelled`, and
`forced_kill`; a forced result also identifies whether stop, timeout,
cancellation, or post-completion descendant cleanup triggered the escalation.
Normal non-zero process exits remain command errors, while lifecycle termination
is returned as an inspectable terminal outcome. Repeating `stop_job` is
idempotent and returns the persisted terminal result, including after an MCP
server restart.

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
