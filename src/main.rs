mod approvals;
mod command_output;
mod config;
mod job_journal;
mod mcp;
mod sandbox;
mod tool_args;

use anyhow::Result;
use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(version, about)]
struct Cli {
    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Subcommand)]
enum Command {
    /// Start a session in the current directory and show its permission UI.
    Start {
        /// Session ID to use instead of generating a UUID.
        session_id: Option<String>,
        /// Allow every unsandboxed call from startup without prompting.
        #[arg(long)]
        yolo: bool,
    },
    /// Run the session-independent MCP server over stdin/stdout.
    Mcp,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    match cli.command.unwrap_or(Command::Start {
        session_id: None,
        yolo: false,
    }) {
        Command::Start { session_id, yolo } => approvals::start(session_id.as_deref(), yolo).await,
        Command::Mcp => mcp::serve().await,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_start_yolo_without_session_id() {
        let cli = Cli::try_parse_from(["local-mcp", "start", "--yolo"]).unwrap();
        match cli.command.unwrap() {
            Command::Start { session_id, yolo } => {
                assert_eq!(session_id, None);
                assert!(yolo);
            }
            Command::Mcp => panic!("expected start command"),
        }
    }

    #[test]
    fn parses_start_yolo_with_named_session() {
        let cli = Cli::try_parse_from(["local-mcp", "start", "web-agent", "--yolo"]).unwrap();
        match cli.command.unwrap() {
            Command::Start { session_id, yolo } => {
                assert_eq!(session_id.as_deref(), Some("web-agent"));
                assert!(yolo);
            }
            Command::Mcp => panic!("expected start command"),
        }
    }
}
