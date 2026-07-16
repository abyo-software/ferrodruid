// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

#![forbid(unsafe_code)]

use clap::{Parser, Subcommand};
use ferrodruid_cli_lib::DruidClient;

/// FerroDruid admin CLI.
#[derive(Parser)]
#[command(name = "ferrodruid-cli", version, about)]
struct Cli {
    /// Router / coordinator URL.
    #[arg(short, long, default_value = "http://localhost:8888")]
    url: String,

    /// Subcommand to execute.
    #[command(subcommand)]
    command: Command,
}

/// Available CLI subcommands.
#[derive(Subcommand)]
enum Command {
    /// Show server status.
    Status,
    /// Health check.
    Health,
    /// List all datasources.
    Datasources,
    /// Execute a native JSON query.
    Query {
        /// Query as a JSON string.
        query_json: String,
    },
    /// Execute an SQL query.
    Sql {
        /// SQL query string.
        query: String,
    },
    /// List all tasks.
    Tasks,
    /// List all supervisors.
    Supervisors,
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    let cli = Cli::parse();
    let client = DruidClient::new(&cli.url);

    let result = match cli.command {
        Command::Status => client.status().await.map(|v| format_json(&v)),
        Command::Health => client.health().await.map(|ok| {
            if ok {
                "OK".to_string()
            } else {
                "UNHEALTHY".to_string()
            }
        }),
        Command::Datasources => client
            .list_datasources()
            .await
            .map(|ds| ds.into_iter().collect::<Vec<_>>().join("\n")),
        Command::Query { query_json } => {
            match serde_json::from_str::<serde_json::Value>(&query_json) {
                Ok(q) => client.native_query(&q).await.map(|v| format_json(&v)),
                Err(e) => Err(ferrodruid_cli_lib::CliError::Json(e.to_string())),
            }
        }
        Command::Sql { query } => client.sql_query(&query).await.map(|v| format_json(&v)),
        Command::Tasks => client.list_tasks().await.map(|v| format_json(&v)),
        Command::Supervisors => client.list_supervisors().await.map(|v| format_json(&v)),
    };

    match result {
        Ok(output) => println!("{output}"),
        Err(e) => {
            eprintln!("Error: {e}");
            std::process::exit(1);
        }
    }
}

fn format_json(v: &serde_json::Value) -> String {
    serde_json::to_string_pretty(v).unwrap_or_else(|_| v.to_string())
}
