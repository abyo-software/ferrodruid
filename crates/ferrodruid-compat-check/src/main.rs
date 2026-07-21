// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

#![forbid(unsafe_code)]
#![deny(missing_docs)]

//! `ferro-compat-check` binary — see the crate-level docs of
//! `ferrodruid_compat_check` for what the battery covers.

use ferrodruid_compat_check::probe::Section;
use ferrodruid_compat_check::report::{render_human, render_json, summarize};
use ferrodruid_compat_check::runner::{Config, run};

const USAGE: &str = "\
ferro-compat-check — verify Druid-SQL compatibility of a running FerroDruid

USAGE:
    ferro-compat-check --url <URL> [OPTIONS]

OPTIONS:
    --url <URL>            Base URL of the running FerroDruid (e.g. http://host:8888). Required.
    --auth <user:pass>     HTTP Basic credentials.
    --datasource <PREFIX>  Prefix for the three self-ingested fixture datasources
                           (<PREFIX>_wiki / _null / _rollup). Default: compatcheck.
    --section <NAME>       Run one battery section only. Repeatable.
                           NAME: ping | aggregates | superset | null | grains | rollup | all.
                           Default: all.
    --json                 Emit the machine-readable JSON report on stdout.
    --cleanup              After the run, soft-disable the fixture datasources
                           (DELETE /druid/coordinator/v1/datasources/<name>). Best-effort.
    --timeout-secs <N>     Per-request timeout. Default: 30.
    -h, --help             Show this help.

EXIT CODES:
    0  every assertive probe matched live-verified Apache Druid behavior
    1  at least one assertive probe failed
    2  setup error (bad arguments, server unreachable)

The expected values are live-measured Apache Druid 30.0.1-36.0.0 outputs
(committed diff-harness evidence, 2026-07-11); no Druid cluster is needed.
Known-divergent surfaces (EXPLAIN body, week_ending_saturday grain) are
reported as INFO, never asserted.";

struct Args {
    config: Config,
    json: bool,
}

fn parse_args(argv: &[String]) -> Result<Args, String> {
    let mut url: Option<String> = None;
    let mut auth: Option<(String, String)> = None;
    let mut prefix = "compatcheck".to_string();
    let mut sections: Vec<Section> = Vec::new();
    let mut all_sections = false;
    let mut json = false;
    let mut cleanup = false;
    let mut timeout_secs: u64 = 30;

    let mut i = 0;
    let next_value = |i: &mut usize, flag: &str| -> Result<String, String> {
        *i += 1;
        argv.get(*i)
            .cloned()
            .ok_or_else(|| format!("{flag} requires a value"))
    };
    while i < argv.len() {
        match argv[i].as_str() {
            "--url" => url = Some(next_value(&mut i, "--url")?),
            "--auth" => {
                let v = next_value(&mut i, "--auth")?;
                let (user, pass) = v
                    .split_once(':')
                    .ok_or_else(|| "--auth expects user:pass".to_string())?;
                auth = Some((user.to_string(), pass.to_string()));
            }
            "--datasource" => prefix = next_value(&mut i, "--datasource")?,
            "--section" => {
                let v = next_value(&mut i, "--section")?;
                if v == "all" {
                    all_sections = true;
                } else {
                    let sec = Section::parse(&v).ok_or_else(|| {
                        format!(
                            "unknown section '{v}' (expected ping, aggregates, superset, \
                             null, grains, rollup, or all)"
                        )
                    })?;
                    if !sections.contains(&sec) {
                        sections.push(sec);
                    }
                }
            }
            "--json" => json = true,
            "--cleanup" => cleanup = true,
            "--timeout-secs" => {
                let v = next_value(&mut i, "--timeout-secs")?;
                timeout_secs = v
                    .parse()
                    .map_err(|_| format!("--timeout-secs expects a number, got '{v}'"))?;
            }
            "-h" | "--help" => return Err(String::new()),
            other => return Err(format!("unknown argument '{other}'")),
        }
        i += 1;
    }

    let base_url = url.ok_or_else(|| "--url is required".to_string())?;
    // Fixture names end up in SQL; keep the prefix a safe identifier.
    if prefix.is_empty()
        || !prefix
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_')
    {
        return Err(format!(
            "--datasource prefix '{prefix}' must be a non-empty [A-Za-z0-9_] identifier"
        ));
    }
    Ok(Args {
        config: Config {
            base_url,
            auth,
            prefix,
            sections: if all_sections || sections.is_empty() {
                None
            } else {
                Some(sections)
            },
            cleanup,
            timeout_secs,
            verbose: !json,
        },
        json,
    })
}

#[tokio::main]
async fn main() {
    let argv: Vec<String> = std::env::args().skip(1).collect();
    let args = match parse_args(&argv) {
        Ok(a) => a,
        Err(msg) => {
            if msg.is_empty() {
                println!("{USAGE}");
                std::process::exit(0);
            }
            eprintln!("error: {msg}\n\n{USAGE}");
            std::process::exit(2);
        }
    };

    let results = match run(&args.config).await {
        Ok(r) => r,
        Err(e) => {
            eprintln!("setup error: {e}");
            std::process::exit(2);
        }
    };

    if args.json {
        let v = render_json(&results, &args.config.base_url, &args.config.prefix);
        match serde_json::to_string_pretty(&v) {
            Ok(s) => println!("{s}"),
            Err(e) => {
                eprintln!("failed to serialize JSON report: {e}");
                std::process::exit(2);
            }
        }
    } else {
        print!("{}", render_human(&results, &args.config.base_url));
    }
    std::process::exit(summarize(&results).exit_code());
}
