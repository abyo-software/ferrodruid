// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

#![forbid(unsafe_code)]
#![deny(missing_docs)]

//! `ferro-logcompat` binary — statically classifies the queries in an
//! Apache Druid request log; it does NOT run anything and needs no data.
//! (For live verification against a running FerroDruid, use
//! `ferrodruid-compat-check` instead.)

use std::io::Write;
use std::process::ExitCode;

use ferrodruid_logcompat::report::{Analyzer, feed_reader, render_json, render_markdown};

const USAGE: &str = "\
ferro-logcompat — static compatibility report for an Apache Druid request log

Statically classifies every query in a Druid broker *file* request log
(druid.request.logging.type=file) through FerroDruid's parse + plan path.
Nothing is executed, no data is read, no network I/O is performed.

USAGE:
    ferro-logcompat <logfile>
    ferro-logcompat --stdin < request.log

OPTIONS:
    --stdin        read the log from standard input instead of a file
    --json         emit the report as JSON (default: Markdown)
    --out <path>   write the report to <path> (default: stdout)
    --top <N>      show the top N incompatible shapes (default: 20)
    --no-redact    include each shape's first-seen query verbatim; the
                   default report contains only literal-stripped shapes
    -h, --help     show this help
";

/// Parsed command-line options.
struct Options {
    logfile: Option<String>,
    stdin: bool,
    json: bool,
    out: Option<String>,
    top: usize,
    no_redact: bool,
}

/// Parse argv. Returns `Err` with a message on invalid usage.
fn parse_args(args: &[String]) -> Result<Options, String> {
    let mut opts = Options {
        logfile: None,
        stdin: false,
        json: false,
        out: None,
        top: 20,
        no_redact: false,
    };
    let mut i = 0;
    while i < args.len() {
        match args[i].as_str() {
            "--stdin" => opts.stdin = true,
            "--json" => opts.json = true,
            "--no-redact" => opts.no_redact = true,
            "--out" => {
                i += 1;
                let path = args
                    .get(i)
                    .ok_or_else(|| "--out requires a path".to_string())?;
                opts.out = Some(path.clone());
            }
            "--top" => {
                i += 1;
                let n = args
                    .get(i)
                    .ok_or_else(|| "--top requires a number".to_string())?;
                opts.top = n
                    .parse::<usize>()
                    .map_err(|_| format!("--top: `{n}` is not a number"))?;
            }
            "-h" | "--help" => return Err(String::new()),
            flag if flag.starts_with('-') => {
                return Err(format!("unknown flag: {flag}"));
            }
            positional => {
                if opts.logfile.is_some() {
                    return Err(format!("unexpected extra argument: {positional}"));
                }
                opts.logfile = Some(positional.to_string());
            }
        }
        i += 1;
    }
    if opts.stdin == opts.logfile.is_some() {
        return Err("provide exactly one input: a <logfile> or --stdin".to_string());
    }
    Ok(opts)
}

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let opts = match parse_args(&args) {
        Ok(o) => o,
        Err(msg) => {
            if msg.is_empty() {
                print!("{USAGE}");
                return ExitCode::SUCCESS;
            }
            eprintln!("error: {msg}");
            eprint!("{USAGE}");
            return ExitCode::from(2);
        }
    };

    let mut analyzer = Analyzer::new(opts.no_redact);
    let feed_result = if opts.stdin {
        feed_reader(&mut analyzer, std::io::stdin().lock())
    } else {
        match opts.logfile.as_deref() {
            Some(path) => match std::fs::File::open(path) {
                Ok(f) => feed_reader(&mut analyzer, f),
                Err(e) => Err(format!("cannot open {path}: {e}")),
            },
            None => Err("no input".to_string()),
        }
    };
    if let Err(msg) = feed_result {
        eprintln!("error: {msg}");
        return ExitCode::FAILURE;
    }

    let report = analyzer.finish();
    let redact = !opts.no_redact;
    let rendered = if opts.json {
        match render_json(&report, redact) {
            Ok(s) => s,
            Err(e) => {
                eprintln!("error: failed to serialize report: {e}");
                return ExitCode::FAILURE;
            }
        }
    } else {
        render_markdown(&report, opts.top, redact)
    };

    match opts.out.as_deref() {
        Some(path) => {
            if let Err(e) = std::fs::write(path, rendered) {
                eprintln!("error: cannot write {path}: {e}");
                return ExitCode::FAILURE;
            }
        }
        None => {
            let mut stdout = std::io::stdout().lock();
            if stdout.write_all(rendered.as_bytes()).is_err() {
                return ExitCode::FAILURE;
            }
        }
    }
    ExitCode::SUCCESS
}
