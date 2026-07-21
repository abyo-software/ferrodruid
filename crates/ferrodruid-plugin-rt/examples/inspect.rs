// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)
//! Dump the import/export sections of a wasm module — useful when
//! debugging a plugin that fails to load against the runtime.

fn main() {
    let path = std::env::args()
        .nth(1)
        .expect("usage: inspect <path-to-wasm>");
    let bytes = std::fs::read(&path).expect("read");
    let engine = wasmtime::Engine::default();
    let module = wasmtime::Module::new(&engine, &bytes).expect("parse");
    println!("==> imports of {path}");
    for imp in module.imports() {
        println!("  {}::{}  ({:?})", imp.module(), imp.name(), imp.ty());
    }
    println!("==> exports");
    for exp in module.exports() {
        println!("  {} ({:?})", exp.name(), exp.ty());
    }
}
