// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! Minimal Druid-native-expression parser/evaluator for the `expression`
//! post-aggregator.
//!
//! This is a deliberately small, self-contained subset of Druid's native
//! expression language (per the public docs), covering exactly what the SQL
//! planner emits for function-over-aggregate projections:
//!
//! - identifiers referencing aggregator outputs: double-quoted
//!   (`"$avg_sum_0"`) or bare (`sum_x`, `[A-Za-z_$][A-Za-z0-9_$]*`)
//! - numeric literals (integer, decimal, optional exponent)
//! - binary `+ - * /`, unary `-`, parentheses, standard precedence
//! - functions: `round(x)`, `round(x, n)`, `abs(x)`, `floor(x)`, `ceil(x)`
//!
//! # Null / error semantics
//!
//! - A referenced field that is missing from the aggregator result map, or
//!   whose value is JSON `null` (or non-numeric), makes the whole expression
//!   evaluate to `None` — i.e. SQL `NULL` upstream, never a silent `0`.
//! - Arithmetic follows IEEE-754 (`x / 0.0` is `±inf`, `0.0 / 0.0` is NaN);
//!   any non-finite final result is reported as `None`.  **This deliberately
//!   differs from the `arithmetic` post-aggregator**, whose documented Druid
//!   behavior maps division by zero to `0`.  The `expression` post-aggregator
//!   mirrors Druid SQL semantics where `x / 0` is `NULL`, which is what the
//!   SQL planner relies on when lowering `ROUND(AVG(x), n)` and friends.
//! - Parse errors and unsupported constructs fail closed: evaluation returns
//!   `None`, and [`parse`] surfaces a descriptive error so query validation
//!   can reject the spec up front.  Nothing in this module panics.
//!
//! # Rounding semantics
//!
//! `round(x, n)` rounds half-up at `n` decimal places on the *shortest
//! round-trip decimal representation* of `x`, matching Druid's documented
//! `BigDecimal`-based behavior (e.g. `round(3.05, 1) == 3.1`, whereas naive
//! `(x * 10).round() / 10` would yield `3.0` because the binary value of
//! `3.05` is slightly below `3.05`).  Ties round away from zero
//! (`round(-2.5) == -3`).  Negative `n` rounds to the left of the decimal
//! point (`round(1250, -2) == 1300`).
//!
//! # Performance note
//!
//! The expression is re-parsed on every `evaluate()` call (single-shot
//! parse, no cache).  This is correctness-first; per-query parse caching is
//! a known perf follow-up if expression post-aggs show up hot in profiles.

use std::collections::HashMap;

/// Parsed expression AST node.
#[derive(Debug, Clone)]
enum Node {
    /// Numeric literal.
    Literal(f64),
    /// Reference to an aggregator output by name.
    Field(String),
    /// Unary negation.
    Neg(Box<Node>),
    /// Binary arithmetic operation.
    Binary(BinOp, Box<Node>, Box<Node>),
    /// Built-in function call.
    Call(Func, Vec<Node>),
}

/// Binary arithmetic operator.
#[derive(Debug, Clone, Copy)]
enum BinOp {
    /// Addition.
    Add,
    /// Subtraction.
    Sub,
    /// Multiplication.
    Mul,
    /// IEEE-754 division (non-finite results become `None` at the top level).
    Div,
}

/// Supported built-in function.
#[derive(Debug, Clone, Copy)]
enum Func {
    /// `round(x)` / `round(x, n)` — half-up decimal rounding.
    Round,
    /// `abs(x)`.
    Abs,
    /// `floor(x)`.
    Floor,
    /// `ceil(x)`.
    Ceil,
}

/// A successfully parsed post-aggregation expression.
#[derive(Debug, Clone)]
pub(crate) struct ParsedExpr {
    root: Node,
}

impl ParsedExpr {
    /// Evaluate against a map of aggregator results.
    ///
    /// Returns `None` on any missing/null/non-numeric field reference or a
    /// non-finite final result (see module docs for the full semantics).
    pub(crate) fn evaluate(&self, agg_results: &HashMap<String, serde_json::Value>) -> Option<f64> {
        eval(&self.root, agg_results).filter(|v| v.is_finite())
    }
}

/// Parse `input` into a [`ParsedExpr`].
///
/// # Errors
///
/// Returns a human-readable message describing the first syntax error or
/// unsupported construct (unknown function, wrong arity, trailing input, …).
pub(crate) fn parse(input: &str) -> Result<ParsedExpr, String> {
    // Anti-DoS caps (codex-review r2, 2026-07-11): the parser recurses on
    // nesting (parens, unary minus, function args), and expression strings
    // arrive verbatim in untrusted native query specs — without a cap, a
    // crafted `((((…))))` or `----…-1` overflows the stack and aborts the
    // process. The planner never emits anything near these limits.
    const MAX_EXPR_LEN: usize = 8 * 1024;
    if input.len() > MAX_EXPR_LEN {
        return Err(format!(
            "expression longer than {MAX_EXPR_LEN} bytes rejected"
        ));
    }
    let mut p = Parser {
        s: input,
        pos: 0,
        depth: 0,
    };
    let root = p.parse_additive()?;
    p.skip_ws();
    if p.pos != p.s.len() {
        return Err(format!(
            "unexpected trailing input at byte {} in expression",
            p.pos
        ));
    }
    Ok(ParsedExpr { root })
}

/// Maximum recursion depth for the expression parser (see the anti-DoS note
/// in [`parse`]). Generous for any real post-aggregation expression.
const MAX_PARSE_DEPTH: usize = 128;

/// Recursive-descent parser state over the raw expression string.
struct Parser<'a> {
    s: &'a str,
    pos: usize,
    /// Current recursion depth (bounded by [`MAX_PARSE_DEPTH`]).
    depth: usize,
}

impl Parser<'_> {
    /// Bump the recursion depth, failing closed past [`MAX_PARSE_DEPTH`].
    fn enter(&mut self) -> Result<(), String> {
        self.depth += 1;
        if self.depth > MAX_PARSE_DEPTH {
            return Err(format!(
                "expression nesting deeper than {MAX_PARSE_DEPTH} rejected"
            ));
        }
        Ok(())
    }

    fn leave(&mut self) {
        self.depth -= 1;
    }
    fn skip_ws(&mut self) {
        while let Some(c) = self.peek() {
            if c.is_ascii_whitespace() {
                self.pos += 1;
            } else {
                break;
            }
        }
    }

    fn peek(&self) -> Option<char> {
        self.s[self.pos..].chars().next()
    }

    /// Consume `c` if it is the next non-whitespace char.
    fn eat(&mut self, c: char) -> bool {
        self.skip_ws();
        if self.peek() == Some(c) {
            self.pos += c.len_utf8();
            true
        } else {
            false
        }
    }

    /// `additive := multiplicative (('+'|'-') multiplicative)*`
    ///
    /// Depth-guarded: every nesting construct (parens, function args)
    /// re-enters through here, so this single guard bounds paren nesting.
    fn parse_additive(&mut self) -> Result<Node, String> {
        self.enter()?;
        let result = self.parse_additive_inner();
        self.leave();
        result
    }

    fn parse_additive_inner(&mut self) -> Result<Node, String> {
        let mut lhs = self.parse_multiplicative()?;
        loop {
            self.skip_ws();
            let op = match self.peek() {
                Some('+') => BinOp::Add,
                Some('-') => BinOp::Sub,
                _ => return Ok(lhs),
            };
            self.pos += 1;
            let rhs = self.parse_multiplicative()?;
            lhs = Node::Binary(op, Box::new(lhs), Box::new(rhs));
        }
    }

    /// `multiplicative := unary (('*'|'/') unary)*`
    fn parse_multiplicative(&mut self) -> Result<Node, String> {
        let mut lhs = self.parse_unary()?;
        loop {
            self.skip_ws();
            let op = match self.peek() {
                Some('*') => BinOp::Mul,
                Some('/') => BinOp::Div,
                _ => return Ok(lhs),
            };
            self.pos += 1;
            let rhs = self.parse_unary()?;
            lhs = Node::Binary(op, Box::new(lhs), Box::new(rhs));
        }
    }

    /// `unary := '-' unary | primary`
    ///
    /// Depth-guarded: self-recursion on `-` would otherwise let a long
    /// `----…-1` run overflow the stack (parens are guarded in
    /// `parse_additive`; unary minus is the other unbounded recursion).
    fn parse_unary(&mut self) -> Result<Node, String> {
        self.enter()?;
        let result = if self.eat('-') {
            self.parse_unary().map(|inner| Node::Neg(Box::new(inner)))
        } else {
            self.parse_primary()
        };
        self.leave();
        result
    }

    /// `primary := number | '"' ident '"' | bare_ident | func '(' args ')'
    ///           | '(' expr ')'`
    fn parse_primary(&mut self) -> Result<Node, String> {
        self.skip_ws();
        match self.peek() {
            Some('(') => {
                self.pos += 1;
                let inner = self.parse_additive()?;
                if !self.eat(')') {
                    return Err("expected ')'".to_string());
                }
                Ok(inner)
            }
            Some('"') => self.parse_quoted_identifier(),
            Some(c) if c.is_ascii_digit() => self.parse_number(),
            Some(c) if c.is_ascii_alphabetic() || c == '_' || c == '$' => {
                self.parse_bare_identifier_or_call()
            }
            Some(c) => Err(format!("unexpected character '{c}' at byte {}", self.pos)),
            None => Err("unexpected end of expression".to_string()),
        }
    }

    /// Double-quoted identifier: `"name"` (no escape support; fail-closed on
    /// unterminated quotes or embedded quotes).
    fn parse_quoted_identifier(&mut self) -> Result<Node, String> {
        // self.peek() == Some('"') guaranteed by caller.
        self.pos += 1;
        let start = self.pos;
        let Some(rel) = self.s[start..].find('"') else {
            return Err("unterminated quoted identifier".to_string());
        };
        let name = &self.s[start..start + rel];
        self.pos = start + rel + 1;
        if name.is_empty() {
            return Err("empty quoted identifier".to_string());
        }
        Ok(Node::Field(name.to_string()))
    }

    /// Numeric literal: `digits ('.' digits)? ([eE] [+-]? digits)?`.
    fn parse_number(&mut self) -> Result<Node, String> {
        let start = self.pos;
        self.consume_digits();
        if self.peek() == Some('.') {
            self.pos += 1;
            let frac_start = self.pos;
            self.consume_digits();
            if self.pos == frac_start {
                return Err("expected digits after decimal point".to_string());
            }
        }
        if matches!(self.peek(), Some('e' | 'E')) {
            let exp_mark = self.pos;
            self.pos += 1;
            if matches!(self.peek(), Some('+' | '-')) {
                self.pos += 1;
            }
            let exp_start = self.pos;
            self.consume_digits();
            if self.pos == exp_start {
                // Not an exponent after all (e.g. `2e` where `e` might be
                // intended as something else) — fail closed.
                self.pos = exp_mark;
                return Err("expected digits in exponent".to_string());
            }
        }
        let text = &self.s[start..self.pos];
        text.parse::<f64>()
            .map(Node::Literal)
            .map_err(|e| format!("invalid numeric literal '{text}': {e}"))
    }

    fn consume_digits(&mut self) {
        while matches!(self.peek(), Some(c) if c.is_ascii_digit()) {
            self.pos += 1;
        }
    }

    /// Bare identifier (`[A-Za-z_$][A-Za-z0-9_$]*`); if immediately followed
    /// by `(` it is treated as a function call, which must be one of the
    /// supported built-ins.
    fn parse_bare_identifier_or_call(&mut self) -> Result<Node, String> {
        let start = self.pos;
        while matches!(self.peek(), Some(c) if c.is_ascii_alphanumeric() || c == '_' || c == '$') {
            self.pos += 1;
        }
        let name = &self.s[start..self.pos];
        self.skip_ws();
        if self.peek() != Some('(') {
            return Ok(Node::Field(name.to_string()));
        }
        // Function call.
        self.pos += 1;
        let func = match name {
            "round" => Func::Round,
            "abs" => Func::Abs,
            "floor" => Func::Floor,
            "ceil" => Func::Ceil,
            other => return Err(format!("unsupported function '{other}'")),
        };
        let mut args = vec![self.parse_additive()?];
        while self.eat(',') {
            args.push(self.parse_additive()?);
        }
        if !self.eat(')') {
            return Err(format!("expected ')' to close call to '{name}'"));
        }
        let arity_ok = match func {
            Func::Round => args.len() == 1 || args.len() == 2,
            Func::Abs | Func::Floor | Func::Ceil => args.len() == 1,
        };
        if !arity_ok {
            return Err(format!(
                "function '{name}' called with {} argument(s)",
                args.len()
            ));
        }
        Ok(Node::Call(func, args))
    }
}

/// Evaluate a node; `None` propagates missing/null field references.
fn eval(node: &Node, agg_results: &HashMap<String, serde_json::Value>) -> Option<f64> {
    match node {
        Node::Literal(v) => Some(*v),
        Node::Field(name) => match agg_results.get(name)? {
            serde_json::Value::Number(n) => n.as_f64(),
            _ => None,
        },
        Node::Neg(inner) => eval(inner, agg_results).map(|v| -v),
        Node::Binary(op, lhs, rhs) => {
            let a = eval(lhs, agg_results)?;
            let b = eval(rhs, agg_results)?;
            Some(match op {
                BinOp::Add => a + b,
                BinOp::Sub => a - b,
                BinOp::Mul => a * b,
                BinOp::Div => a / b,
            })
        }
        Node::Call(func, args) => {
            let x = eval(args.first()?, agg_results)?;
            match func {
                Func::Abs => Some(x.abs()),
                Func::Floor => Some(x.floor()),
                Func::Ceil => Some(x.ceil()),
                Func::Round => {
                    let digits = match args.get(1) {
                        Some(d) => {
                            let d = eval(d, agg_results)?;
                            if !d.is_finite() {
                                return None;
                            }
                            // f64 decimal range is within ±10^309; clamping
                            // keeps the cast well-defined without changing
                            // results.
                            #[allow(clippy::cast_possible_truncation)]
                            let clamped = d.trunc().clamp(-400.0, 400.0) as i32;
                            clamped
                        }
                        None => 0,
                    };
                    round_half_up(x, digits)
                }
            }
        }
    }
}

/// Round `x` half-up at `digits` decimal places using its shortest
/// round-trip decimal representation (Druid `BigDecimal.valueOf(x)
/// .setScale(digits, HALF_UP)` semantics — see module docs).
fn round_half_up(x: f64, digits: i32) -> Option<f64> {
    if !x.is_finite() {
        return None;
    }
    // Rust's `Display` for f64 prints the shortest decimal string that
    // round-trips, always in plain (non-scientific) notation.
    let repr = format!("{x}");
    let (neg, unsigned) = match repr.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, repr.as_str()),
    };
    let (int_part, frac_part) = match unsigned.split_once('.') {
        Some((i, f)) => (i, f),
        None => (unsigned, ""),
    };
    let mut ds: Vec<u8> = int_part
        .bytes()
        .chain(frac_part.bytes())
        .map(|b| b.wrapping_sub(b'0'))
        .collect();
    if ds.iter().any(|&d| d > 9) {
        // Defensive: non-digit in the Display output (cannot happen for
        // finite f64, but fail closed rather than corrupt).
        return None;
    }
    // Number of digits to the left of the decimal point.
    let point = int_part.len() as i64;
    // Keep digits in `ds[..cut]`; `ds[cut]` decides the half-up carry.
    let cut = point.saturating_add(i64::from(digits));
    if cut >= ds.len() as i64 {
        return Some(x); // Requested precision is at or beyond available digits.
    }
    if cut < 0 {
        return Some(0.0); // All significant digits rounded away.
    }
    #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
    let cut = cut as usize;
    let carry_in = ds[cut] >= 5;
    for d in &mut ds[cut..] {
        *d = 0;
    }
    let mut point = point;
    if carry_in {
        let mut i = cut;
        let mut carry = true;
        while carry && i > 0 {
            i -= 1;
            if ds[i] == 9 {
                ds[i] = 0;
            } else {
                ds[i] += 1;
                carry = false;
            }
        }
        if carry {
            ds.insert(0, 1);
            point += 1;
        }
    }
    reconstruct(neg, &ds, point)
}

/// Rebuild an f64 from a digit vector and decimal-point position.
fn reconstruct(neg: bool, ds: &[u8], point: i64) -> Option<f64> {
    let mut out = String::with_capacity(ds.len() + 8);
    if neg {
        out.push('-');
    }
    if point <= 0 {
        out.push_str("0.");
        for _ in 0..(-point) {
            out.push('0');
        }
        for &d in ds {
            out.push(char::from(b'0' + d));
        }
    } else if point as usize >= ds.len() {
        for &d in ds {
            out.push(char::from(b'0' + d));
        }
        for _ in 0..(point as usize - ds.len()) {
            out.push('0');
        }
    } else {
        #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
        let point = point as usize;
        for (i, &d) in ds.iter().enumerate() {
            if i == point {
                out.push('.');
            }
            out.push(char::from(b'0' + d));
        }
    }
    let v = out.parse::<f64>().ok()?;
    // Normalize -0.0 to 0.0 (decimal rounding has no signed zero).
    Some(if v == 0.0 { 0.0 } else { v })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn ctx(pairs: &[(&str, serde_json::Value)]) -> HashMap<String, serde_json::Value> {
        pairs
            .iter()
            .map(|(k, v)| ((*k).to_string(), v.clone()))
            .collect()
    }

    fn eval_str(expr: &str, pairs: &[(&str, serde_json::Value)]) -> Option<f64> {
        parse(expr).ok()?.evaluate(&ctx(pairs))
    }

    #[test]
    fn literals_and_precedence() {
        assert_eq!(eval_str("1 + 2 * 3", &[]), Some(7.0));
        assert_eq!(eval_str("(1 + 2) * 3", &[]), Some(9.0));
        assert_eq!(eval_str("10 - 4 - 3", &[]), Some(3.0)); // left-assoc
        assert_eq!(eval_str("100 / 10 / 2", &[]), Some(5.0)); // left-assoc
        assert_eq!(eval_str("2.5 * 4", &[]), Some(10.0));
        assert_eq!(eval_str("1e2 + 1", &[]), Some(101.0));
        assert_eq!(eval_str("-3 + 5", &[]), Some(2.0));
        assert_eq!(eval_str("--4", &[]), Some(4.0));
        assert_eq!(eval_str("2 * -3", &[]), Some(-6.0));
    }

    #[test]
    fn identifiers_quoted_and_bare() {
        let vars = [("$avg_sum_0", json!(12.0)), ("cnt", json!(4))];
        assert_eq!(eval_str(r#""$avg_sum_0" / cnt"#, &vars), Some(3.0));
        assert_eq!(eval_str("cnt + 1", &vars), Some(5.0));
        assert_eq!(eval_str(r#""cnt" * 2"#, &vars), Some(8.0));
    }

    #[test]
    fn null_propagation() {
        let vars = [("a", json!(1.0)), ("b", json!(null))];
        assert_eq!(eval_str("a + b", &vars), None); // null field
        assert_eq!(eval_str("a + missing", &vars), None); // absent field
        assert_eq!(eval_str("round(b, 2)", &vars), None);
        assert_eq!(eval_str("a + 1", &vars), Some(2.0));
    }

    #[test]
    fn division_by_zero_is_none() {
        assert_eq!(eval_str("1 / 0", &[]), None); // +inf -> None
        assert_eq!(eval_str("-1 / 0", &[]), None); // -inf -> None
        assert_eq!(eval_str("0 / 0", &[]), None); // NaN -> None
        assert_eq!(eval_str("1 / 0 - 1 / 0", &[]), None); // NaN -> None
    }

    #[test]
    fn functions() {
        assert_eq!(eval_str("abs(-4.5)", &[]), Some(4.5));
        assert_eq!(eval_str("floor(2.9)", &[]), Some(2.0));
        assert_eq!(eval_str("floor(-2.1)", &[]), Some(-3.0));
        assert_eq!(eval_str("ceil(2.1)", &[]), Some(3.0));
        assert_eq!(eval_str("ceil(-2.9)", &[]), Some(-2.0));
        assert_eq!(eval_str("round(2.4)", &[]), Some(2.0));
        assert_eq!(eval_str("round(2.5)", &[]), Some(3.0)); // half-up
        assert_eq!(eval_str("round(-2.5)", &[]), Some(-3.0)); // away from zero
        assert_eq!(eval_str("round(2.34567, 2)", &[]), Some(2.35));
        assert_eq!(eval_str("round(2.34567, 4)", &[]), Some(2.3457));
    }

    #[test]
    fn round_decimal_half_up_matches_bigdecimal_semantics() {
        // The binary double closest to 3.05 is 3.049999...; BigDecimal
        // rounding on the shortest decimal repr still yields 3.1.
        assert_eq!(eval_str("round(3.05, 1)", &[]), Some(3.1));
        assert_eq!(eval_str("round(2.675, 2)", &[]), Some(2.68));
        assert_eq!(eval_str("round(1.005, 2)", &[]), Some(1.01));
        // Negative digit counts round left of the decimal point.
        assert_eq!(eval_str("round(1250, -2)", &[]), Some(1300.0));
        assert_eq!(eval_str("round(1249, -2)", &[]), Some(1200.0));
        assert_eq!(eval_str("round(50, -3)", &[]), Some(0.0));
        // Precision beyond available digits is identity.
        assert_eq!(eval_str("round(3.5, 10)", &[]), Some(3.5));
        // Carry across all digits (9.99 -> 10.0).
        assert_eq!(eval_str("round(9.99, 1)", &[]), Some(10.0));
        assert_eq!(eval_str("round(0.95, 1)", &[]), Some(1.0));
        // Negative values round away from zero on ties.
        assert_eq!(eval_str("round(-3.05, 1)", &[]), Some(-3.1));
        // -0.04 rounded to 1 place is 0, not -0.
        let v = eval_str("round(-0.04, 1)", &[]).expect("value");
        assert_eq!(v, 0.0);
        assert!(v.is_sign_positive(), "-0.0 must be normalized to 0.0");
    }

    #[test]
    fn round_over_field_ratio() {
        let vars = [("s", json!(22.0)), ("c", json!(7))];
        // 22/7 = 3.142857... -> 3.1
        assert_eq!(eval_str(r#"round("s" / "c", 1)"#, &vars), Some(3.1));
    }

    #[test]
    fn parse_errors_fail_closed() {
        for bad in [
            "",
            "1 +",
            "(1 + 2",
            "foo(1)",       // unknown function
            "round()",      // arity
            "round(1,2,3)", // arity
            "abs(1, 2)",    // arity
            "\"unterminated",
            "\"\"",  // empty quoted identifier
            "1 & 2", // unsupported operator
            "2 ..",
            "1.  ",
        ] {
            assert!(parse(bad).is_err(), "expected parse failure for {bad:?}");
        }
    }

    #[test]
    fn trailing_garbage_rejected() {
        assert!(parse("1 + 2 x").is_err());
        assert!(parse("(1) )").is_err());
    }
}

#[cfg(test)]
mod dos_tests {
    use super::*;

    /// codex-review r2 High (2026-07-11): a deeply nested expression from an
    /// untrusted native query spec must be rejected by the parser, not
    /// overflow the stack (process-aborting DoS).
    #[test]
    fn deeply_nested_expression_rejected_not_stack_overflow() {
        let deep_parens = format!("{}1{}", "(".repeat(100_000), ")".repeat(100_000));
        assert!(parse(&deep_parens).is_err(), "deep parens must be rejected");

        let deep_unary = format!("{}1", "-".repeat(100_000));
        assert!(parse(&deep_unary).is_err(), "deep unary must be rejected");

        // Sanity: reasonable nesting still parses.
        let ok = format!("{}1{}", "(".repeat(20), ")".repeat(20));
        assert!(parse(&ok).is_ok(), "shallow nesting must still parse");
    }
}
