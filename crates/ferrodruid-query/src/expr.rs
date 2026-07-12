// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! Value-producing expression evaluator for virtual columns.
//!
//! This is a clean-room, self-contained mini-language evaluator that computes a
//! single value (number / string / null / boolean) from an input row.  It is
//! distinct from the boolean-only expression filter in [`crate::filter`]: that
//! one answers "does this row match?", whereas this one answers "what is the
//! value of the derived column for this row?".
//!
//! ## Supported grammar
//!
//! ```text
//! expr      := or
//! or        := and  ("||" and)*
//! and       := cmp  ("&&" cmp)*
//! cmp       := add  (("=="|"!="|">"|"<"|">="|"<=") add)?
//! add       := mul  (("+"|"-") mul)*
//! mul       := unary (("*"|"/"|"%") unary)*
//! unary     := ("-"|"!") unary | primary
//! primary   := number | string | ident
//!            | ident "(" args ")"            -- function call
//!            | "(" expr ")"
//! args      := (expr ("," expr)*)?
//! ```
//!
//! ## Supported functions
//!
//! * `strlen(s)` — character length of a string.
//! * `substring(s, idx)` / `substring(s, idx, len)` — character substring
//!   (out-of-range indices clamp; `idx` is 0-based).
//! * `concat(a, b, ...)` — string concatenation (nulls treated as empty).
//! * `lower(s)` / `upper(s)` — ASCII-and-Unicode case folding.
//! * `abs(x)` — absolute value.
//! * `coalesce(a, b, ...)` — first non-null argument.
//! * `case(cond, then, cond2, then2, ..., else)` — searched-CASE: evaluate each
//!   `cond`; the first truthy one returns its paired `then`; the trailing odd
//!   argument is the `else` (default null when absent).
//!
//! Anything outside this grammar (unknown function, unbalanced parens, bad
//! token) surfaces as an [`ExprError`] at *compile* time so a virtual column
//! with a malformed expression fails the query cleanly rather than silently
//! evaluating to null per-row.

use std::collections::HashMap;

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

/// An error produced while compiling or evaluating an expression.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExprError(pub String);

impl std::fmt::Display for ExprError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::error::Error for ExprError {}

// ---------------------------------------------------------------------------
// Value
// ---------------------------------------------------------------------------

/// A runtime value produced by expression evaluation.
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    /// A floating-point or integral number.
    Num(f64),
    /// A string.
    Str(String),
    /// A boolean.
    Bool(bool),
    /// A null / missing value.
    Null,
}

impl Value {
    /// Convert a JSON value into an expression [`Value`].
    pub fn from_json(v: &serde_json::Value) -> Value {
        match v {
            serde_json::Value::Null => Value::Null,
            serde_json::Value::Bool(b) => Value::Bool(*b),
            serde_json::Value::Number(n) => n.as_f64().map_or(Value::Null, Value::Num),
            serde_json::Value::String(s) => Value::Str(s.clone()),
            // Arrays / objects are not addressable by this mini-language.
            other => Value::Str(other.to_string()),
        }
    }

    /// Convert this value into a JSON value for inclusion in a row map.
    ///
    /// Numbers that are integral (and fit in an `i64`) serialize as JSON
    /// integers; everything else serializes as the natural JSON type.
    pub fn to_json(&self) -> serde_json::Value {
        match self {
            Value::Null => serde_json::Value::Null,
            Value::Bool(b) => serde_json::Value::Bool(*b),
            Value::Str(s) => serde_json::Value::String(s.clone()),
            Value::Num(n) => {
                if n.is_finite()
                    && n.fract() == 0.0
                    && *n >= i64::MIN as f64
                    && *n <= i64::MAX as f64
                {
                    serde_json::Value::Number(serde_json::Number::from(*n as i64))
                } else {
                    serde_json::Number::from_f64(*n)
                        .map_or(serde_json::Value::Null, serde_json::Value::Number)
                }
            }
        }
    }

    /// Coerce this value to `f64` if possible (parsing string numbers, treating
    /// booleans as 0/1).  Returns `None` for null or non-numeric strings.
    fn as_f64(&self) -> Option<f64> {
        match self {
            Value::Num(n) => Some(*n),
            Value::Bool(b) => Some(if *b { 1.0 } else { 0.0 }),
            Value::Str(s) => s.parse().ok(),
            Value::Null => None,
        }
    }

    /// Coerce this value to a string (null becomes the empty string).
    fn as_str(&self) -> String {
        match self {
            Value::Str(s) => s.clone(),
            Value::Num(n) => Value::Num(*n)
                .to_json()
                .to_string()
                .trim_matches('"')
                .to_string(),
            Value::Bool(b) => b.to_string(),
            Value::Null => String::new(),
        }
    }

    /// Truthiness (used by `&&`, `||`, `!`, and `case`).
    fn is_truthy(&self) -> bool {
        match self {
            Value::Bool(b) => *b,
            Value::Num(n) => *n != 0.0,
            Value::Str(s) => !s.is_empty(),
            Value::Null => false,
        }
    }
}

// ---------------------------------------------------------------------------
// Tokens
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq)]
enum Token {
    Num(f64),
    Str(String),
    Ident(String),
    Op(String),
    LParen,
    RParen,
    Comma,
}

fn tokenize(input: &str) -> Result<Vec<Token>, ExprError> {
    let chars: Vec<char> = input.chars().collect();
    let mut tokens = Vec::new();
    let mut i = 0;
    while i < chars.len() {
        let c = chars[i];
        if c.is_ascii_whitespace() {
            i += 1;
            continue;
        }
        match c {
            '(' => {
                tokens.push(Token::LParen);
                i += 1;
            }
            ')' => {
                tokens.push(Token::RParen);
                i += 1;
            }
            ',' => {
                tokens.push(Token::Comma);
                i += 1;
            }
            '\'' | '"' => {
                let quote = c;
                i += 1;
                let mut s = String::new();
                let mut closed = false;
                while i < chars.len() {
                    if chars[i] == '\\' && i + 1 < chars.len() {
                        // Minimal escape handling for quote / backslash.
                        s.push(chars[i + 1]);
                        i += 2;
                        continue;
                    }
                    if chars[i] == quote {
                        closed = true;
                        i += 1;
                        break;
                    }
                    s.push(chars[i]);
                    i += 1;
                }
                if !closed {
                    return Err(ExprError(format!("unterminated string literal: {input}")));
                }
                tokens.push(Token::Str(s));
            }
            '+' | '-' | '*' | '/' | '%' => {
                tokens.push(Token::Op(c.to_string()));
                i += 1;
            }
            '=' | '!' | '<' | '>' | '&' | '|' => {
                if i + 1 < chars.len() {
                    let two: String = chars[i..=i + 1].iter().collect();
                    if matches!(two.as_str(), "==" | "!=" | ">=" | "<=" | "&&" | "||") {
                        tokens.push(Token::Op(two));
                        i += 2;
                        continue;
                    }
                }
                if matches!(c, '<' | '>' | '!') {
                    tokens.push(Token::Op(c.to_string()));
                    i += 1;
                } else {
                    return Err(ExprError(format!(
                        "unexpected operator char '{c}' in: {input}"
                    )));
                }
            }
            _ if c.is_ascii_digit()
                || (c == '.' && i + 1 < chars.len() && chars[i + 1].is_ascii_digit()) =>
            {
                let start = i;
                while i < chars.len() && (chars[i].is_ascii_digit() || chars[i] == '.') {
                    i += 1;
                }
                let num: String = chars[start..i].iter().collect();
                let n = num
                    .parse::<f64>()
                    .map_err(|_| ExprError(format!("bad numeric literal '{num}'")))?;
                tokens.push(Token::Num(n));
            }
            _ if c.is_alphabetic() || c == '_' => {
                let start = i;
                while i < chars.len() && (chars[i].is_alphanumeric() || chars[i] == '_') {
                    i += 1;
                }
                let ident: String = chars[start..i].iter().collect();
                tokens.push(Token::Ident(ident));
            }
            _ => return Err(ExprError(format!("unexpected character '{c}' in: {input}"))),
        }
    }
    Ok(tokens)
}

// ---------------------------------------------------------------------------
// AST
// ---------------------------------------------------------------------------

/// A compiled expression AST node.
#[derive(Debug, Clone)]
pub enum Expr {
    /// A literal value.
    Literal(Value),
    /// A column reference (resolved against the input row).
    Column(String),
    /// A unary operation (`-`, `!`).
    Unary {
        /// The operator string.
        op: String,
        /// The operand.
        operand: Box<Expr>,
    },
    /// A binary operation.
    Binary {
        /// The operator string.
        op: String,
        /// Left operand.
        lhs: Box<Expr>,
        /// Right operand.
        rhs: Box<Expr>,
    },
    /// A function call.
    Call {
        /// Function name (lower-cased).
        name: String,
        /// Argument expressions.
        args: Vec<Expr>,
    },
}

// ---------------------------------------------------------------------------
// Parser
// ---------------------------------------------------------------------------

struct Parser {
    tokens: Vec<Token>,
    pos: usize,
}

impl Parser {
    fn peek(&self) -> Option<&Token> {
        self.tokens.get(self.pos)
    }

    fn next(&mut self) -> Option<Token> {
        let t = self.tokens.get(self.pos).cloned();
        if t.is_some() {
            self.pos += 1;
        }
        t
    }

    fn eat_op(&mut self, ops: &[&str]) -> Option<String> {
        if let Some(Token::Op(op)) = self.peek()
            && ops.contains(&op.as_str())
        {
            let op = op.clone();
            self.pos += 1;
            return Some(op);
        }
        None
    }

    fn parse_or(&mut self) -> Result<Expr, ExprError> {
        let mut lhs = self.parse_and()?;
        while self.eat_op(&["||"]).is_some() {
            let rhs = self.parse_and()?;
            lhs = Expr::Binary {
                op: "||".into(),
                lhs: Box::new(lhs),
                rhs: Box::new(rhs),
            };
        }
        Ok(lhs)
    }

    fn parse_and(&mut self) -> Result<Expr, ExprError> {
        let mut lhs = self.parse_cmp()?;
        while self.eat_op(&["&&"]).is_some() {
            let rhs = self.parse_cmp()?;
            lhs = Expr::Binary {
                op: "&&".into(),
                lhs: Box::new(lhs),
                rhs: Box::new(rhs),
            };
        }
        Ok(lhs)
    }

    fn parse_cmp(&mut self) -> Result<Expr, ExprError> {
        let lhs = self.parse_add()?;
        if let Some(op) = self.eat_op(&["==", "!=", ">", "<", ">=", "<="]) {
            let rhs = self.parse_add()?;
            return Ok(Expr::Binary {
                op,
                lhs: Box::new(lhs),
                rhs: Box::new(rhs),
            });
        }
        Ok(lhs)
    }

    fn parse_add(&mut self) -> Result<Expr, ExprError> {
        let mut lhs = self.parse_mul()?;
        while let Some(op) = self.eat_op(&["+", "-"]) {
            let rhs = self.parse_mul()?;
            lhs = Expr::Binary {
                op,
                lhs: Box::new(lhs),
                rhs: Box::new(rhs),
            };
        }
        Ok(lhs)
    }

    fn parse_mul(&mut self) -> Result<Expr, ExprError> {
        let mut lhs = self.parse_unary()?;
        while let Some(op) = self.eat_op(&["*", "/", "%"]) {
            let rhs = self.parse_unary()?;
            lhs = Expr::Binary {
                op,
                lhs: Box::new(lhs),
                rhs: Box::new(rhs),
            };
        }
        Ok(lhs)
    }

    fn parse_unary(&mut self) -> Result<Expr, ExprError> {
        if let Some(op) = self.eat_op(&["-", "!"]) {
            let operand = self.parse_unary()?;
            return Ok(Expr::Unary {
                op,
                operand: Box::new(operand),
            });
        }
        self.parse_primary()
    }

    fn parse_primary(&mut self) -> Result<Expr, ExprError> {
        let tok = self
            .next()
            .ok_or_else(|| ExprError("unexpected end of expression".into()))?;
        match tok {
            Token::Num(n) => Ok(Expr::Literal(Value::Num(n))),
            Token::Str(s) => Ok(Expr::Literal(Value::Str(s))),
            Token::LParen => {
                let e = self.parse_or()?;
                match self.next() {
                    Some(Token::RParen) => Ok(e),
                    _ => Err(ExprError("missing closing parenthesis".into())),
                }
            }
            Token::Ident(ident) => {
                // Keyword literals.
                match ident.as_str() {
                    "null" => return Ok(Expr::Literal(Value::Null)),
                    "true" => return Ok(Expr::Literal(Value::Bool(true))),
                    "false" => return Ok(Expr::Literal(Value::Bool(false))),
                    _ => {}
                }
                // Function call?
                if matches!(self.peek(), Some(Token::LParen)) {
                    self.pos += 1; // consume '('
                    let mut args = Vec::new();
                    if !matches!(self.peek(), Some(Token::RParen)) {
                        loop {
                            args.push(self.parse_or()?);
                            match self.peek() {
                                Some(Token::Comma) => {
                                    self.pos += 1;
                                }
                                _ => break,
                            }
                        }
                    }
                    match self.next() {
                        Some(Token::RParen) => {}
                        _ => return Err(ExprError("missing ')' in function call".into())),
                    }
                    return Ok(Expr::Call {
                        name: ident.to_lowercase(),
                        args,
                    });
                }
                Ok(Expr::Column(ident))
            }
            other => Err(ExprError(format!("unexpected token {other:?}"))),
        }
    }
}

// ---------------------------------------------------------------------------
// Compile entry point
// ---------------------------------------------------------------------------

impl Expr {
    /// Compile an expression string into an [`Expr`] AST, validating syntax and
    /// known function names.  Malformed input returns an [`ExprError`].
    pub fn compile(input: &str) -> Result<Expr, ExprError> {
        let tokens = tokenize(input)?;
        if tokens.is_empty() {
            return Err(ExprError("empty expression".into()));
        }
        let mut parser = Parser { tokens, pos: 0 };
        let expr = parser.parse_or()?;
        if parser.pos != parser.tokens.len() {
            return Err(ExprError(format!(
                "trailing tokens after expression: {input}"
            )));
        }
        expr.validate_functions()?;
        Ok(expr)
    }

    /// Walk the AST and reject any unknown function names so a misspelled
    /// function fails at compile time rather than evaluating to null.
    fn validate_functions(&self) -> Result<(), ExprError> {
        match self {
            Expr::Literal(_) | Expr::Column(_) => Ok(()),
            Expr::Unary { operand, .. } => operand.validate_functions(),
            Expr::Binary { lhs, rhs, .. } => {
                lhs.validate_functions()?;
                rhs.validate_functions()
            }
            Expr::Call { name, args } => {
                if !matches!(
                    name.as_str(),
                    "strlen"
                        | "substring"
                        | "concat"
                        | "lower"
                        | "upper"
                        | "abs"
                        | "coalesce"
                        | "case"
                ) {
                    return Err(ExprError(format!("unknown function '{name}'")));
                }
                for a in args {
                    a.validate_functions()?;
                }
                Ok(())
            }
        }
    }

    /// Evaluate this expression against a row (column-name → JSON value).
    pub fn eval(&self, row: &HashMap<String, serde_json::Value>) -> Value {
        match self {
            Expr::Literal(v) => v.clone(),
            Expr::Column(name) => row.get(name).map_or(Value::Null, Value::from_json),
            Expr::Unary { op, operand } => {
                let v = operand.eval(row);
                match op.as_str() {
                    "-" => v.as_f64().map_or(Value::Null, |n| Value::Num(-n)),
                    "!" => Value::Bool(!v.is_truthy()),
                    _ => Value::Null,
                }
            }
            Expr::Binary { op, lhs, rhs } => eval_binary(op, lhs, rhs, row),
            Expr::Call { name, args } => eval_call(name, args, row),
        }
    }
}

fn eval_binary(
    op: &str,
    lhs: &Expr,
    rhs: &Expr,
    row: &HashMap<String, serde_json::Value>,
) -> Value {
    // Short-circuit logical operators.
    match op {
        "&&" => {
            let l = lhs.eval(row);
            if !l.is_truthy() {
                return Value::Bool(false);
            }
            return Value::Bool(rhs.eval(row).is_truthy());
        }
        "||" => {
            let l = lhs.eval(row);
            if l.is_truthy() {
                return Value::Bool(true);
            }
            return Value::Bool(rhs.eval(row).is_truthy());
        }
        _ => {}
    }

    let l = lhs.eval(row);
    let r = rhs.eval(row);

    match op {
        "+" => {
            // Null propagates (matches arithmetic semantics).  If both sides
            // coerce to numbers, add numerically; otherwise string-concatenate
            // (a permissive `+` overload for non-null operands).
            if matches!(l, Value::Null) || matches!(r, Value::Null) {
                return Value::Null;
            }
            match (l.as_f64(), r.as_f64()) {
                (Some(a), Some(b)) => Value::Num(a + b),
                _ => Value::Str(format!("{}{}", l.as_str(), r.as_str())),
            }
        }
        "-" | "*" | "/" | "%" => {
            let (a, b) = match (l.as_f64(), r.as_f64()) {
                (Some(a), Some(b)) => (a, b),
                _ => return Value::Null,
            };
            let out = match op {
                "-" => a - b,
                "*" => a * b,
                "/" => {
                    if b == 0.0 {
                        return Value::Null;
                    }
                    a / b
                }
                "%" => {
                    if b == 0.0 {
                        return Value::Null;
                    }
                    a % b
                }
                _ => return Value::Null,
            };
            Value::Num(out)
        }
        "==" | "!=" | ">" | "<" | ">=" | "<=" => Value::Bool(compare(&l, op, &r)),
        _ => Value::Null,
    }
}

fn compare(l: &Value, op: &str, r: &Value) -> bool {
    if matches!(l, Value::Null) || matches!(r, Value::Null) {
        return match op {
            "==" => matches!(l, Value::Null) && matches!(r, Value::Null),
            "!=" => !(matches!(l, Value::Null) && matches!(r, Value::Null)),
            _ => false,
        };
    }
    if let (Some(a), Some(b)) = (l.as_f64(), r.as_f64()) {
        return match op {
            "==" => (a - b).abs() < f64::EPSILON,
            "!=" => (a - b).abs() >= f64::EPSILON,
            ">" => a > b,
            "<" => a < b,
            ">=" => a >= b,
            "<=" => a <= b,
            _ => false,
        };
    }
    let (a, b) = (l.as_str(), r.as_str());
    match op {
        "==" => a == b,
        "!=" => a != b,
        ">" => a > b,
        "<" => a < b,
        ">=" => a >= b,
        "<=" => a <= b,
        _ => false,
    }
}

fn eval_call(name: &str, args: &[Expr], row: &HashMap<String, serde_json::Value>) -> Value {
    let evald: Vec<Value> = args.iter().map(|a| a.eval(row)).collect();
    match name {
        "strlen" => evald.first().map_or(Value::Null, |v| {
            Value::Num(v.as_str().chars().count() as f64)
        }),
        "lower" => evald
            .first()
            .map_or(Value::Null, |v| Value::Str(v.as_str().to_lowercase())),
        "upper" => evald
            .first()
            .map_or(Value::Null, |v| Value::Str(v.as_str().to_uppercase())),
        "abs" => evald
            .first()
            .and_then(Value::as_f64)
            .map_or(Value::Null, |n| Value::Num(n.abs())),
        "concat" => {
            let mut s = String::new();
            for v in &evald {
                if !matches!(v, Value::Null) {
                    s.push_str(&v.as_str());
                }
            }
            Value::Str(s)
        }
        "coalesce" => evald
            .into_iter()
            .find(|v| !matches!(v, Value::Null))
            .unwrap_or(Value::Null),
        "substring" => {
            let Some(s) = evald.first().map(Value::as_str) else {
                return Value::Null;
            };
            let chars: Vec<char> = s.chars().collect();
            let idx = evald
                .get(1)
                .and_then(Value::as_f64)
                .map(|f| f as isize)
                .unwrap_or(0);
            let start = idx.clamp(0, chars.len() as isize) as usize;
            let end = match evald.get(2).and_then(Value::as_f64) {
                Some(len) => {
                    let len = len.max(0.0) as usize;
                    start.saturating_add(len).min(chars.len())
                }
                None => chars.len(),
            };
            if start >= end {
                Value::Str(String::new())
            } else {
                Value::Str(chars[start..end].iter().collect())
            }
        }
        "case" => {
            // case(cond1, then1, cond2, then2, ..., [else])
            let mut i = 0;
            while i + 1 < evald.len() {
                if evald[i].is_truthy() {
                    return evald[i + 1].clone();
                }
                i += 2;
            }
            // Trailing odd argument is the else branch.
            if evald.len() % 2 == 1
                && let Some(last) = evald.last()
            {
                return last.clone();
            }
            Value::Null
        }
        _ => Value::Null,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn row(pairs: &[(&str, serde_json::Value)]) -> HashMap<String, serde_json::Value> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.clone()))
            .collect()
    }

    fn eval(expr: &str, r: &HashMap<String, serde_json::Value>) -> Value {
        Expr::compile(expr).expect("compile").eval(r)
    }

    #[test]
    fn arithmetic() {
        let r = row(&[("a", json!(10)), ("b", json!(4))]);
        assert_eq!(eval("a + b", &r), Value::Num(14.0));
        assert_eq!(eval("a - b", &r), Value::Num(6.0));
        assert_eq!(eval("a * b", &r), Value::Num(40.0));
        assert_eq!(eval("a / b", &r), Value::Num(2.5));
        assert_eq!(eval("a % b", &r), Value::Num(2.0));
        assert_eq!(eval("(a + b) * 2", &r), Value::Num(28.0));
    }

    #[test]
    fn division_by_zero_is_null() {
        let r = row(&[("a", json!(10))]);
        assert_eq!(eval("a / 0", &r), Value::Null);
    }

    #[test]
    fn comparisons_and_logic() {
        let r = row(&[("a", json!(10)), ("b", json!(4))]);
        assert_eq!(eval("a > b", &r), Value::Bool(true));
        assert_eq!(eval("a < b", &r), Value::Bool(false));
        assert_eq!(eval("a > b && b > 1", &r), Value::Bool(true));
        assert_eq!(eval("a < b || b > 1", &r), Value::Bool(true));
        assert_eq!(eval("!(a < b)", &r), Value::Bool(true));
    }

    #[test]
    fn string_functions() {
        let r = row(&[("name", json!("Tokyo"))]);
        assert_eq!(eval("strlen(name)", &r), Value::Num(5.0));
        assert_eq!(eval("lower(name)", &r), Value::Str("tokyo".into()));
        assert_eq!(eval("upper(name)", &r), Value::Str("TOKYO".into()));
        assert_eq!(eval("substring(name, 0, 3)", &r), Value::Str("Tok".into()));
        assert_eq!(eval("substring(name, 2)", &r), Value::Str("kyo".into()));
        assert_eq!(
            eval("concat(name, '-', 'JP')", &r),
            Value::Str("Tokyo-JP".into())
        );
    }

    #[test]
    fn coalesce_and_case() {
        let r = row(&[("a", json!(null)), ("b", json!(7))]);
        assert_eq!(eval("coalesce(a, b)", &r), Value::Num(7.0));
        let r2 = row(&[("x", json!(5))]);
        assert_eq!(
            eval("case(x > 10, 'big', x > 3, 'mid', 'small')", &r2),
            Value::Str("mid".into())
        );
        assert_eq!(
            eval("case(x > 10, 'big', 'small')", &r2),
            Value::Str("small".into())
        );
    }

    #[test]
    fn abs_function() {
        let r = row(&[("a", json!(-9))]);
        assert_eq!(eval("abs(a)", &r), Value::Num(9.0));
    }

    #[test]
    fn plus_concatenates_strings() {
        let r = row(&[("a", json!("foo")), ("b", json!("bar"))]);
        assert_eq!(eval("a + b", &r), Value::Str("foobar".into()));
    }

    #[test]
    fn null_column_evaluates_null() {
        let r = row(&[]);
        assert_eq!(eval("missing + 1", &r), Value::Null);
    }

    #[test]
    fn unknown_function_fails_compile() {
        let err = Expr::compile("frobnicate(x)").expect_err("must fail");
        assert!(err.0.contains("unknown function"), "{err}");
    }

    #[test]
    fn malformed_expression_fails_compile() {
        assert!(Expr::compile("a + ").is_err());
        assert!(Expr::compile("(a + b").is_err());
        assert!(Expr::compile("").is_err());
    }

    #[test]
    fn to_json_integral_emits_integer() {
        assert_eq!(Value::Num(14.0).to_json(), json!(14));
        assert_eq!(Value::Num(2.5).to_json(), json!(2.5));
    }
}
