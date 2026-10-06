//! A small strict bencode framing parser. It preserves source spans and rejects
//! duplicate or unsorted dictionary keys, avoiding re-encoding for infohashes.
use std::ops::Range;
use thiserror::Error;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Value {
    Integer(i64),
    Bytes(Range<usize>),
    List(Vec<Value>),
    Dict(Vec<(Range<usize>, Value)>),
}

#[derive(Clone, Copy, Debug)]
pub struct Limits {
    pub depth: usize,
    pub items: usize,
    pub string: usize,
    pub input: usize,
}
impl Default for Limits {
    fn default() -> Self {
        Self {
            depth: 32,
            items: 100_000,
            string: 8 * 1024 * 1024,
            input: 64 * 1024 * 1024,
        }
    }
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum Error {
    #[error("input exceeds configured limit")]
    InputLimit,
    #[error("truncated or malformed bencode at byte {0}")]
    Invalid(usize),
    #[error("nesting limit exceeded")]
    Depth,
    #[error("item limit exceeded")]
    Items,
    #[error("string limit exceeded")]
    StringLimit,
    #[error("integer overflow or non-canonical integer")]
    Integer,
    #[error("dictionary keys must be unique and bytewise sorted")]
    DictionaryKey,
    #[error("trailing bytes after top-level value")]
    Trailing,
}

pub fn parse(input: &[u8], limits: Limits) -> Result<Value, Error> {
    let (value, used) = parse_prefix(input, limits)?;
    if used != input.len() {
        return Err(Error::Trailing);
    }
    Ok(value)
}

/// Parse one complete bencode value and return the first byte after it.
/// Trailing bytes are left untouched for protocols that append binary data.
pub fn parse_prefix(input: &[u8], limits: Limits) -> Result<(Value, usize), Error> {
    if input.len() > limits.input {
        return Err(Error::InputLimit);
    }
    let mut p = Parser {
        input,
        at: 0,
        items: 0,
        limits,
    };
    let v = p.value(0)?;
    Ok((v, p.at))
}

struct Parser<'a> {
    input: &'a [u8],
    at: usize,
    items: usize,
    limits: Limits,
}
impl Parser<'_> {
    fn value(&mut self, depth: usize) -> Result<Value, Error> {
        if depth > self.limits.depth {
            return Err(Error::Depth);
        }
        self.items += 1;
        if self.items > self.limits.items {
            return Err(Error::Items);
        }
        match self.input.get(self.at).copied() {
            Some(b'i') => self.integer(),
            Some(b'l') => self.list(depth),
            Some(b'd') => self.dict(depth),
            Some(b'0'..=b'9') => self.bytes(),
            _ => Err(Error::Invalid(self.at)),
        }
    }
    fn integer(&mut self) -> Result<Value, Error> {
        self.at += 1;
        let start = self.at;
        while self.input.get(self.at).is_some_and(|b| *b != b'e') {
            self.at += 1;
        }
        if self.at == self.input.len() {
            return Err(Error::Invalid(self.at));
        }
        let s = std::str::from_utf8(&self.input[start..self.at]).map_err(|_| Error::Integer)?;
        if s.is_empty() || s == "-0" || (s.starts_with('0') && s.len() > 1) || s.starts_with("-0") {
            return Err(Error::Integer);
        }
        let n = s.parse().map_err(|_| Error::Integer)?;
        self.at += 1;
        Ok(Value::Integer(n))
    }
    fn bytes(&mut self) -> Result<Value, Error> {
        let mut len = 0usize;
        let start = self.at;
        while let Some(b'0'..=b'9') = self.input.get(self.at).copied() {
            len = len
                .checked_mul(10)
                .and_then(|x| x.checked_add((self.input[self.at] - b'0') as usize))
                .ok_or(Error::StringLimit)?;
            self.at += 1;
        }
        if self.at == start
            || self.input.get(self.at) != Some(&b':')
            || (self.at - start > 1 && self.input[start] == b'0')
        {
            return Err(Error::Invalid(start));
        }
        if len > self.limits.string {
            return Err(Error::StringLimit);
        }
        self.at += 1;
        let begin = self.at;
        let end = begin.checked_add(len).ok_or(Error::StringLimit)?;
        if end > self.input.len() {
            return Err(Error::Invalid(begin));
        }
        self.at = end;
        Ok(Value::Bytes(begin..end))
    }
    fn list(&mut self, depth: usize) -> Result<Value, Error> {
        self.at += 1;
        let mut xs = Vec::new();
        while self.input.get(self.at) != Some(&b'e') {
            if self.at >= self.input.len() {
                return Err(Error::Invalid(self.at));
            }
            xs.push(self.value(depth + 1)?);
        }
        self.at += 1;
        Ok(Value::List(xs))
    }
    fn dict(&mut self, depth: usize) -> Result<Value, Error> {
        self.at += 1;
        let mut xs = Vec::new();
        let mut prev: Option<Vec<u8>> = None;
        while self.input.get(self.at) != Some(&b'e') {
            if self.at >= self.input.len() {
                return Err(Error::Invalid(self.at));
            }
            let Value::Bytes(key) = self.bytes()? else {
                unreachable!()
            };
            let k = self.input[key.clone()].to_vec();
            if prev.as_ref().is_some_and(|p| p >= &k) {
                return Err(Error::DictionaryKey);
            }
            prev = Some(k);
            let v = self.value(depth + 1)?;
            xs.push((key, v));
        }
        self.at += 1;
        Ok(Value::Dict(xs))
    }
}

pub fn dict_get<'a>(value: &'a Value, input: &'a [u8], key: &[u8]) -> Option<&'a Value> {
    let Value::Dict(items) = value else {
        return None;
    };
    items
        .iter()
        .find(|(k, _)| &input[k.clone()] == key)
        .map(|(_, v)| v)
}
pub fn bytes<'a>(value: &'a Value, input: &'a [u8]) -> Option<&'a [u8]> {
    if let Value::Bytes(r) = value {
        Some(&input[r.clone()])
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn rejects_duplicate_unsorted_and_noncanonical_values() {
        for bad in [
            b"d1:ai1e1:ai2ee".as_slice(),
            b"d1:bi1e1:ai2ee",
            b"i03e",
            b"i-0e",
            b"01:a",
        ] {
            assert!(parse(bad, Limits::default()).is_err(), "accepted {bad:?}");
        }
    }
    #[test]
    fn preserves_byte_ranges() {
        let src = b"d1:ai1ee";
        let v = parse(src, Limits::default()).unwrap();
        assert_eq!(
            dict_get(&v, src, b"a").and_then(|x| if let Value::Integer(i) = x {
                Some(*i)
            } else {
                None
            }),
            Some(1)
        );
    }

    #[test]
    fn rejects_numeric_and_resource_limit_overflows() {
        assert_eq!(
            parse(b"i9223372036854775808e", Limits::default()),
            Err(Error::Integer)
        );
        assert_eq!(
            parse(b"i-9223372036854775809e", Limits::default()),
            Err(Error::Integer)
        );
        assert_eq!(
            parse(
                b"5:hello",
                Limits {
                    string: 4,
                    ..Limits::default()
                }
            ),
            Err(Error::StringLimit)
        );
        assert_eq!(
            parse(
                b"lli1eee",
                Limits {
                    depth: 1,
                    ..Limits::default()
                }
            ),
            Err(Error::Depth)
        );
        assert_eq!(
            parse(
                b"li1ei2ee",
                Limits {
                    items: 2,
                    ..Limits::default()
                }
            ),
            Err(Error::Items)
        );
    }
}
