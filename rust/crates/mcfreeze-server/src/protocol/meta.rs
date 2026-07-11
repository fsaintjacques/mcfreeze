// SPDX-License-Identifier: Apache-2.0

//! Memcache meta protocol — parser and framer.
//!
//! ## Parser
//!
//! [`parse_command`] reads one command from a [`BytesMut`] buffer using
//! streaming nom parsers:
//!
//! - `Ok(Some(cmd))` — complete command; buffer advanced past it.
//! - `Ok(None)` — not enough data yet; call again when more arrives.
//! - `Err(ProtoError)` — unrecoverable parse error; close the connection.
//!
//! ## Framer
//!
//! [`write_va`], [`write_en`], [`write_server_error`], and [`write_version`]
//! append response bytes to a [`BytesMut`] write buffer. The caller flushes
//! the buffer to the socket after processing each pipeline batch.

use bytes::{Buf, BufMut, Bytes, BytesMut};
use nom::{
    branch::alt,
    bytes::streaming::{tag, take_until},
    character::streaming::{digit1, space1},
    combinator::{map_res, opt},
    sequence::preceded,
    IResult,
};

// ---------------------------------------------------------------------------
// Public types
// ---------------------------------------------------------------------------

/// Flags parsed from an `mg` command line.
#[derive(Debug, Default, PartialEq)]
pub struct MgFlags {
    /// `v` — return value bytes (always honoured by this server).
    pub v: bool,
    /// `t` — return TTL remaining; server returns `t-1` (no expiry in mcfreeze).
    pub t: bool,
    /// `h` — return hit-before flag; not tracked, omitted from the response.
    pub h: bool,
    /// `k` — echo the key in the `VA` response line.
    pub k: bool,
}

/// A fully-parsed client command.
#[derive(Debug, PartialEq)]
pub enum Command {
    /// `mg <key> [flags]\r\n`
    Mg { key: Bytes, flags: MgFlags },
    /// `version\r\n`
    Version,
    /// `quit\r\n`
    Quit,
    /// `ms`/`md`/`ma` — write command; this server is read-only.
    ///
    /// `data_len` is the size of the `ms` data body in bytes.  After
    /// receiving this variant the caller **must drain `data_len + 2` bytes**
    /// from the read buffer — the data block itself (`data_len`) plus its
    /// mandatory `\r\n` terminator (`+2`) — before the next command begins.
    /// Zero for `md` and `ma`, which carry no data body.
    WriteRejected { data_len: usize },
}

/// Parse-layer error.  Signals unrecoverable input; the connection should be
/// closed after sending an appropriate error response.
#[derive(Debug, thiserror::Error)]
pub enum ProtoError {
    #[error("parse error: {0}")]
    Parse(String),
}

// ---------------------------------------------------------------------------
// Parser (internal)
// ---------------------------------------------------------------------------

type ParseResult<'a, O> = IResult<&'a [u8], O>;

/// Non-whitespace run — stops at SP, CR, or LF.
fn token(input: &[u8]) -> ParseResult<'_, &[u8]> {
    nom::bytes::streaming::take_while1(|c| c != b' ' && c != b'\r' && c != b'\n')(input)
}

fn crlf(input: &[u8]) -> ParseResult<'_, ()> {
    let (input, _) = tag(b"\r\n")(input)?;
    Ok((input, ()))
}

fn parse_mg(input: &[u8]) -> ParseResult<'_, Command> {
    let (input, _) = tag(b"mg")(input)?;
    let (input, _) = space1(input)?;
    let (input, key_bytes) = token(input)?;

    let mut flags = MgFlags::default();
    let mut rest = input;

    // Zero or more ` <flag-token>` pairs until CRLF.
    // Each flag token is a single letter optionally followed by a value
    // (e.g. `N300`); we only inspect the first byte.
    // Unknown flags are silently ignored for forward compatibility.
    loop {
        match opt(preceded(space1::<&[u8], nom::error::Error<&[u8]>>, token))(rest) {
            Ok((r, Some(tok))) => {
                if let Some(&b) = tok.first() {
                    match b {
                        b'v' => flags.v = true,
                        b't' => flags.t = true,
                        b'h' => flags.h = true,
                        b'k' => flags.k = true,
                        _ => {}
                    }
                }
                rest = r;
            }
            Ok((_, None)) => break,
            Err(e) => return Err(e),
        }
    }

    let (rest, _) = crlf(rest)?;
    Ok((
        rest,
        Command::Mg {
            key: Bytes::copy_from_slice(key_bytes),
            flags,
        },
    ))
}

fn parse_version(input: &[u8]) -> ParseResult<'_, Command> {
    let (input, _) = tag(b"version")(input)?;
    let (input, _) = crlf(input)?;
    Ok((input, Command::Version))
}

fn parse_quit(input: &[u8]) -> ParseResult<'_, Command> {
    let (input, _) = tag(b"quit")(input)?;
    let (input, _) = crlf(input)?;
    Ok((input, Command::Quit))
}

/// `ms <key> <bytes> [flags]\r\n` — meta set; rejected by this read-only server.
/// We parse `<bytes>` so the caller knows how much data body to drain.
fn parse_ms(input: &[u8]) -> ParseResult<'_, Command> {
    let (input, _) = tag(b"ms")(input)?;
    let (input, _) = space1(input)?;
    let (input, _) = token(input)?; // key
    let (input, _) = space1(input)?;
    let (input, data_len) = map_res(map_res(digit1, std::str::from_utf8), |s: &str| {
        s.parse::<usize>()
    })(input)?;
    let (input, _) = take_until(b"\r\n" as &[u8])(input)?;
    let (input, _) = crlf(input)?;
    Ok((input, Command::WriteRejected { data_len }))
}

/// `md`/`ma` — meta delete / meta arithmetic; no data body.
fn parse_md_ma(input: &[u8]) -> ParseResult<'_, Command> {
    let (input, _) = alt((tag(b"md"), tag(b"ma")))(input)?;
    let (input, _) = take_until(b"\r\n" as &[u8])(input)?;
    let (input, _) = crlf(input)?;
    Ok((input, Command::WriteRejected { data_len: 0 }))
}

fn nom_parse(input: &[u8]) -> ParseResult<'_, Command> {
    alt((parse_mg, parse_version, parse_quit, parse_ms, parse_md_ma))(input)
}

/// Parse one command from `src`.
///
/// Advances `src` past the consumed bytes on success.
pub fn parse_command(src: &mut BytesMut) -> Result<Option<Command>, ProtoError> {
    match nom_parse(src) {
        Ok((remaining, cmd)) => {
            let consumed = src.len() - remaining.len();
            src.advance(consumed);
            Ok(Some(cmd))
        }
        Err(nom::Err::Incomplete(_)) => Ok(None),
        Err(nom::Err::Error(e)) | Err(nom::Err::Failure(e)) => {
            Err(ProtoError::Parse(format!("{:?}", e.code)))
        }
    }
}

// ---------------------------------------------------------------------------
// Framer
// ---------------------------------------------------------------------------

/// Write a `VA` (hit) response.
///
/// Echoes `k<key>` if `flags.k` is set; appends `t-1` if `flags.t` is set
/// (mcfreeze snapshots have no TTL).
pub fn write_va(dst: &mut BytesMut, value: &[u8], flags: &MgFlags, key: &[u8]) {
    dst.put_slice(b"VA ");
    dst.put_slice(value.len().to_string().as_bytes());
    if flags.k {
        dst.put_slice(b" k");
        dst.put_slice(key);
    }
    if flags.t {
        dst.put_slice(b" t-1");
    }
    dst.put_slice(b"\r\n");
    dst.put_slice(value);
    dst.put_slice(b"\r\n");
}

/// Write an `EN` (miss) response.
pub fn write_en(dst: &mut BytesMut) {
    dst.put_slice(b"EN\r\n");
}

/// Write a `SERVER_ERROR` response.
///
/// `msg` must not contain CR or LF — embedding them would split the response
/// line and corrupt the frame stream.
pub fn write_server_error(dst: &mut BytesMut, msg: &[u8]) {
    debug_assert!(
        !msg.contains(&b'\r') && !msg.contains(&b'\n'),
        "write_server_error: msg must not contain CR or LF"
    );
    dst.put_slice(b"SERVER_ERROR ");
    dst.put_slice(msg);
    dst.put_slice(b"\r\n");
}

/// Write a `VERSION` response, including the catalog generation counter.
pub fn write_version(dst: &mut BytesMut, semver: &str, generation: u64) {
    dst.put_slice(b"VERSION ");
    dst.put_slice(semver.as_bytes());
    dst.put_slice(b" gen/");
    dst.put_slice(generation.to_string().as_bytes());
    dst.put_slice(b"\r\n");
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(input: &[u8]) -> Result<Option<Command>, ProtoError> {
        parse_command(&mut BytesMut::from(input))
    }

    // --- Parser: mg ---

    #[test]
    fn mg_no_flags() {
        let cmd = parse(b"mg mykey\r\n").unwrap().unwrap();
        assert_eq!(
            cmd,
            Command::Mg {
                key: Bytes::from_static(b"mykey"),
                flags: MgFlags::default(),
            }
        );
    }

    #[test]
    fn mg_all_flags() {
        let cmd = parse(b"mg mykey v t h k\r\n").unwrap().unwrap();
        assert_eq!(
            cmd,
            Command::Mg {
                key: Bytes::from_static(b"mykey"),
                flags: MgFlags {
                    v: true,
                    t: true,
                    h: true,
                    k: true
                },
            }
        );
    }

    #[test]
    fn mg_unknown_flag_ignored() {
        let cmd = parse(b"mg key v X\r\n").unwrap().unwrap();
        assert_eq!(
            cmd,
            Command::Mg {
                key: Bytes::from_static(b"key"),
                flags: MgFlags {
                    v: true,
                    ..Default::default()
                },
            }
        );
    }

    #[test]
    fn mg_flag_with_value_token() {
        // Flags like `N300` (inline value) — first byte is `N`, which is unknown;
        // should be silently ignored.
        let cmd = parse(b"mg key N300 v\r\n").unwrap().unwrap();
        assert_eq!(
            cmd,
            Command::Mg {
                key: Bytes::from_static(b"key"),
                flags: MgFlags {
                    v: true,
                    ..Default::default()
                },
            }
        );
    }

    // --- Parser: version / quit ---

    #[test]
    fn version_command() {
        assert_eq!(parse(b"version\r\n").unwrap(), Some(Command::Version));
    }

    #[test]
    fn quit_command() {
        assert_eq!(parse(b"quit\r\n").unwrap(), Some(Command::Quit));
    }

    // --- Parser: write commands ---

    #[test]
    fn ms_rejected_with_data_len() {
        let cmd = parse(b"ms mykey 42 S12\r\n").unwrap().unwrap();
        assert_eq!(cmd, Command::WriteRejected { data_len: 42 });
    }

    #[test]
    fn ms_drain_contract_with_pipelined_command() {
        // Full ms frame: command line + 5-byte data body + \r\n terminator,
        // followed by a pipelined version command.
        let mut buf = BytesMut::from(&b"ms mykey 5 S12\r\nhello\r\nversion\r\n"[..]);

        // Parser returns WriteRejected after consuming only the command line;
        // the data body is NOT consumed.
        let cmd = parse_command(&mut buf).unwrap().unwrap();
        assert_eq!(cmd, Command::WriteRejected { data_len: 5 });

        // Caller drains data_len + 2 (body + mandatory \r\n terminator).
        buf.advance(5 + 2);

        // Next parse_command sees the pipelined command cleanly.
        assert_eq!(parse_command(&mut buf).unwrap(), Some(Command::Version));
        assert!(buf.is_empty());
    }

    #[test]
    fn md_rejected_no_data() {
        let cmd = parse(b"md mykey\r\n").unwrap().unwrap();
        assert_eq!(cmd, Command::WriteRejected { data_len: 0 });
    }

    #[test]
    fn ma_rejected_no_data() {
        let cmd = parse(b"ma counter\r\n").unwrap().unwrap();
        assert_eq!(cmd, Command::WriteRejected { data_len: 0 });
    }

    // --- Parser: streaming / pipelining ---

    #[test]
    fn incomplete_returns_none() {
        assert_eq!(parse(b"mg key").unwrap(), None);
        assert_eq!(parse(b"mg key v").unwrap(), None);
        assert_eq!(parse(b"version").unwrap(), None);
    }

    #[test]
    fn buffer_advanced_past_command() {
        let mut buf = BytesMut::from(&b"version\r\nquit\r\n"[..]);
        assert_eq!(parse_command(&mut buf).unwrap(), Some(Command::Version));
        assert_eq!(buf.as_ref(), b"quit\r\n");
        assert_eq!(parse_command(&mut buf).unwrap(), Some(Command::Quit));
        assert!(buf.is_empty());
    }

    // --- Framer: write_va ---

    #[test]
    fn va_value_only() {
        let mut dst = BytesMut::new();
        write_va(&mut dst, b"hello", &MgFlags::default(), b"key");
        assert_eq!(&dst[..], b"VA 5\r\nhello\r\n");
    }

    #[test]
    fn va_with_key_echo() {
        let mut dst = BytesMut::new();
        write_va(
            &mut dst,
            b"hello",
            &MgFlags {
                k: true,
                ..Default::default()
            },
            b"mykey",
        );
        assert_eq!(&dst[..], b"VA 5 kmykey\r\nhello\r\n");
    }

    #[test]
    fn va_with_ttl() {
        let mut dst = BytesMut::new();
        write_va(
            &mut dst,
            b"hello",
            &MgFlags {
                t: true,
                ..Default::default()
            },
            b"key",
        );
        assert_eq!(&dst[..], b"VA 5 t-1\r\nhello\r\n");
    }

    #[test]
    fn va_with_key_and_ttl() {
        let mut dst = BytesMut::new();
        write_va(
            &mut dst,
            b"v",
            &MgFlags {
                k: true,
                t: true,
                ..Default::default()
            },
            b"k",
        );
        assert_eq!(&dst[..], b"VA 1 kk t-1\r\nv\r\n");
    }

    // --- Framer: write_en ---

    #[test]
    fn en_response() {
        let mut dst = BytesMut::new();
        write_en(&mut dst);
        assert_eq!(&dst[..], b"EN\r\n");
    }

    // --- Framer: write_server_error ---

    #[test]
    fn server_error_response() {
        let mut dst = BytesMut::new();
        write_server_error(&mut dst, b"read-only");
        assert_eq!(&dst[..], b"SERVER_ERROR read-only\r\n");
    }

    // --- Framer: write_version ---

    #[test]
    fn version_response() {
        let mut dst = BytesMut::new();
        write_version(&mut dst, "0.1.0", 7);
        assert_eq!(&dst[..], b"VERSION 0.1.0 gen/7\r\n");
    }
}
