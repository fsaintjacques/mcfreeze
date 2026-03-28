//! Command dispatch — wires parsed [`Command`]s to [`Lookup`] and the framer.
//!
//! [`dispatch`] is the single entry point for the connection handler.  It
//! takes one already-parsed command, executes it against the provided
//! [`Lookup`], writes the response bytes into `dst`, and returns a
//! [`Disposition`] indicating whether the connection should stay open.
//!
//! All lookup errors are handled internally: they produce a `SERVER_ERROR`
//! response and keep the connection alive.  The caller never needs to handle
//! an `Err` from `dispatch`.

use bytes::BytesMut;

use crate::lookup::Lookup;
use super::meta::{Command, MgFlags, write_en, write_server_error, write_va, write_version};

// ---------------------------------------------------------------------------
// Public types
// ---------------------------------------------------------------------------

/// What the connection handler should do after `dispatch` returns.
#[derive(Debug, PartialEq)]
pub enum Disposition {
    /// Keep the connection open and process more commands.
    Continue,
    /// Close the connection (client sent `quit`).
    Close,
    /// Drain `n` bytes from the read buffer before parsing the next command.
    ///
    /// Returned for `ms` write commands: the parser consumes only the command
    /// line; the caller must advance the buffer by `data_len + 2` (data body
    /// plus its mandatory `\r\n` terminator) to re-synchronise the stream.
    Drain(usize),
}

// ---------------------------------------------------------------------------
// Dispatch
// ---------------------------------------------------------------------------

/// Dispatch one command, write the response into `dst`, return the disposition.
pub async fn dispatch(
    cmd: Command,
    lookup: &dyn Lookup,
    dst: &mut BytesMut,
    semver: &str,
    generation: u64,
) -> Disposition {
    match cmd {
        Command::Mg { ref key, ref flags } => {
            dispatch_mg(key, flags, lookup, dst).await;
            Disposition::Continue
        }
        Command::Version => {
            write_version(dst, semver, generation);
            Disposition::Continue
        }
        Command::Quit => Disposition::Close,
        Command::WriteRejected { data_len } => {
            write_server_error(dst, b"read-only");
            if data_len == 0 {
                // md / ma: the parser consumed the entire command line including
                // its CRLF. No data body follows; nothing to drain.
                Disposition::Continue
            } else {
                // ms: parser consumed only the command line. The data body
                // (data_len bytes) plus its mandatory \r\n terminator remain
                // in the stream and must be drained before the next command.
                Disposition::Drain(data_len + 2)
            }
        }
    }
}

async fn dispatch_mg(key: &[u8], flags: &MgFlags, lookup: &dyn Lookup, dst: &mut BytesMut) {
    match lookup.get(key).await {
        Ok(Some(value)) => write_va(dst, &value, flags, key),
        Ok(None)        => write_en(dst),
        Err(e) => {
            tracing::error!(key = %String::from_utf8_lossy(key), "lookup error: {e}");
            write_server_error(dst, b"internal error");
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ServeError, lookup::Lookup};
    use async_trait::async_trait;
    use bytes::Bytes;
    use std::collections::HashMap;

    // Trivial in-memory Lookup stub — no snapshot, no file I/O.
    struct MockLookup(HashMap<&'static [u8], &'static [u8]>);

    impl MockLookup {
        fn new(entries: &[(&'static [u8], &'static [u8])]) -> Self {
            Self(entries.iter().copied().collect())
        }
    }

    #[async_trait]
    impl Lookup for MockLookup {
        async fn get(&self, key: &[u8]) -> Result<Option<Bytes>, ServeError> {
            Ok(self.0.get(key).map(|&v| Bytes::from_static(v)))
        }
    }

    // Lookup stub that always returns an error.
    struct ErrLookup;

    #[async_trait]
    impl Lookup for ErrLookup {
        async fn get(&self, _key: &[u8]) -> Result<Option<Bytes>, ServeError> {
            Err(ServeError::BlockingTaskPanicked("test".into()))
        }
    }

    fn buf() -> BytesMut { BytesMut::new() }

    // --- mg: hit ---

    #[tokio::test]
    async fn mg_hit_writes_va() {
        let lookup = MockLookup::new(&[(b"hello", b"world")]);
        let mut dst = buf();
        let cmd = Command::Mg { key: Bytes::from_static(b"hello"), flags: MgFlags::default() };
        let d = dispatch(cmd, &lookup, &mut dst, "0.1.0", 0).await;
        assert_eq!(d, Disposition::Continue);
        assert_eq!(&dst[..], b"VA 5\r\nworld\r\n");
    }

    #[tokio::test]
    async fn mg_hit_with_key_flag() {
        let lookup = MockLookup::new(&[(b"k", b"v")]);
        let mut dst = buf();
        let cmd = Command::Mg {
            key:   Bytes::from_static(b"k"),
            flags: MgFlags { k: true, ..Default::default() },
        };
        dispatch(cmd, &lookup, &mut dst, "0.1.0", 0).await;
        assert_eq!(&dst[..], b"VA 1 kk\r\nv\r\n");
    }

    #[tokio::test]
    async fn mg_hit_with_ttl_flag() {
        let lookup = MockLookup::new(&[(b"k", b"v")]);
        let mut dst = buf();
        let cmd = Command::Mg {
            key:   Bytes::from_static(b"k"),
            flags: MgFlags { t: true, ..Default::default() },
        };
        dispatch(cmd, &lookup, &mut dst, "0.1.0", 0).await;
        assert_eq!(&dst[..], b"VA 1 t-1\r\nv\r\n");
    }

    // --- mg: miss ---

    #[tokio::test]
    async fn mg_miss_writes_en() {
        let lookup = MockLookup::new(&[]);
        let mut dst = buf();
        let cmd = Command::Mg { key: Bytes::from_static(b"absent"), flags: MgFlags::default() };
        let d = dispatch(cmd, &lookup, &mut dst, "0.1.0", 0).await;
        assert_eq!(d, Disposition::Continue);
        assert_eq!(&dst[..], b"EN\r\n");
    }

    // --- mg: lookup error ---

    #[tokio::test]
    async fn mg_error_writes_server_error() {
        let mut dst = buf();
        let cmd = Command::Mg { key: Bytes::from_static(b"k"), flags: MgFlags::default() };
        let d = dispatch(cmd, &ErrLookup, &mut dst, "0.1.0", 0).await;
        assert_eq!(d, Disposition::Continue);
        assert_eq!(&dst[..], b"SERVER_ERROR internal error\r\n");
    }

    // --- version ---

    #[tokio::test]
    async fn version_writes_version_line() {
        let lookup = MockLookup::new(&[]);
        let mut dst = buf();
        let d = dispatch(Command::Version, &lookup, &mut dst, "0.1.0", 3).await;
        assert_eq!(d, Disposition::Continue);
        assert_eq!(&dst[..], b"VERSION 0.1.0 gen/3\r\n");
    }

    // --- quit ---

    #[tokio::test]
    async fn quit_returns_close() {
        let lookup = MockLookup::new(&[]);
        let mut dst = buf();
        let d = dispatch(Command::Quit, &lookup, &mut dst, "0.1.0", 0).await;
        assert_eq!(d, Disposition::Close);
        assert!(dst.is_empty());  // no response bytes
    }

    // --- write commands ---

    #[tokio::test]
    async fn write_rejected_md_returns_continue() {
        // md/ma: data_len=0, parser already consumed the CRLF — nothing to drain.
        let lookup = MockLookup::new(&[]);
        let mut dst = buf();
        let d = dispatch(
            Command::WriteRejected { data_len: 0 },
            &lookup, &mut dst, "0.1.0", 0,
        ).await;
        assert_eq!(d, Disposition::Continue);
        assert_eq!(&dst[..], b"SERVER_ERROR read-only\r\n");
    }

    #[tokio::test]
    async fn write_rejected_ms_returns_drain_data_len_plus_2() {
        // ms with a 5-byte body: drain = 5+2 = 7.
        let lookup = MockLookup::new(&[]);
        let mut dst = buf();
        let d = dispatch(
            Command::WriteRejected { data_len: 5 },
            &lookup, &mut dst, "0.1.0", 0,
        ).await;
        assert_eq!(d, Disposition::Drain(7));
        assert_eq!(&dst[..], b"SERVER_ERROR read-only\r\n");
    }

    // --- pipelining: multiple commands accumulate in dst ---

    #[tokio::test]
    async fn pipeline_accumulates_responses() {
        let lookup = MockLookup::new(&[(b"k", b"v")]);
        let mut dst = buf();

        dispatch(Command::Mg { key: Bytes::from_static(b"k"),      flags: MgFlags::default() }, &lookup, &mut dst, "0.1.0", 1).await;
        dispatch(Command::Mg { key: Bytes::from_static(b"absent"), flags: MgFlags::default() }, &lookup, &mut dst, "0.1.0", 1).await;
        dispatch(Command::Version, &lookup, &mut dst, "0.1.0", 1).await;

        assert_eq!(&dst[..], b"VA 1\r\nv\r\nEN\r\nVERSION 0.1.0 gen/1\r\n");
    }
}
