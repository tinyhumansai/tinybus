//! Bulk payloads: chunked, flow-controlled byte streams between two peers.
//!
//! A frame is capped at [`MAX_FRAME_LEN`](crate::message::codec::MAX_FRAME_LEN)
//! and that cap is not negotiable — it is what stops a peer announcing a
//! gigabyte and making the reader allocate it. A stream is how a payload larger
//! than one frame crosses the bus anyway: the sender opens a stream on the
//! receiver, writes it as a sequence of bounded chunks, and closes it. What
//! travels in the method call is a [`StreamRef`] — a handle a few dozen bytes
//! long — and the bytes travel beside it.
//!
//! # Why this is a peer-to-peer interface and not a broker feature
//!
//! Every chunk is an ordinary method call addressed to the receiving peer. The
//! broker reads the header, routes it, and forwards it, exactly as it does for
//! everything else; it never sees a stream as anything other than traffic. A
//! broker that assembled streams would be a broker that buffers every payload
//! on the bus — which is both the memory problem and the "the broker has seen
//! every credential" problem, at once.
//!
//! # Flow control
//!
//! `Write` is a call, so it has a reply and a deadline. The receiver does not
//! reply until the chunk has room in the reader's window
//! ([`StreamLimits::window_chunks`]), so a sender runs exactly as fast as the
//! receiver drains and no faster. There is no unbounded buffer anywhere: a
//! receiver that never reads stalls the sender, the stall shows up as no
//! activity on the stream, and the idle reaper aborts it. That is the
//! misbehaving-peer invariant applied to bulk transfer — one peer's refusal to
//! read costs it its own stream and nobody else's memory.
//!
//! # Ordering
//!
//! Chunks carry a sequence number and the receiver requires the next one
//! exactly. The transport is already ordered, so this catches a pipelining
//! sender rather than a reordering network: two chunks in flight at once would
//! be dispatched into two tasks on the receiver and could land either way
//! round, and silently transposing two megabytes of a PDF is worse than an
//! error.
//!
//! # Base64
//!
//! Bodies are JSON, so a chunk is base64 and costs a third of its size in
//! overhead. That is the price of the payload travelling on the same wire as
//! everything else, and it is why `ROADMAP.md` still wants `SCM_RIGHTS`: an
//! fd-passing fast path can slot in under this same API later, because callers
//! hold a [`StreamRef`], not a byte array.

pub mod base64;

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::sync::{Mutex, mpsc};

use crate::error::{Error, Result};
use crate::message::Header;
use crate::name::{BusName, MemberName};

/// The interface a peer serves so others can push bulk payloads at it.
///
/// Served by every [`Connection`](crate::Connection) automatically, before the
/// object tree is consulted: a stream is bus plumbing, not something each
/// service should have to remember to export.
pub const STREAM_INTERFACE: &str = "ai.tinyhumans.tinybus.Stream";

/// The object path [`STREAM_INTERFACE`] lives at.
pub const STREAM_PATH: &str = "/ai/tinyhumans/tinybus/Stream";

/// The largest chunk a sender may put in one `Write`, before base64.
///
/// Half a megabyte encodes to about 700 KB, which leaves the 16 MB frame cap
/// two orders of magnitude of headroom for the header and for any future field.
/// Small enough that a chunk is a cheap unit of retry and of flow control;
/// large enough that a 100 MB payload is two hundred round trips, not two
/// hundred thousand.
pub const MAX_CHUNK_LEN: usize = 512 * 1024;

/// What a receiver will accept, and how much of itself it will spend doing it.
///
/// A receiver's limits are its own: nothing here is negotiated with the sender,
/// because a limit a peer can talk you out of is not a limit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StreamLimits {
    /// The largest single stream, in bytes. A sender that exceeds it has its
    /// stream aborted rather than being allowed to keep going.
    pub max_stream_len: u64,
    /// How many streams one peer may have open at once. Per peer, not global,
    /// so a busy peer cannot starve every other peer of slots.
    pub max_streams_per_peer: usize,
    /// How many chunks may sit between the wire and the reader. This is the
    /// flow-control window: the sender is never more than this far ahead.
    pub window_chunks: usize,
    /// How long a stream may see no writes before it is reaped. Bounds what an
    /// abandoned stream — a sender that exited mid-transfer, or one stalled
    /// against a reader that never reads — can hold open.
    pub idle_timeout: Duration,
}

impl Default for StreamLimits {
    fn default() -> Self {
        Self {
            // 256 MB is a video file or a disk image, not a transcript. Past
            // that a caller wants a path or a content store, not the bus.
            max_stream_len: 256 * 1024 * 1024,
            max_streams_per_peer: 4,
            // Eight chunks is 4 MB in flight: enough that a round trip per
            // chunk does not dominate throughput, bounded enough that
            // `max_streams_per_peer` × this is a number you can hold in mind.
            window_chunks: 8,
            idle_timeout: Duration::from_secs(60),
        }
    }
}

/// A handle to a stream open on the receiving peer.
///
/// This is what travels in a method body in place of the payload. It is only
/// meaningful to the peer that minted it, and only usable by the peer that
/// opened it — the receiver checks the broker-stamped `sender` on every chunk.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamRef {
    /// Opaque, minted by the receiver. Never parse it.
    pub id: String,
    /// What the payload is, if the sender said. Advisory: a receiver that cares
    /// must still validate the bytes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_type: Option<String>,
    /// The total length, once the sender has declared or finished it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub len: Option<u64>,
}

/// What a sender says about a payload before sending it.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamDescriptor {
    /// A media type, if the sender knows one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_type: Option<String>,
    /// The total length, when it is known up front.
    ///
    /// Declaring it lets the receiver reject an oversized transfer at `Open`
    /// instead of after it has already accepted 256 MB of it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub total_len: Option<u64>,
}

impl StreamDescriptor {
    /// A descriptor for a payload of known length and unknown type.
    pub fn with_len(total_len: u64) -> Self {
        Self {
            content_type: None,
            total_len: Some(total_len),
        }
    }

    /// Set the content type.
    pub fn content_type(mut self, content_type: impl Into<String>) -> Self {
        self.content_type = Some(content_type.into());
        self
    }
}

/// How a stream ended, from the receiver's side.
#[derive(Debug, Clone)]
enum Outcome {
    /// The sender called `Close` and the length it declared checked out.
    Complete,
    /// The stream will produce no more bytes, and did not finish.
    ///
    /// The reason is always crate-generated. A peer-supplied string would be a
    /// peer writing into the receiver's logs.
    Aborted(&'static str),
}

/// One stream being received.
struct Inbound {
    /// The peer that opened it, as stamped by the broker. `None` only on a
    /// direct connection with no broker in the middle, where there is no sender
    /// to distinguish peers in the first place.
    owner: Option<BusName>,
    content_type: Option<String>,
    declared_len: Option<u64>,
    /// Serialises the sequence check and the handoff to the reader. Without it
    /// two pipelined chunks could pass the check in order and reach the reader
    /// out of order, since each call is dispatched on its own task.
    ///
    /// It guards nothing but ordering: the counters beside it are atomics
    /// precisely so that `Close` and `Abort` can read them *without* taking
    /// this lock. A chunk write parks here while the window is full, and a peer
    /// that could make `Close` wait on that would have found a way to wedge the
    /// receiver from outside.
    gate: Mutex<()>,
    next_seq: AtomicU64,
    received: AtomicU64,
    /// Dropped to signal end-of-stream; the reader then consults `outcome` to
    /// learn whether that end was a `Close` or an abort.
    chunks: std::sync::Mutex<Option<mpsc::Sender<Vec<u8>>>>,
    /// Taken once, by whoever reads the stream.
    reader: std::sync::Mutex<Option<mpsc::Receiver<Vec<u8>>>>,
    /// Shared with the reader by `Arc` rather than reached through this
    /// struct, because a reader must not keep the `Inbound` — and therefore the
    /// channel's sending half — alive. If it did, a receiving connection that
    /// died mid-stream would leave a reader parked on a channel that can never
    /// close, which is precisely the hang this project exists to not have.
    outcome: Arc<std::sync::Mutex<Option<Outcome>>>,
    last_activity: std::sync::Mutex<Instant>,
}

impl Inbound {
    /// Record how the stream ended, keeping the first verdict.
    ///
    /// First rather than last because the first is the cause and anything after
    /// it is a consequence — a reaped stream whose sender then aborts should
    /// still read as reaped.
    fn finish(&self, outcome: Outcome) {
        let mut slot = self.outcome.lock().expect("stream outcome lock");
        if slot.is_none() {
            *slot = Some(outcome);
        }
    }

    /// Close the writing half, and with it the reader's channel.
    fn seal(&self) {
        *self.chunks.lock().expect("stream chunks lock") = None;
    }

    fn writer(&self) -> Option<mpsc::Sender<Vec<u8>>> {
        self.chunks.lock().expect("stream chunks lock").clone()
    }

    fn touch(&self) {
        *self.last_activity.lock().expect("stream activity lock") = Instant::now();
    }

    fn idle_for(&self) -> Duration {
        self.last_activity
            .lock()
            .expect("stream activity lock")
            .elapsed()
    }
}

/// Every stream one connection is receiving.
///
/// Lives on the connection rather than on the object tree because handling a
/// chunk needs the message header — specifically the stamped `sender` — and
/// [`Interface`](crate::Interface) deliberately does not get one.
pub(crate) struct StreamRegistry {
    limits: std::sync::RwLock<StreamLimits>,
    inbound: std::sync::Mutex<HashMap<String, Arc<Inbound>>>,
    next_id: AtomicU64,
}

impl StreamRegistry {
    pub(crate) fn new() -> Self {
        Self {
            limits: std::sync::RwLock::new(StreamLimits::default()),
            inbound: std::sync::Mutex::new(HashMap::new()),
            next_id: AtomicU64::new(1),
        }
    }

    pub(crate) fn limits(&self) -> StreamLimits {
        *self.limits.read().expect("stream limits lock")
    }

    pub(crate) fn set_limits(&self, limits: StreamLimits) {
        *self.limits.write().expect("stream limits lock") = limits;
    }

    /// Whether a call addresses the built-in stream interface.
    pub(crate) fn handles(header: &Header) -> bool {
        header
            .interface
            .as_ref()
            .is_some_and(|interface| interface.as_str() == STREAM_INTERFACE)
            && header
                .path
                .as_ref()
                .is_some_and(|path| path.as_str() == STREAM_PATH)
    }

    /// Run one call against the stream interface.
    pub(crate) async fn dispatch(&self, header: &Header, body: Value) -> Result<Value> {
        let member = header
            .member
            .as_ref()
            .ok_or_else(|| Error::protocol("stream call is missing a member"))?;
        match member.as_str() {
            "Open" => self.open(header, member, body),
            "Write" => self.write(header, member, body).await,
            "Close" => self.close(header, member, body),
            "Abort" => self.abort(header, member, body),
            _ => Err(Error::UnknownMethod {
                interface: header
                    .interface
                    .clone()
                    .expect("dispatch only runs once the interface matched"),
                member: member.clone(),
            }),
        }
    }

    fn open(&self, header: &Header, member: &MemberName, body: Value) -> Result<Value> {
        let (descriptor,): (StreamDescriptor,) =
            serde_json::from_value(body).map_err(|e| Error::bad_arguments(member.clone(), e))?;
        let limits = self.limits();

        if descriptor
            .total_len
            .is_some_and(|total| total > limits.max_stream_len)
        {
            // Rejecting a declared oversize here rather than at the byte that
            // crosses the line saves both peers the whole transfer.
            return Err(Error::StreamTooLarge {
                limit: limits.max_stream_len,
            });
        }

        let (chunks, reader) = mpsc::channel(limits.window_chunks.max(1));
        let id = format!("s{}", self.next_id.fetch_add(1, Ordering::Relaxed));
        let inbound = Arc::new(Inbound {
            owner: header.sender.clone(),
            content_type: descriptor.content_type,
            declared_len: descriptor.total_len,
            gate: Mutex::new(()),
            next_seq: AtomicU64::new(0),
            received: AtomicU64::new(0),
            chunks: std::sync::Mutex::new(Some(chunks)),
            reader: std::sync::Mutex::new(Some(reader)),
            outcome: Arc::new(std::sync::Mutex::new(None)),
            last_activity: std::sync::Mutex::new(Instant::now()),
        });

        let mut streams = self.inbound.lock().expect("stream registry lock");
        // Reaping here, rather than on a timer, is enough: a stream only
        // lingers once it stops being written to, and the only thing a lingering
        // stream costs anyone is a slot in this check.
        streams.retain(|_, stream| {
            let live = stream.idle_for() < limits.idle_timeout;
            if !live {
                stream.finish(Outcome::Aborted("the stream went idle and was reaped"));
                stream.seal();
            }
            live
        });

        // A closed-but-unread stream still holds its window, so it is capped
        // too — separately from live ones, because the two are different
        // failures. Too many live streams is a sender running ahead of itself;
        // too many closed ones is a receiver that is not collecting what it
        // was sent. Evicting the oldest keeps the newest transfer — the one a
        // call is most likely still waiting on — alive.
        let mut live = 0usize;
        let mut sealed: Vec<(String, Instant)> = Vec::new();
        for (key, stream) in streams.iter() {
            if stream.owner != header.sender {
                continue;
            }
            if stream.writer().is_some() {
                live += 1;
            } else {
                sealed.push((
                    key.clone(),
                    *stream.last_activity.lock().expect("stream activity lock"),
                ));
            }
        }
        if live >= limits.max_streams_per_peer {
            return Err(Error::TooManyStreams {
                limit: limits.max_streams_per_peer,
            });
        }
        if sealed.len() >= limits.max_streams_per_peer {
            sealed.sort_by_key(|(_, at)| *at);
            for (key, _) in sealed
                .iter()
                .take(sealed.len() + 1 - limits.max_streams_per_peer)
            {
                if let Some(stream) = streams.remove(key) {
                    stream.finish(Outcome::Aborted("the receiver never collected the stream"));
                }
            }
        }

        streams.insert(id.clone(), inbound);
        Ok(Value::String(id))
    }

    async fn write(&self, header: &Header, member: &MemberName, body: Value) -> Result<Value> {
        let (id, seq, data): (String, u64, String) =
            serde_json::from_value(body).map_err(|e| Error::bad_arguments(member.clone(), e))?;
        let stream = self.lookup(&id, header)?;
        let limits = self.limits();
        let chunk = base64::decode(&data)?;
        if chunk.len() > MAX_CHUNK_LEN {
            self.kill(&id, &stream, "the sender exceeded the chunk cap");
            return Err(Error::protocol(format!(
                "chunk of {} bytes exceeds the {MAX_CHUNK_LEN}-byte cap",
                chunk.len()
            )));
        }

        let gate = stream.gate.lock().await;
        let Some(chunks) = stream.writer() else {
            return Err(Error::StreamAborted {
                reason: "the stream is already closed".to_string(),
            });
        };
        if seq != stream.next_seq.load(Ordering::Relaxed) {
            drop(gate);
            self.kill(&id, &stream, "the sender wrote chunks out of order");
            return Err(Error::protocol("stream chunk arrived out of order"));
        }
        let received = stream.received.load(Ordering::Relaxed) + chunk.len() as u64;
        if received > limits.max_stream_len
            || stream.declared_len.is_some_and(|total| received > total)
        {
            drop(gate);
            self.kill(&id, &stream, "the sender exceeded the length it may write");
            return Err(Error::StreamTooLarge {
                limit: limits.max_stream_len,
            });
        }
        stream.next_seq.fetch_add(1, Ordering::Relaxed);
        stream.received.store(received, Ordering::Relaxed);
        stream.touch();
        // The window is the whole flow-control story: this await is where a
        // sender that has run ahead of the reader waits, and the reply it is
        // waiting on carries the sender's own deadline.
        let delivered = chunks.send(chunk).await;
        drop(gate);
        if delivered.is_err() {
            // The reader was dropped. Tell the sender now rather than letting
            // it push the rest of a payload nobody will ever look at.
            self.kill(&id, &stream, "the receiver stopped reading");
            return Err(Error::StreamAborted {
                reason: "the receiver stopped reading".to_string(),
            });
        }
        stream.touch();
        Ok(Value::Null)
    }

    fn close(&self, header: &Header, member: &MemberName, body: Value) -> Result<Value> {
        let (id, total_len): (String, u64) =
            serde_json::from_value(body).map_err(|e| Error::bad_arguments(member.clone(), e))?;
        let stream = self.lookup(&id, header)?;

        // The entry stays in the registry, sealed. A payload that fits inside
        // the window can be written and closed before the receiving method has
        // even been dispatched, and dropping the entry here would turn that —
        // the *fast* case — into "no such stream".
        let received = stream.received.load(Ordering::Relaxed);
        stream.touch();
        stream.seal();

        if received != total_len {
            stream.finish(Outcome::Aborted("the sender closed a truncated stream"));
            return Err(Error::protocol(format!(
                "stream closed after {received} bytes, {total_len} declared"
            )));
        }
        stream.finish(Outcome::Complete);
        Ok(Value::Null)
    }

    fn abort(&self, header: &Header, member: &MemberName, body: Value) -> Result<Value> {
        let (id,): (String,) =
            serde_json::from_value(body).map_err(|e| Error::bad_arguments(member.clone(), e))?;
        let stream = self.lookup(&id, header)?;
        self.kill(&id, &stream, "the sender aborted the stream");
        Ok(Value::Null)
    }

    /// Find a stream and check that the peer asking owns it.
    ///
    /// The ownership check is the whole authorisation story for streams, and it
    /// rests on `sender` being stamped by the broker: without it, any peer that
    /// guessed an id could interleave its own bytes into someone else's
    /// transfer.
    fn lookup(&self, id: &str, header: &Header) -> Result<Arc<Inbound>> {
        let streams = self.inbound.lock().expect("stream registry lock");
        let stream = streams
            .get(id)
            .ok_or_else(|| Error::UnknownStream { id: id.to_string() })?;
        if stream.owner != header.sender {
            // Deliberately the same error as "no such stream": telling a peer
            // that an id it does not own exists is telling it about traffic
            // between two other peers.
            return Err(Error::UnknownStream { id: id.to_string() });
        }
        Ok(stream.clone())
    }

    /// End a stream from the receiver's side and drop it from the registry.
    fn kill(&self, id: &str, stream: &Arc<Inbound>, reason: &'static str) {
        self.inbound
            .lock()
            .expect("stream registry lock")
            .remove(id);
        stream.finish(Outcome::Aborted(reason));
        // Dropping the sending half is what wakes a reader parked on `recv`.
        stream.seal();
        // And dropping the *reading* half, if nobody ever claimed it, is what
        // wakes a chunk write parked against a full window: without this a
        // stream killed while a sender is mid-`Write` leaves that write parked
        // until the sender's own deadline expires.
        drop(stream.reader.lock().expect("stream reader lock").take());
    }

    /// Hand the reading half of a stream to the caller. Once only.
    ///
    /// The entry leaves the registry: from here the reader owns the stream, and
    /// a sender writing to it is talking to the reader's window rather than to
    /// a table this connection has to keep swept.
    pub(crate) fn take_reader(&self, id: &str) -> Result<StreamReader> {
        let stream = {
            let mut streams = self.inbound.lock().expect("stream registry lock");
            let stream = streams
                .get(id)
                .cloned()
                .ok_or_else(|| Error::UnknownStream { id: id.to_string() })?;
            // A sealed stream has nothing left to route to it; a live one still
            // needs its entry so `Write` can find it.
            if stream.writer().is_none() {
                streams.remove(id);
            }
            stream
        };
        let chunks = stream
            .reader
            .lock()
            .expect("stream reader lock")
            .take()
            .ok_or_else(|| Error::StreamAborted {
                reason: "the stream is already being read".to_string(),
            })?;
        Ok(StreamReader {
            content_type: stream.content_type.clone(),
            declared_len: stream.declared_len,
            outcome: stream.outcome.clone(),
            chunks,
        })
    }
}

/// The sending half of a stream: chunks out, one at a time, at the receiver's
/// pace.
///
/// Obtained from [`Connection::open_stream`](crate::Connection::open_stream).
/// Every write is a call with a deadline, so a receiver that stops draining
/// surfaces as an error on the write rather than as a hang — the same rule the
/// rest of the bus lives by.
pub struct StreamWriter {
    connection: crate::Connection,
    destination: BusName,
    id: String,
    content_type: Option<String>,
    declared_len: Option<u64>,
    timeout: Duration,
    seq: u64,
    sent: u64,
    finished: bool,
}

impl StreamWriter {
    pub(crate) fn new(
        connection: crate::Connection,
        destination: BusName,
        id: String,
        descriptor: StreamDescriptor,
        timeout: Duration,
    ) -> Self {
        Self {
            connection,
            destination,
            id,
            content_type: descriptor.content_type,
            declared_len: descriptor.total_len,
            timeout,
            seq: 0,
            sent: 0,
            finished: false,
        }
    }

    /// The handle to put in the method body that tells the receiver what these
    /// bytes are for.
    ///
    /// Available before the payload has been written, and that is the intended
    /// order: send the call first, then feed the stream. The window is only a
    /// few megabytes, so a sender that writes everything before making the call
    /// stalls against a reader that does not exist yet.
    pub fn stream_ref(&self) -> StreamRef {
        StreamRef {
            id: self.id.clone(),
            content_type: self.content_type.clone(),
            len: self.declared_len,
        }
    }

    /// How many bytes have been accepted by the receiver so far.
    pub fn sent(&self) -> u64 {
        self.sent
    }

    /// Write `bytes`, splitting them across as many chunks as it takes.
    pub async fn write(&mut self, bytes: &[u8]) -> Result<()> {
        for chunk in bytes.chunks(MAX_CHUNK_LEN) {
            self.write_chunk(chunk).await?;
        }
        Ok(())
    }

    /// Write exactly one chunk, which must be no larger than [`MAX_CHUNK_LEN`].
    ///
    /// There is no "already finished" case to guard against: [`Self::finish`]
    /// and [`Self::abort`] both consume the writer, so the type system has
    /// already ruled out a write after either of them.
    pub async fn write_chunk(&mut self, chunk: &[u8]) -> Result<()> {
        if chunk.len() > MAX_CHUNK_LEN {
            return Err(Error::protocol(format!(
                "chunk of {} bytes exceeds the {MAX_CHUNK_LEN}-byte cap",
                chunk.len()
            )));
        }
        self.call(
            "Write",
            serde_json::json!([self.id, self.seq, base64::encode(chunk)]),
        )
        .await?;
        self.seq += 1;
        self.sent += chunk.len() as u64;
        Ok(())
    }

    /// Close the stream and return the handle, now carrying its final length.
    pub async fn finish(mut self) -> Result<StreamRef> {
        self.call("Close", serde_json::json!([self.id, self.sent]))
            .await?;
        self.finished = true;
        Ok(StreamRef {
            id: self.id.clone(),
            content_type: self.content_type.clone(),
            len: Some(self.sent),
        })
    }

    /// Abandon the stream, telling the receiver not to wait for the rest.
    pub async fn abort(mut self) -> Result<()> {
        self.finished = true;
        self.call("Abort", serde_json::json!([self.id])).await?;
        Ok(())
    }

    async fn call(&self, member: &str, args: Value) -> Result<Value> {
        self.connection
            .call_stream_member(&self.destination, member, args, self.timeout)
            .await
    }
}

impl std::fmt::Debug for StreamWriter {
    /// Deliberately never the payload: a writer is printed in error paths, and
    /// the bytes going through it are the caller's data.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StreamWriter")
            .field("destination", &self.destination)
            .field("id", &self.id)
            .field("sent", &self.sent)
            .finish_non_exhaustive()
    }
}

impl Drop for StreamWriter {
    /// A dropped writer aborts, so a sender that fails halfway does not leave
    /// the receiver holding a window open until the idle reaper notices.
    /// Best-effort by necessity: `Drop` cannot await, and the process may be on
    /// its way out.
    fn drop(&mut self) {
        if self.finished {
            return;
        }
        let (connection, destination, id, timeout) = (
            self.connection.clone(),
            self.destination.clone(),
            self.id.clone(),
            self.timeout,
        );
        if tokio::runtime::Handle::try_current().is_ok() {
            tokio::spawn(async move {
                let _ = connection
                    .call_stream_member(&destination, "Abort", serde_json::json!([id]), timeout)
                    .await;
            });
        }
    }
}

/// The receiving half of a stream: chunks, in order, as they land.
///
/// Reading incrementally is the point — a receiver writing a payload to disk
/// should never hold more than one chunk of it — but
/// [`StreamReader::read_to_end_capped`] is there for the common case where the payload
/// is merely too big for a frame, not too big for memory.
pub struct StreamReader {
    chunks: mpsc::Receiver<Vec<u8>>,
    /// Only the verdict is shared with the receiving connection — deliberately
    /// not the whole stream record, whose drop is what closes this channel.
    outcome: Arc<std::sync::Mutex<Option<Outcome>>>,
    content_type: Option<String>,
    declared_len: Option<u64>,
}

impl std::fmt::Debug for StreamReader {
    /// The metadata, never the buffered chunks.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StreamReader")
            .field("content_type", &self.content_type)
            .field("declared_len", &self.declared_len)
            .finish_non_exhaustive()
    }
}

impl StreamReader {
    /// What the sender said the payload is, if anything.
    pub fn content_type(&self) -> Option<&str> {
        self.content_type.as_deref()
    }

    /// What the sender declared the total length to be, if it declared one.
    pub fn declared_len(&self) -> Option<u64> {
        self.declared_len
    }

    /// The next chunk, or `None` at a clean end of stream.
    ///
    /// Returns an error if the sender aborted, went idle, or closed the stream
    /// short of the length it declared — a truncated payload must never be
    /// mistaken for a complete one.
    pub async fn next_chunk(&mut self) -> Result<Option<Vec<u8>>> {
        if let Some(chunk) = self.chunks.recv().await {
            return Ok(Some(chunk));
        }
        match self.outcome.lock().expect("stream outcome lock").clone() {
            Some(Outcome::Complete) => Ok(None),
            Some(Outcome::Aborted(reason)) => Err(Error::StreamAborted {
                reason: reason.to_string(),
            }),
            // The channel closed with no verdict recorded: the connection that
            // was receiving the stream went away underneath it.
            None => Err(Error::StreamAborted {
                reason: "the connection closed mid-stream".to_string(),
            }),
        }
    }

    /// Drain the whole stream into memory, refusing to exceed `limit` bytes.
    ///
    /// The initial reservation is **not** taken from the sender's declared
    /// length. `declared_len` arrives from the peer before any payload does, so
    /// sizing a buffer from it is a remote-triggered allocation — the same
    /// mistake the frame-length cap in
    /// [`codec`](crate::message::codec) exists to prevent, one layer up. A peer
    /// could declare the maximum on each of its permitted streams and make a
    /// receiver reserve gigabytes for bytes it never intends to send. Reserving
    /// one chunk and letting the vector grow costs an amortised handful of
    /// reallocations on a real transfer and nothing at all on a lie.
    pub async fn read_to_end_capped(&mut self, limit: u64) -> Result<Vec<u8>> {
        let mut out = Vec::with_capacity(limit.min(MAX_CHUNK_LEN as u64) as usize);
        while let Some(chunk) = self.next_chunk().await? {
            if out.len() as u64 + chunk.len() as u64 > limit {
                return Err(Error::StreamTooLarge { limit });
            }
            out.extend_from_slice(&chunk);
        }
        Ok(out)
    }
}

#[cfg(test)]
mod stream_test;
