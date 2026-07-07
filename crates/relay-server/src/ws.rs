//! MQTT-over-WebSocket transport.
//!
//! Browsers (and many mobile stacks) cannot open a raw MQTT TCP socket, so the
//! standard carries MQTT inside WebSocket **binary** frames, negotiated with the
//! `mqtt` subprotocol. This module bridges a [`WebSocketStream`] to the byte-level
//! [`AsyncRead`]/[`AsyncWrite`] that the MQTT [`Codec`](rmqtt_codec::v5::Codec)
//! expects, so [`crate::connection::handle`] runs unchanged over WebSocket.
//!
//! MQTT framing and WebSocket framing are independent: one binary frame may carry
//! several MQTT packets, or a single packet may span frames. We therefore treat
//! the concatenation of all binary-frame payloads as one continuous byte stream
//! and let the MQTT codec re-find packet boundaries.

use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};

use bytes::{Buf, Bytes};
use futures::{Sink, Stream};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio_tungstenite::tungstenite::handshake::server::{ErrorResponse, Request, Response};
use tokio_tungstenite::tungstenite::http::header::{HeaderValue, SEC_WEBSOCKET_PROTOCOL};
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::WebSocketStream;

fn io_err<E: std::error::Error + Send + Sync + 'static>(e: E) -> io::Error {
    io::Error::other(e)
}

/// WebSocket upgrade callback: echo the `mqtt` subprotocol when the client asks
/// for it (mqtt.js, Paho, HiveMQ web clients all send `Sec-WebSocket-Protocol:
/// mqtt`), as the MQTT-over-WS spec requires.
#[allow(clippy::result_large_err)]
pub fn upgrade_callback(req: &Request, mut response: Response) -> Result<Response, ErrorResponse> {
    let wants_mqtt = req
        .headers()
        .get_all(SEC_WEBSOCKET_PROTOCOL)
        .iter()
        .filter_map(|v| v.to_str().ok())
        .flat_map(|v| v.split(','))
        .any(|p| p.trim().eq_ignore_ascii_case("mqtt"));
    if wants_mqtt {
        response
            .headers_mut()
            .insert(SEC_WEBSOCKET_PROTOCOL, HeaderValue::from_static("mqtt"));
    }
    Ok(response)
}

/// Adapts a [`WebSocketStream`] to a byte stream: reads concatenate binary-frame
/// payloads; writes are emitted as binary frames.
pub struct WsByteStream<S> {
    ws: WebSocketStream<S>,
    /// Bytes from a binary frame not yet handed to the reader.
    read_buf: Bytes,
}

impl<S> WsByteStream<S> {
    pub fn new(ws: WebSocketStream<S>) -> Self {
        Self {
            ws,
            read_buf: Bytes::new(),
        }
    }
}

impl<S> AsyncRead for WsByteStream<S>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        loop {
            if !this.read_buf.is_empty() {
                let n = this.read_buf.len().min(buf.remaining());
                buf.put_slice(&this.read_buf[..n]);
                this.read_buf.advance(n);
                return Poll::Ready(Ok(()));
            }
            match Pin::new(&mut this.ws).poll_next(cx) {
                Poll::Ready(Some(Ok(msg))) => match msg {
                    Message::Binary(data) => this.read_buf = data,
                    // A graceful Close maps to EOF.
                    Message::Close(_) => return Poll::Ready(Ok(())),
                    // Ping/Pong are handled inside tungstenite; Text is not valid
                    // for MQTT — ignore and poll for the next frame.
                    _ => {}
                },
                Poll::Ready(Some(Err(e))) => return Poll::Ready(Err(io_err(e))),
                Poll::Ready(None) => return Poll::Ready(Ok(())), // stream ended -> EOF
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

impl<S> AsyncWrite for WsByteStream<S>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        match Pin::new(&mut this.ws).poll_ready(cx) {
            Poll::Ready(Ok(())) => {
                let msg = Message::Binary(Bytes::copy_from_slice(buf));
                match Pin::new(&mut this.ws).start_send(msg) {
                    Ok(()) => Poll::Ready(Ok(buf.len())),
                    Err(e) => Poll::Ready(Err(io_err(e))),
                }
            }
            Poll::Ready(Err(e)) => Poll::Ready(Err(io_err(e))),
            Poll::Pending => Poll::Pending,
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().ws)
            .poll_flush(cx)
            .map_err(io_err)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().ws)
            .poll_close(cx)
            .map_err(io_err)
    }
}
