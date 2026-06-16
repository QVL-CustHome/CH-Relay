//! Minimal embedded admin/monitoring dashboard — no extra HTTP framework, no
//! separate service. A hand-rolled HTTP/1.1 responder on its own listener serves
//! two routes:
//! - `GET /`       — a self-contained HTML page that polls the stats endpoint;
//! - `GET /stats`  — the broker's [`Stats`] as JSON.
//!
//! Each request is one short read + one `Connection: close` response, which is
//! all a polling dashboard needs.
//!
//! [`Stats`]: crate::hub::Stats

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tracing::warn;

use crate::hub::{Hub, Stats};

/// Accept loop for the dashboard listener. Runs until the process exits.
pub async fn serve(listener: TcpListener, hub: Hub) {
    loop {
        match listener.accept().await {
            Ok((mut socket, _peer)) => {
                let hub = hub.clone();
                tokio::spawn(async move {
                    if let Err(e) = handle(&mut socket, &hub).await {
                        warn!(error = %e, "dashboard request failed");
                    }
                });
            }
            Err(e) => warn!(error = %e, "dashboard accept failed"),
        }
    }
}

async fn handle(socket: &mut TcpStream, hub: &Hub) -> std::io::Result<()> {
    // GET requests are tiny; one read covers the request line + headers.
    let mut buf = [0u8; 2048];
    let n = socket.read(&mut buf).await?;
    if n == 0 {
        return Ok(());
    }
    let request = String::from_utf8_lossy(&buf[..n]);
    let path = request
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .unwrap_or("/");

    let (status, content_type, body) = match path {
        "/" | "/index.html" => ("200 OK", "text/html; charset=utf-8", INDEX_HTML.to_string()),
        "/stats" | "/stats.json" => ("200 OK", "application/json", stats_json(&hub.stats())),
        _ => ("404 Not Found", "text/plain; charset=utf-8", "not found".to_string()),
    };

    let response = format!(
        "HTTP/1.1 {status}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\n\
         Connection: close\r\nCache-Control: no-store\r\n\r\n{body}",
        body.len()
    );
    socket.write_all(response.as_bytes()).await?;
    socket.flush().await
}

/// Hand-serialize [`Stats`] to JSON (a flat object of integers — no serde_json).
fn stats_json(s: &Stats) -> String {
    format!(
        "{{\"clients_online\":{},\"clients_total\":{},\"subscriptions\":{},\
         \"retained\":{},\"dead_letters\":{},\"events\":{},\"next_offset\":{}}}",
        s.clients_online,
        s.clients_total,
        s.subscriptions,
        s.retained,
        s.dead_letters,
        s.events,
        s.next_offset
    )
}

const INDEX_HTML: &str = r#"<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Relay — broker dashboard</title>
<style>
  :root { color-scheme: dark; }
  body { margin: 0; font: 15px/1.5 system-ui, sans-serif; background: #0f1115; color: #e6e6e6; }
  header { padding: 24px 32px; border-bottom: 1px solid #232733; display: flex; align-items: baseline; gap: 12px; }
  h1 { margin: 0; font-size: 20px; letter-spacing: .5px; }
  header .sub { color: #8b93a7; font-size: 13px; }
  .grid { display: grid; grid-template-columns: repeat(auto-fill, minmax(200px, 1fr)); gap: 16px; padding: 32px; }
  .card { background: #171a21; border: 1px solid #232733; border-radius: 10px; padding: 18px 20px; }
  .card .label { color: #8b93a7; font-size: 12px; text-transform: uppercase; letter-spacing: .6px; }
  .card .value { font-size: 32px; font-weight: 600; margin-top: 6px; font-variant-numeric: tabular-nums; }
  footer { padding: 0 32px 32px; color: #6b7280; font-size: 12px; }
  .dot { width: 8px; height: 8px; border-radius: 50%; background: #36c46b; display: inline-block; }
</style>
</head>
<body>
<header>
  <span class="dot"></span>
  <h1>Relay</h1>
  <span class="sub">MQTT 5.0 broker — live monitoring</span>
</header>
<div class="grid" id="grid"></div>
<footer id="footer">connecting…</footer>
<script>
const CARDS = [
  ["clients_online", "Clients online"],
  ["clients_total", "Sessions total"],
  ["subscriptions", "Subscriptions"],
  ["retained", "Retained messages"],
  ["dead_letters", "Dead letters"],
  ["events", "Logged events"],
  ["next_offset", "Next offset"],
];
const grid = document.getElementById("grid");
const footer = document.getElementById("footer");
for (const [key, label] of CARDS) {
  const card = document.createElement("div");
  card.className = "card";
  card.innerHTML = `<div class="label">${label}</div><div class="value" id="v_${key}">–</div>`;
  grid.appendChild(card);
}
async function refresh() {
  try {
    const r = await fetch("/stats", { cache: "no-store" });
    const s = await r.json();
    for (const [key] of CARDS) {
      const el = document.getElementById("v_" + key);
      if (el) el.textContent = s[key];
    }
    footer.textContent = "updated " + new Date().toLocaleTimeString();
  } catch (e) {
    footer.textContent = "broker unreachable — retrying…";
  }
}
refresh();
setInterval(refresh, 2000);
</script>
</body>
</html>
"#;
