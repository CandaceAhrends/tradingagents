use anyhow::{bail, ensure, Context, Result};
use chrono::{Datelike, NaiveDate, Weekday};
use futures_util::{SinkExt, StreamExt};
use redis::aio::MultiplexedConnection;
use rustc_hash::{FxHashMap, FxHashSet};
use serde::{Deserialize, Serialize};
use std::env;
use std::net::SocketAddr;
use std::time::Duration;
use tokio::net::TcpStream;
use tokio_native_tls::native_tls::TlsConnector;
use tokio_tungstenite::tungstenite::handshake::client::Request;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{client_async, MaybeTlsStream, WebSocketStream};
use url::Url;

const PUBLISH_EVERY: Duration = Duration::from_secs(1);
const SUMMARY_EVERY_TICKS: u64 = 10;

/// Massive sometimes sends counts in float form (e.g. block prints with `"z":4e+06`),
/// so read them as f64. Exact for any count below 2^53.
fn count<'de, D: serde::Deserializer<'de>>(d: D) -> Result<u64, D::Error> {
    f64::deserialize(d).map(|x| x as u64)
}

// final format sent to redis should be 
// {
//         "rank": 2,
//         "symbol": "APLD",
//         "price": 23.51,
//         "day_volume": 10728745,
//         "dollar_volume": 252233867.82,
//         "day_vwap": 24.011,
//         "ema_5": 23.7570,
//         "ema_9": 23.8724,
//         "ema_21": 24.0301,
//         "ema_55": 24.1201,
//         "ema_100": 24.1389,
//         "ema_200": 24.1466,
//         "volume_ratio": 1.7847,
//         "bullish_stack": false
//     }


#[derive(Debug, Deserialize)]
pub struct AggregateBar {
    pub ev: String,
    pub sym: String,
    #[serde(deserialize_with = "count")]
    pub v: u64,
    #[serde(default, deserialize_with = "count")]
    pub av: u64,
    #[serde(default)]
    pub op: f64,
    #[serde(default)]
    pub vw: f64,
    pub o: f64,
    pub c: f64,
    pub h: f64,
    pub l: f64,
    #[serde(default)]
    pub a: f64,
    #[serde(default, deserialize_with = "count")]
    pub z: u64,
    pub s: u64,
    pub e: u64,
    #[serde(default)]
    pub otc: bool,
}

#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct FastBar {
    pub start_ms: u64,
    pub end_ms: u64,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub vwap: f64,
    pub volume: u64,
    pub day_volume: u64,
    pub trade_count: u64,
}

impl From<&AggregateBar> for FastBar {
    #[inline(always)]
    fn from(bar: &AggregateBar) -> Self {
        Self {
            start_ms: bar.s,
            end_ms: bar.e,
            open: bar.o,
            high: bar.h,
            low: bar.l,
            close: bar.c,
            vwap: bar.vw,
            volume: bar.v,
            day_volume: bar.av,
            trade_count: bar.z,
        }
    }
}

/// One ticker in the per-second top N snapshot written to the Redis Stream.
#[derive(Serialize)]
struct Row<'a> {
    rank: usize,
    sym: &'a str,
    close: f64,
    open: f64,
    high: f64,
    low: f64,
    vwap: f64,
    volume: u64,
    day_volume: u64,
    bar_end_ms: u64,
}

#[derive(Clone)]
struct DbConfig {
    host: String,
    port: u16,
    user: String,
    password: String,
    name: String,
}

impl DbConfig {
    fn from_env() -> Result<Self> {
        Ok(Self {
            host: env::var("DB_HOST").unwrap_or_else(|_| "localhost".to_string()),
            port: env_parse("DB_PORT", 5432u16)?,
            user: env::var("DB_USER").context("DB_USER must be set in .env")?,
            password: env::var("DB_PASSWORD").unwrap_or_default(),
            name: env::var("DB_NAME").context("DB_NAME must be set in .env")?,
        })
    }

    async fn connect(&self) -> Result<tokio_postgres::Client> {
        let (client, connection) = tokio_postgres::Config::new()
            .host(&self.host)
            .port(self.port)
            .user(&self.user)
            .password(&self.password)
            .dbname(&self.name)
            .connect(tokio_postgres::NoTls)
            .await?;
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("[db] connection error: {e}");
            }
        });
        Ok(client)
    }
}

struct Config {
    api_key: String,
    debug_mock: bool,
    ws_url: String,
    top_n: usize,
    min_close: f64,
    run_seconds: u64,
    db: DbConfig,
    redis_url: String,
    stream: String,
    stream_maxlen: u64,
}

impl Config {
    fn from_env() -> Result<Self> {
        let debug_mock = env_parse("DEBUG_MOCK", false)?;
        let ws_url = if debug_mock {
            env::var("MASSIVE_MOCK_BASE_URL").unwrap_or_else(|_| "ws://127.0.0.1:8080".to_string())
        } else {
            env::var("MASSIVE_WS_BASE_URL").unwrap_or_else(|_| "wss://delayed.massive.com/stocks".to_string())
        };
        Ok(Self {
            api_key: env::var("MASSIVE_API_KEY").context("MASSIVE_API_KEY must be set in .env")?,
            debug_mock,
            ws_url,
            top_n: env_parse("TOP_N", 50usize)?,
            min_close: env_parse("MIN_CLOSE", 5.0f64)?,
            run_seconds: env_parse("RUN_SECONDS", 0u64)?,
            db: DbConfig::from_env()?,
            redis_url: env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379".to_string()),
            stream: env::var("REDIS_STREAM").unwrap_or_else(|_| "top50:stream".to_string()),
            // ~1 hour of one-per-second entries.
            stream_maxlen: env_parse("REDIS_STREAM_MAXLEN", 3600u64)?,
        })
    }
}

fn env_parse<T: std::str::FromStr>(name: &str, default: T) -> Result<T> {
    match env::var(name) {
        Ok(v) => v
            .trim()
            .parse()
            .map_err(|_| anyhow::anyhow!("{name} has an invalid value: {v}")),
        Err(_) => Ok(default),
    }
}

/// Pins the main thread to core 0. On Linux this is a hard pin (sched_setaffinity);
/// macOS on Apple Silicon does not support pinning, so it returns false there.
fn pin_main() {
    match core_affinity::get_core_ids().and_then(|c| c.first().copied()) {
        Some(c) if core_affinity::set_for_current(c) => println!("[pin] main -> core {}", c.id),
        Some(c) => println!("[pin] main -> core {} not supported by this OS, running unpinned", c.id),
        None => println!("[pin] main -> no core available, running unpinned"),
    }
}

/// Exchange-traded product types: ETF, ETN (e.g. GDXU), ETV (trusts like SLV, GLD, BITB)
/// and ETS (single-stock ETFs like MSTY, TSLY).
const EXCLUDED_TYPES: [&str; 4] = ["ETF", "ETN", "ETV", "ETS"];

async fn api_get(client: &reqwest::Client, api_key: &str, url: &str) -> Result<serde_json::Value> {
    let u = reqwest::Url::parse(url)?;
    ensure!(
        u.scheme() == "https" && u.host_str() == Some("api.massive.com"),
        "Untrusted API URL"
    );
    Ok(client
        .get(u)
        .bearer_auth(api_key)
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?)
}

/// Every active exchange-traded product from Massive's reference API; the stream has no ETF flag.
async fn load_etf_symbols(client: &reqwest::Client, api_key: &str) -> Result<FxHashSet<String>> {
    let mut etfs = FxHashSet::default();
    for ty in EXCLUDED_TYPES {
        let mut url = Some(format!(
            "https://api.massive.com/v3/reference/tickers?type={ty}&market=stocks&active=true&limit=1000"
        ));
        let mut pages = 0;
        while let Some(page) = url {
            pages += 1;
            ensure!(pages <= 50, "Excessive ETF pagination");
            let v = api_get(client, api_key, &page).await?;
            if let Some(rows) = v["results"].as_array() {
                etfs.extend(rows.iter().filter_map(|r| r["ticker"].as_str().map(str::to_owned)));
            }
            url = v["next_url"].as_str().map(str::to_owned);
        }
    }
    Ok(etfs)
}

/// Seeds the latest-bar map from the full-market snapshot (OTC excluded by the
/// endpoint) so the first published top N is complete.
async fn load_snapshot(
    client: &reqwest::Client,
    cfg: &Config,
    etfs: &FxHashSet<String>,
) -> Result<FxHashMap<String, FastBar>> {
    let v = api_get(
        client,
        &cfg.api_key,
        "https://api.massive.com/v2/snapshot/locale/us/markets/stocks/tickers",
    )
    .await?;
    let mut latest = FxHashMap::default();
    for t in v["tickers"].as_array().into_iter().flatten() {
        let Some(sym) = t["ticker"].as_str() else { continue };
        let (min, day) = (&t["min"], &t["day"]);
        let f = |k: &str| min[k].as_f64().filter(|x| *x > 0.0).or(day[k].as_f64()).unwrap_or(0.0);
        let close = f("c");
        let day_volume = min["av"].as_f64().or(day["v"].as_f64()).unwrap_or(0.0) as u64;
        if close > cfg.min_close && day_volume > 0 && !etfs.contains(sym) {
            let start_ms = min["t"].as_u64().unwrap_or(0);
            latest.insert(
                sym.to_owned(),
                FastBar {
                    start_ms,
                    end_ms: start_ms,
                    open: f("o"),
                    high: f("h"),
                    low: f("l"),
                    close,
                    vwap: f("vw"),
                    volume: min["v"].as_f64().unwrap_or(0.0) as u64,
                    day_volume,
                    trade_count: min["n"].as_u64().unwrap_or(0),
                },
            );
        }
    }
    Ok(latest)
}

/// Makes sure `prev_close` holds the last trading day before today (New York time),
/// filling it from Massive's grouped daily bars if missing. Returns that date.
async fn ensure_prev_close(client: &reqwest::Client, cfg: &Config) -> Result<NaiveDate> {
    let db = cfg.db.connect().await.context("Postgres connect failed")?;
    db.batch_execute(
        "CREATE TABLE IF NOT EXISTS prev_close (
            trade_date date NOT NULL,
            ticker varchar(16) NOT NULL,
            close double precision NOT NULL,
            PRIMARY KEY (trade_date, ticker)
        )",
    )
    .await?;

    let today = chrono::Utc::now().with_timezone(&chrono_tz::America::New_York).date_naive();
    let mut day = today;
    for _ in 0..10 {
        day = day.pred_opt().context("date underflow")?;
        if matches!(day.weekday(), Weekday::Sat | Weekday::Sun) {
            continue;
        }
        let have: i64 = db
            .query_one("SELECT count(*) FROM prev_close WHERE trade_date = $1", &[&day])
            .await?
            .get(0);
        if have > 0 {
            println!("Prev close: {have} tickers for {day} already in Postgres");
            return Ok(day);
        }
        let v = api_get(
            client,
            &cfg.api_key,
            &format!("https://api.massive.com/v2/aggs/grouped/locale/us/market/stocks/{day}?adjusted=true"),
        )
        .await?;
        let (mut tickers, mut closes) = (Vec::new(), Vec::new());
        for r in v["results"].as_array().into_iter().flatten() {
            if let (Some(t), Some(c)) = (r["T"].as_str(), r["c"].as_f64()) {
                tickers.push(t.to_owned());
                closes.push(c);
            }
        }
        if tickers.is_empty() {
            continue; // market holiday
        }
        let inserted = db
            .execute(
                "INSERT INTO prev_close (trade_date, ticker, close)
                 SELECT $1, t, c FROM UNNEST($2::text[], $3::float8[]) AS u(t, c)
                 ON CONFLICT DO NOTHING",
                &[&day, &tickers, &closes],
            )
            .await?;
        println!("Prev close: stored {inserted} tickers for {day} from Massive");
        return Ok(day);
    }
    bail!("No trading day with closes found in the 10 days before {today}")
}

/// The top `n` tickers by day volume, highest first. Partial select, then sorts only the n kept.
fn rank_top(latest: &FxHashMap<String, FastBar>, n: usize) -> Vec<(&String, &FastBar)> {
    let by_volume = |a: &(&String, &FastBar), b: &(&String, &FastBar)| b.1.day_volume.cmp(&a.1.day_volume);
    let mut ranked: Vec<(&String, &FastBar)> = latest.iter().collect();
    if ranked.len() > n {
        ranked.select_nth_unstable_by(n, by_volume);
        ranked.truncate(n);
    }
    ranked.sort_unstable_by(by_volume);
    ranked
}

/// Appends the top N, sorted by day volume, as one stream entry. Returns the tickers published.
async fn publish_top(
    con: &mut MultiplexedConnection,
    cfg: &Config,
    latest: &FxHashMap<String, FastBar>,
    prev_date: &str,
) -> Result<Vec<(String, u64)>> {
    let ranked = rank_top(latest, cfg.top_n);

    let rows: Vec<Row> = ranked
        .iter()
        .enumerate()
        .map(|(i, (sym, b))| Row {
            rank: i + 1,
            sym,
            close: b.close,
            open: b.open,
            high: b.high,
            low: b.low,
            vwap: b.vwap,
            volume: b.volume,
            day_volume: b.day_volume,
            bar_end_ms: b.end_ms,
        })
        .collect();
    let ts = chrono::Utc::now().timestamp_millis();
    redis::cmd("XADD")
        .arg(&cfg.stream)
        .arg("MAXLEN")
        .arg("~")
        .arg(cfg.stream_maxlen)
        .arg("*")
        .arg("ts")
        .arg(ts)
        .arg("prev_date")
        .arg(prev_date)
        .arg("data")
        .arg(serde_json::to_string(&rows)?)
        .query_async::<String>(con)
        .await?;
    Ok(ranked.iter().map(|(s, b)| ((*s).clone(), b.day_volume)).collect())
}

/// Connects and completes the websocket handshake: TLS for `wss`, plain TCP for `ws`.
async fn connect_ws(ws_url: &str) -> Result<WebSocketStream<MaybeTlsStream<TcpStream>>> {
    let url = Url::parse(ws_url)?;
    let scheme = url.scheme();
    ensure!(scheme == "ws" || scheme == "wss", "Unsupported websocket scheme: {scheme}");
    let host = url.host_str().context("URL must contain host")?;
    let port = url.port_or_known_default().unwrap_or(443);

    let target_addr: SocketAddr = tokio::net::lookup_host(format!("{host}:{port}"))
        .await?
        .next()
        .context("Could not resolve host")?;
    let tcp_stream = TcpStream::connect(target_addr).await?;
    // Disable Nagle's algorithm so small frames are sent immediately.
    tcp_stream.set_nodelay(true)?;

    let stream = if scheme == "wss" {
        let tls_connector = tokio_native_tls::TlsConnector::from(TlsConnector::builder().build()?);
        MaybeTlsStream::NativeTls(tls_connector.connect(host, tcp_stream).await?)
    } else {
        MaybeTlsStream::Plain(tcp_stream)
    };

    let request = Request::builder()
        .uri(url.as_str())
        .header("Host", host)
        .header("Connection", "Upgrade")
        .header("Upgrade", "websocket")
        .header("Sec-WebSocket-Version", "13")
        .header(
            "Sec-WebSocket-Key",
            tokio_tungstenite::tungstenite::handshake::client::generate_key(),
        )
        .body(())?;
    let (ws_stream, response) = client_async(request, stream).await?;
    println!("Handshake successful (Status: {})", response.status());
    Ok(ws_stream)
}

/// Filter (no OTC, no ETFs, close > min_close) and keep each ticker's latest bar.
fn apply_bars(
    latest: &mut FxHashMap<String, FastBar>,
    bars: &[AggregateBar],
    min_close: f64,
    etfs: &FxHashSet<String>,
) {
    for bar in bars {
        if bar.otc || bar.c <= min_close || etfs.contains(&bar.sym) {
            latest.remove(&bar.sym);
        } else if let Some(slot) = latest.get_mut(&bar.sym) {
            *slot = FastBar::from(bar);
        } else {
            latest.insert(bar.sym.clone(), FastBar::from(bar));
        }
    }
}

async fn run(cfg: &Config) -> Result<()> {
    if cfg.debug_mock {
        println!("[mock] DEBUG_MOCK=true, using mock websocket at {}", cfg.ws_url);
    }
    let client = reqwest::Client::new();
    let etfs = load_etf_symbols(&client, &cfg.api_key).await?;
    println!("Loaded {} ETF/ETN/ETV/ETS tickers to exclude", etfs.len());
    // Latest bar for every ticker that passes the filters, ranked once per second.
    let mut latest = if cfg.debug_mock {
        // Real tickers would never update on the mock feed but would keep their top N slots.
        println!("[mock] skipping snapshot seed");
        FxHashMap::default()
    } else {
        match load_snapshot(&client, cfg, &etfs).await {
            Ok(m) => {
                println!("Seeded {} tickers from snapshot", m.len());
                m
            }
            Err(e) => {
                println!("[warn] snapshot seed failed ({e}); top {} fills in as bars arrive", cfg.top_n);
                FxHashMap::default()
            }
        }
    };
    let prev_date = ensure_prev_close(&client, cfg).await?.to_string();

    let mut redis_con = redis::Client::open(cfg.redis_url.as_str())?
        .get_multiplexed_async_connection()
        .await
        .context("Redis connect failed")?;
    println!("Publishing top {} to Redis stream {} every second", cfg.top_n, cfg.stream);

    let ws_stream = connect_ws(&cfg.ws_url).await?;

    let (mut write, mut read) = ws_stream.split();
    // The mock connection is unencrypted, so the real key never goes over it.
    let ws_key = if cfg.debug_mock { "mock-key" } else { cfg.api_key.as_str() };
    let auth_msg = format!(r#"{{"action":"auth","params":"{ws_key}"}}"#);
    write.send(Message::Text(auth_msg.into())).await?;
    write
        .send(Message::Text(r#"{"action":"subscribe","params":"A.*"}"#.into()))
        .await?;
    println!("Auth and subscription (A.*) sent. Waiting for stream...\n");

    let mut ticker = tokio::time::interval(PUBLISH_EVERY);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let deadline = (cfg.run_seconds > 0).then(|| tokio::time::Instant::now() + Duration::from_secs(cfg.run_seconds));
    let (mut ticks, mut bars_seen, mut published) = (0u64, 0u64, 0u64);

    loop {
        let msg = tokio::select! {
            m = read.next() => m,
            _ = ticker.tick() => {
                ticks += 1;
                let top = publish_top(&mut redis_con, cfg, &latest, &prev_date).await?;
                published += 1;
                if ticks % SUMMARY_EVERY_TICKS == 0 {
                    let leaders: Vec<String> = top.iter().take(5).map(|(s, v)| format!("{s}:{v}")).collect();
                    println!(
                        "[main] {} bars/s in | tracking {} tickers | published {} entries | top {} cutoff vol {} | {}",
                        bars_seen / SUMMARY_EVERY_TICKS,
                        latest.len(),
                        published,
                        top.len(),
                        top.last().map_or(0, |t| t.1),
                        leaders.join(", ")
                    );
                    bars_seen = 0;
                }
                continue;
            }
            _ = tokio::signal::ctrl_c() => { println!("\nCtrl-C, shutting down"); break; }
            _ = async { tokio::time::sleep_until(deadline.unwrap()).await }, if deadline.is_some() => {
                println!("\nRUN_SECONDS reached, shutting down");
                break;
            }
        };
        let mut bytes = match msg {
            Some(Ok(Message::Text(text))) => text.as_bytes().to_vec(),
            Some(Ok(Message::Binary(bin))) => bin.to_vec(),
            Some(Ok(Message::Close(frame))) => bail!("Connection closed: {frame:?}"),
            Some(Ok(_)) => continue,
            Some(Err(e)) => bail!("Socket error: {e}"),
            None => bail!("Stream ended"),
        };

        let bars = match simd_json::from_slice::<Vec<AggregateBar>>(&mut bytes) {
            Ok(bars) => bars,
            Err(e) => {
                let text = String::from_utf8_lossy(&bytes);
                if text.contains(r#""ev":"status""#) {
                    println!("[MASSIVE EVENT]: {text}");
                } else {
                    eprintln!("[UNPARSED] {e}: {text}");
                }
                continue;
            }
        };

        bars_seen += bars.len() as u64;
        apply_bars(&mut latest, &bars, cfg.min_close, &etfs);
    }
    Ok(())
}

fn main() -> Result<()> {
    dotenvy::dotenv().ok();
    let cfg = Config::from_env()?;
    pin_main();
    // Single-threaded runtime so all socket work stays on main's core.
    let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build()?;
    runtime.block_on(run(&cfg))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bar(day_volume: u64) -> FastBar {
        FastBar {
            start_ms: 0,
            end_ms: 0,
            open: 0.0,
            high: 0.0,
            low: 0.0,
            close: 10.0,
            vwap: 0.0,
            volume: 0,
            day_volume,
            trade_count: 0,
        }
    }

    #[test]
    fn rank_top_keeps_highest_volume_sorted() {
        let latest: FxHashMap<String, FastBar> = (1..=70u64).map(|i| (format!("T{i}"), bar(i * 100))).collect();
        let top = rank_top(&latest, 50);
        assert_eq!(top.len(), 50);
        assert_eq!(top[0].0, "T70");
        assert_eq!(top[49].0, "T21");
        assert!(top.windows(2).all(|w| w[0].1.day_volume >= w[1].1.day_volume));
    }

    #[test]
    fn parses_float_encoded_counts() {
        // Real block-print message that previously failed to parse.
        let mut raw = br#"[{"ev":"A","sym":"PEP","v":2500000,"av":18394294,"op":136.58,"vw":137.58,"o":137.58,"c":137.58,"h":137.58,"l":137.58,"a":137.5194,"z":2.5e+06,"s":1793390981000,"e":1793390982000,"dv":"2500000.0","dav":"18394294.527489"}]"#.to_vec();
        let bars = simd_json::from_slice::<Vec<AggregateBar>>(&mut raw).unwrap();
        assert_eq!(bars[0].z, 2_500_000);
        assert_eq!(bars[0].av, 18_394_294);
    }

    #[test]
    fn rank_top_with_fewer_than_n() {
        let latest: FxHashMap<String, FastBar> = (1..=3u64).map(|i| (format!("T{i}"), bar(i))).collect();
        let top: Vec<&str> = rank_top(&latest, 50).iter().map(|(s, _)| s.as_str()).collect();
        assert_eq!(top, vec!["T3", "T2", "T1"]);
    }

    fn agg(sym: &str, close: f64, day_volume: u64) -> serde_json::Value {
        serde_json::json!({
            "ev": "A", "sym": sym, "v": 1000, "av": day_volume, "op": close, "vw": close,
            "o": close, "c": close, "h": close, "l": close, "a": close, "z": 2.5e6,
            "s": 1793390981000u64, "e": 1793390982000u64,
        })
    }

    /// Minimal stand-in for Massive: connected, auth, subscribe, then two batches of bars.
    async fn mock_massive(listener: tokio::net::TcpListener) {
        let (tcp, _) = listener.accept().await.unwrap();
        let mut ws = tokio_tungstenite::accept_async(tcp).await.unwrap();
        let status = |status: &str, message: &str| {
            Message::Text(serde_json::json!([{"ev": "status", "status": status, "message": message}]).to_string().into())
        };

        ws.send(status("connected", "Connected Successfully")).await.unwrap();

        let auth = ws.next().await.unwrap().unwrap().into_text().unwrap();
        println!("[mock server] received {auth}");
        assert_eq!(auth.as_str(), r#"{"action":"auth","params":"mock-key"}"#);
        ws.send(status("auth_success", "authenticated")).await.unwrap();

        let sub = ws.next().await.unwrap().unwrap().into_text().unwrap();
        println!("[mock server] received {sub}");
        assert_eq!(sub.as_str(), r#"{"action":"subscribe","params":"A.*"}"#);
        ws.send(status("success", "subscribed to: A.*")).await.unwrap();

        let mut otc = agg("OTCX", 20.0, 9_000_000);
        otc["otc"] = true.into();
        let batches = [
            vec![
                agg("AAPL", 200.0, 3_000),
                agg("MSFT", 400.0, 2_000),
                agg("NVDA", 100.0, 1_000),
                agg("SPY", 500.0, 8_000_000), // ETF
                agg("PENY", 4.5, 7_000_000),  // under min close
                otc,
            ],
            // NVDA takes the lead, so its latest bar must replace the first one.
            vec![agg("NVDA", 101.0, 5_000)],
        ];
        for batch in batches {
            ws.send(Message::Text(serde_json::to_string(&batch).unwrap().into())).await.unwrap();
        }
        ws.close(None).await.unwrap();
    }

    #[tokio::test]
    async fn mock_feed_end_to_end() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let server = tokio::spawn(mock_massive(listener));

        let ws = connect_ws(&format!("ws://127.0.0.1:{port}")).await.unwrap();
        let (mut write, mut read) = ws.split();
        write
            .send(Message::Text(r#"{"action":"auth","params":"mock-key"}"#.into()))
            .await
            .unwrap();
        write
            .send(Message::Text(r#"{"action":"subscribe","params":"A.*"}"#.into()))
            .await
            .unwrap();

        let etfs: FxHashSet<String> = ["SPY".to_string()].into_iter().collect();
        let mut latest = FxHashMap::default();
        let (mut statuses, mut batches) = (0, 0);
        while let Some(msg) = read.next().await {
            let Message::Text(text) = msg.unwrap() else { continue };
            println!("[client] received {text}");
            let mut bytes = text.as_bytes().to_vec();
            match simd_json::from_slice::<Vec<AggregateBar>>(&mut bytes) {
                Ok(bars) => {
                    batches += 1;
                    apply_bars(&mut latest, &bars, 5.0, &etfs);
                }
                Err(_) => {
                    assert!(text.contains(r#""ev":"status""#), "unparsed non-status frame: {text}");
                    statuses += 1;
                }
            }
        }
        server.await.unwrap();

        assert_eq!((statuses, batches), (3, 2));
        let top: Vec<&str> = rank_top(&latest, 50).iter().map(|(s, _)| s.as_str()).collect();
        assert_eq!(top, vec!["NVDA", "AAPL", "MSFT"]);
        assert_eq!(latest["NVDA"].close, 101.0);
        assert_eq!(latest["AAPL"].trade_count, 2_500_000);
    }

    #[tokio::test]
    async fn connect_ws_rejects_other_schemes() {
        let err = connect_ws("http://127.0.0.1:8080").await.unwrap_err();
        assert!(err.to_string().contains("Unsupported websocket scheme"));
    }
}
