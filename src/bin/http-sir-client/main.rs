#![feature(async_closure)]

use std::cmp::min;
use std::collections::{HashMap, HashSet, VecDeque};
use std::convert::Infallible;
use std::net::{AddrParseError, Ipv4Addr, SocketAddr};
use std::str::FromStr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use clap::Parser;
use hyper::body::HttpBody;
use hyper::client::{Client, HttpConnector};
use hyper::header::{AsHeaderName, HeaderMap, HeaderValue};
use hyper::http::Method;
use hyper::service::{make_service_fn, service_fn};
use hyper::{Body, Request, Response, Server, StatusCode};
use hyper_rustls::{HttpsConnector, HttpsConnectorBuilder};
use rustls::{ClientConfig, ALL_CIPHER_SUITES, ALL_KX_GROUPS, ALL_VERSIONS};
use thiserror::Error;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Mutex, MutexGuard};
use tokio::time::sleep;
use uuid::Uuid;

#[derive(Parser, Debug)]
#[clap()]
struct Args {
    #[clap(long)]
    listen: String,
    #[clap(default_value = "[::]:0", long)]
    http_bind: String,
    #[clap(long)]
    url: String,
    #[clap(long)]
    target: String,
}

struct Cfg {
    target: String,
    listen: SocketAddr,
    url: String,
}

type CfgShared = Arc<Cfg>;

struct Conn {
    active_tx: AtomicUsize,
    id: String,
    seq_rx: AtomicUsize,
}

type ConnShared = Arc<Conn>;

struct Ctx {
    client: HttpsClient,
}

type CtxShared = Arc<Ctx>;

type HttpsClient = Client<HttpsConnector<HttpConnector>>;

#[derive(Debug, Error)]
enum MainError {
    #[error(transparent)]
    AddrParse(#[from] AddrParseError),
    #[error(transparent)]
    Hyper(#[from] hyper::Error),
    #[error(transparent)]
    Service(#[from] ServiceError),
}

#[derive(Debug, Error)]
enum ServiceError {
    #[error(transparent)]
    Io(#[from] std::io::Error),
}

impl Ctx {
    fn new() -> Self {
        let mut http_connector = HttpConnector::new();
        http_connector.enforce_http(false);
        http_connector.set_nodelay(true);
        let https = HttpsConnectorBuilder::new()
            .with_tls_config(
                ClientConfig::builder()
                    .with_cipher_suites(&ALL_CIPHER_SUITES)
                    .with_kx_groups(&ALL_KX_GROUPS)
                    .with_protocol_versions(ALL_VERSIONS)
                    .unwrap()
                    .with_custom_certificate_verifier(Arc::new(
                        danger::NoCertificateVerification {},
                    ))
                    .with_no_client_auth(),
            )
            .https_or_http()
            .enable_http1()
            .enable_http2()
            .build();
        let client = Client::builder().build::<_, hyper::Body>(https);
        Self { client }
    }
}

async fn handle(cfg: CfgShared, ctx: CtxShared, mut stream: TcpStream, addr: SocketAddr) -> () {
    let _ = stream.set_nodelay(true);

    let target = if cfg.target == "socks5" {
        let mut buf = [0u8; 255];
        if stream.read_exact(&mut buf[..1]).await.unwrap_or(0) != 1 {
            eprintln!("socks5 couldn't read ver");
            return;
        }
        if buf[0] != 5 {
            eprintln!("socks5 invalid ver");
            return;
        }
        if stream.read_exact(&mut buf[..1]).await.unwrap_or(0) != 1 {
            eprintln!("socks5 couldn't read nmethods");
            return;
        }
        let nmethods = buf[0] as usize;
        if stream.read_exact(&mut buf[..nmethods]).await.unwrap_or(0) != nmethods {
            eprintln!("socks5 couldn't read methods");
            return;
        }
        if buf[..nmethods].iter().find(|m| **m == 0u8) == None {
            eprintln!("socks5 method 0 wasn't offered");
            return;
        }
        if !stream.write_all(&[5u8, 0]).await.is_ok() {
            eprintln!("socks5 couldn't send method");
            return;
        }
        if stream.read_exact(&mut buf[..10]).await.unwrap_or(0) != 10 {
            eprintln!("socks5 couldn't read request");
            return;
        }
        if buf[0] != 5 {
            eprintln!("socks5 incorrect version");
            return;
        }
        if buf[1] != 1 {
            //eprintln!("socks5 incorrect cmd");
            return;
        }
        if buf[2] != 0 {
            eprintln!("socks5 incorrect rsv");
            return;
        }
        // TODO: ipv6 support
        if buf[3] != 1 {
            eprintln!("socks5 incorrect atyp expected 1 got {}", buf[3]);
            return;
        }
        if !stream
            .write_all(&[5u8, 0, 0, 1, 0, 0, 0, 0, 0, 0])
            .await
            .is_ok()
        {
            eprintln!("socks5 couldn't send reply");
            return;
        }
        //eprintln!("socks5 proceeding");
        SocketAddr::new(
            Ipv4Addr::new(buf[4], buf[5], buf[6], buf[7]).into(),
            (buf[8] as u16) << 8 | (buf[9] as u16),
        )
        .to_string()
    } else {
        cfg.target.clone()
    };

    let id = format!("{}", Uuid::new_v4().hyphenated());
    let client = ctx.client.clone();

    eprintln!("{} connecting to {}", id, target);

    let connect_req = Request::builder()
        .header("d", &target)
        .header("i", &id)
        .method(Method::POST)
        .uri(&cfg.url)
        .body(Body::empty())
        .unwrap();
    let connect_res = match client.request(connect_req).await {
        Ok(r) => r,
        Err(e) => {
            eprintln!("{} connect_req {:?}", id, e);
            return;
        }
    };
    if connect_res.status() != StatusCode::CREATED {
        eprintln!("{} connect_res status {}", id, connect_res.status());
        return;
    }

    let conn = ConnShared::new(Conn {
        active_tx: AtomicUsize::default(),
        id: id.clone(),
        seq_rx: AtomicUsize::default(),
    });

    let (mut stream_rx, stream_tx) = stream.into_split();

    tokio::spawn(receive_loop(
        cfg.clone(),
        ctx.clone(),
        conn.clone(),
        stream_tx,
    ));

    let mut seq = 0usize;
    let mut buf = [0u8; 65535];
    while let Ok(bytes_received) = stream_rx.read(&mut buf).await {
        if bytes_received == 0 {
            break;
        }
        let buf_valid = &buf[..bytes_received];
        while conn.active_tx.load(Ordering::Relaxed) >= 4 {
            sleep(Duration::from_millis(5)).await;
        }
        //eprintln!("{} spawn transmit seq {} len {}", &id, seq, buf_valid.len());
        tokio::spawn(transmit(
            cfg.clone(),
            ctx.clone(),
            conn.clone(),
            seq,
            Vec::from(buf_valid),
            false,
        ));
        conn.active_tx.fetch_add(1, Ordering::Relaxed);
        seq += buf_valid.len();
    }

    eprintln!("{} left read loop", id);

    tokio::spawn(transmit(cfg, ctx, conn, seq, Vec::new(), true));

    eprintln!("{} sent fin", id);
}

#[tokio::main]
async fn main() -> Result<(), MainError> {
    let args = Args::parse();

    let listen: SocketAddr = args.listen.parse()?;

    let cfg = Arc::new(Cfg {
        target: args.target,
        listen,
        url: args.url,
    });

    let ctx = Arc::new(Ctx::new());

    service(cfg, ctx).await?;

    Ok(())
}

async fn receive_loop(
    cfg: CfgShared,
    ctx: CtxShared,
    conn: ConnShared,
    mut stream_tx: OwnedWriteHalf,
) -> () {
    let mut seq = 0usize;
    'outer: loop {
        let req = Request::builder()
            .header("a", seq.to_string())
            .header("i", &conn.id)
            .header("s", seq.to_string())
            .method(Method::GET)
            .uri(&cfg.url)
            .body(Body::empty())
            .unwrap();
        let res = match ctx.client.request(req).await {
            Ok(r) => r,
            Err(e) => {
                //eprintln!("receive_loop {:?}", e);
                continue;
            }
        };
        if res.status() != StatusCode::OK {
            //eprintln!("receive_loop {}", res.status());
            break;
        }
        let mut body = res.into_body();
        while let Some(Ok(chunk)) = body.data().await {
            let mut i = 0usize;
            while i < chunk.len() {
                match stream_tx.write(&chunk[i..]).await {
                    Ok(this_written) => {
                        i += this_written;
                        seq += this_written;
                        conn.seq_rx.store(seq, Ordering::Relaxed);
                    }
                    Err(_) => {
                        break 'outer;
                    }
                }
            }
        }
    }
}

async fn service(cfg: CfgShared, ctx: CtxShared) -> Result<(), ServiceError> {
    let listener = TcpListener::bind(cfg.listen).await?;

    loop {
        let (stream, addr) = listener.accept().await?;
        tokio::spawn(handle(cfg.clone(), ctx.clone(), stream, addr));
    }
}

async fn transmit(
    cfg: CfgShared,
    ctx: CtxShared,
    conn: ConnShared,
    seq: usize,
    data: Vec<u8>,
    fin: bool,
) -> () {
    //eprintln!("transmit seq {} len {}", seq, data.len());
    loop {
        let mut req = Request::builder()
            .header("a", conn.seq_rx.load(Ordering::Relaxed))
            .header("i", &conn.id)
            .header("s", seq.to_string())
            .method(Method::POST)
            .uri(&cfg.url)
            .body(Body::from(data.clone()))
            .unwrap();
        if fin {
            req.headers_mut()
                .insert("f", HeaderValue::from_str("1").unwrap());
        }
        let res = match ctx.client.request(req).await {
            Ok(r) => r,
            Err(e) => {
                eprintln!("{} transmit {} {:?}", conn.id, seq, e);
                sleep(Duration::from_millis(5)).await;
                continue;
            }
        };
        if res.status() != StatusCode::OK {
            eprintln!("{} transmit {} status {}", conn.id, seq, res.status());
            if res.status() == StatusCode::RANGE_NOT_SATISFIABLE
                || res.status() == StatusCode::SERVICE_UNAVAILABLE
            {
                sleep(Duration::from_millis(5)).await;
                continue;
            } else {
                break;
            }
        }
        break;
    }

    conn.active_tx.fetch_sub(1, Ordering::Relaxed);
}

mod danger {
    pub struct NoCertificateVerification {}

    impl rustls::client::ServerCertVerifier for NoCertificateVerification {
        fn verify_server_cert(
            &self,
            _end_entity: &rustls::Certificate,
            _intermediates: &[rustls::Certificate],
            _server_name: &rustls::ServerName,
            _scts: &mut dyn Iterator<Item = &[u8]>,
            _ocsp: &[u8],
            _now: std::time::SystemTime,
        ) -> Result<rustls::client::ServerCertVerified, rustls::Error> {
            Ok(rustls::client::ServerCertVerified::assertion())
        }
    }
}
