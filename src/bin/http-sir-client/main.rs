#![feature(async_closure)]

use std::cmp::min;
use std::collections::{HashMap, HashSet, VecDeque};
use std::convert::Infallible;
use std::net::{AddrParseError, SocketAddr};
use std::str::FromStr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use clap::Parser;
use hyper::body::HttpBody;
use hyper::client::{Client, HttpConnector};
use hyper::header::{AsHeaderName, HeaderMap, HeaderValue};
use hyper::http::Method;
use hyper::service::{make_service_fn, service_fn};
use hyper::{Body, Request, Response, Server, StatusCode};
use hyper_tls::HttpsConnector;
use thiserror::Error;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Mutex, MutexGuard};
use uuid::Uuid;

#[derive(Parser, Debug)]
#[clap()]
struct Args {
    #[clap(long)]
    connect: String,
    #[clap(long)]
    listen: String,
    #[clap(default_value = "[::]:0", long)]
    http_bind: String,
    #[clap(long)]
    url: String,
}

struct Cfg {
    connect: String,
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
        Self {
            client: Client::builder().build(HttpsConnector::new()),
        }
    }
}

async fn handle(cfg: CfgShared, ctx: CtxShared, stream: TcpStream, addr: SocketAddr) -> () {
    let id = format!("{}", Uuid::new_v4().to_hyphenated_ref());
    let client = ctx.client.clone();

    let connect_req = Request::builder()
        .header("d", &cfg.connect)
        .header("i", &id)
        .method(Method::POST)
        .uri(&cfg.url)
        .body(Body::empty())
        .unwrap();
    let connect_res = match client.request(connect_req).await {
        Ok(r) => r,
        Err(e) => {
            //eprintln!("connect_req {:?}", e);
            return;
        }
    };
    if connect_res.status() != StatusCode::OK {
        //eprintln!("connect_req {}", connect_res.status());
    }

    let conn = ConnShared::new(Conn {
        active_tx: AtomicUsize::default(),
        id,
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
        let buf_valid = &buf[..bytes_received];
        tokio::spawn(transmit(
            cfg.clone(),
            ctx.clone(),
            conn.clone(),
            seq,
            Vec::from(buf_valid),
        ));
        seq += buf_valid.len();
    }
}

#[tokio::main]
async fn main() -> Result<(), MainError> {
    let args = Args::parse();

    let listen: SocketAddr = args.listen.parse()?;

    let cfg = Arc::new(Cfg {
        connect: args.connect,
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
) -> () {
    //eprintln!("transmit seq {} len {}", seq, data.len());
    loop {
        let req = Request::builder()
            .header("a", conn.seq_rx.load(Ordering::Relaxed))
            .header("i", &conn.id)
            .header("s", seq.to_string())
            .method(Method::POST)
            .uri(&cfg.url)
            .body(Body::from(data.clone()))
            .unwrap();
        let res = match ctx.client.request(req).await {
            Ok(r) => r,
            Err(e) => {
                //eprintln!("transmit {:?}", e);
                continue;
            }
        };
        if res.status() != StatusCode::OK {
            //eprintln!("transmit {}", res.status());
            if res.status() == StatusCode::RANGE_NOT_SATISFIABLE {
                continue;
            } else {
                break;
            }
        }
        //eprintln!("transmit ok");
        break;
    }
}
