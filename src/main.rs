#![feature(async_closure)]

use std::cmp::min;
use std::collections::{HashMap, HashSet, VecDeque};
use std::convert::Infallible;
use std::net::{AddrParseError, SocketAddr};
use std::str::FromStr;
use std::sync::Arc;

use clap::Parser;

use hyper::body::HttpBody;
use hyper::header::{AsHeaderName, HeaderMap, HeaderValue};
use hyper::http::Method;
use hyper::server::conn::AddrStream;
use hyper::service::{make_service_fn, service_fn};
use hyper::{Body, Request, Response, Server, StatusCode};
use thiserror::Error;
use tokio::io::AsyncWriteExt;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::TcpSocket;
use tokio::sync::{Mutex, MutexGuard};

#[derive(Parser, Debug)]
#[clap()]
struct Args {
    #[clap(long)]
    http_bind: String,
    #[clap(long)]
    stream_bind: String,
}

struct Cfg {
    stream_bind: SocketAddr,
}

struct Conn {
    rx: Mutex<ConnRx>,
    tx: Mutex<ConnTx>,
}

struct ConnRx {
    buf: VecDeque<u8>,
    seq: usize,
    stream: OwnedReadHalf,
}

struct ConnTx {
    seq: usize,
    stream: OwnedWriteHalf,
}

type ConnShared = Arc<Conn>;

struct Ctx {
    conns: HashMap<String, ConnShared>,
    reserved_ids: HashSet<String>,
}

type CtxLcoked<'a> = MutexGuard<'a, Ctx>;
type CtxShared = Arc<Mutex<Ctx>>;

#[derive(Debug, Error)]
enum MainError {
    #[error(transparent)]
    AddrParse(#[from] AddrParseError),
    #[error(transparent)]
    Hyper(#[from] hyper::Error),
}

impl Ctx {
    fn new() -> Self {
        Self {
            conns: HashMap::default(),
            reserved_ids: HashSet::default(),
        }
    }
}

fn blank_status(s: StatusCode) -> Response<Body> {
    Response::builder().status(s).body(Body::empty()).unwrap()
}

fn get_header_str<K: AsHeaderName>(hm: &HeaderMap<HeaderValue>, key: K) -> Option<&str> {
    hm.get(key)?.to_str().ok()
}

async fn handle(
    cfg: Arc<Cfg>,
    ctx: CtxShared,
    addr: SocketAddr,
    req: Request<Body>,
) -> Result<Response<Body>, Infallible> {
    match req.method() {
        &Method::GET => handle_get(cfg, ctx, addr, req).await,
        &Method::POST => handle_post(cfg, ctx, addr, req).await,
        _ => Ok(blank_status(StatusCode::METHOD_NOT_ALLOWED)),
    }
}

async fn handle_get<'a>(
    cfg: Arc<Cfg>,
    ctx: CtxShared,
    addr: SocketAddr,
    mut req: Request<Body>,
) -> Result<Response<Body>, Infallible> {
    Ok(blank_status(StatusCode::OK))
}

async fn handle_new_conn<'a>(
    cfg: Arc<Cfg>,
    ctx: CtxShared,
    addr: SocketAddr,
    mut req: Request<Body>,
) -> Result<Response<Body>, Infallible> {
    let dst = match get_header_str(req.headers(), "d").map(SocketAddr::from_str) {
        Some(Ok(d)) => d,
        _ => return Ok(blank_status(StatusCode::BAD_REQUEST)),
    };

    let id = match get_header_str(req.headers(), "i") {
        Some(h) => h,
        None => return Ok(blank_status(StatusCode::BAD_REQUEST)),
    }
    .to_owned();

    {
        let mut ctx_locked = ctx.lock().await;
        if ctx_locked.conns.contains_key(&id) {
            return Ok(blank_status(StatusCode::CONFLICT));
        }
        if !ctx_locked.reserved_ids.insert(id.clone()) {
            return Ok(blank_status(StatusCode::CONFLICT));
        }
    }

    let revert = {
        let ctx = ctx.clone();
        async move |id: String| {
            ctx.lock().await.conns.remove(&id);
        }
    };

    let sock = match TcpSocket::new_v4() {
        Ok(s) => s,
        _ => {
            revert(id).await;
            return Ok(blank_status(StatusCode::INTERNAL_SERVER_ERROR));
        }
    };
    match sock.bind(cfg.stream_bind) {
        Ok(s) => s,
        _ => {
            revert(id).await;
            return Ok(blank_status(StatusCode::INTERNAL_SERVER_ERROR));
        }
    }

    let stream = match sock.connect(dst).await {
        Ok(s) => s,
        _ => {
            revert(id).await;
            return Ok(blank_status(StatusCode::BAD_GATEWAY));
        }
    };

    let _ = stream.set_nodelay(true);

    let (stream_rx, mut stream_tx) = stream.into_split();

    let mut written = 0usize;

    'outer: while let Some(chunk_result) = req.body_mut().data().await {
        match chunk_result {
            Ok(chunk) => {
                let mut i = 0usize;
                while i < chunk.len() {
                    match stream_tx.write(&chunk[i..]).await {
                        Ok(this_written) => {
                            i += this_written;
                            written += this_written;
                        }
                        Err(_) => {
                            break 'outer;
                        }
                    }
                }
            }
            Err(_) => {
                break 'outer;
            }
        }
    }

    let conn = Arc::new(Conn {
        rx: Mutex::new(ConnRx {
            buf: VecDeque::default(),
            seq: 0,
            stream: stream_rx,
        }),
        tx: Mutex::new(ConnTx {
            seq: written,
            stream: stream_tx,
        }),
    });

    {
        let mut ctx_locked = ctx.lock().await;
        ctx_locked.reserved_ids.remove(&id);
        ctx_locked.conns.insert(id, conn);
    }

    Ok(Response::builder()
        .status(StatusCode::NO_CONTENT)
        .header("w", written.to_string())
        .body(Body::empty())
        .unwrap())
}

async fn handle_post(
    cfg: Arc<Cfg>,
    ctx: CtxShared,
    addr: SocketAddr,
    mut req: Request<Body>,
) -> Result<Response<Body>, Infallible> {
    if req.headers().contains_key("d") {
        return handle_new_conn(cfg, ctx, addr, req).await;
    };

    let id = match get_header_str(req.headers(), "i") {
        Some(h) => h,
        None => return Ok(blank_status(StatusCode::BAD_REQUEST)),
    };

    let mut seq = match get_header_str(req.headers(), "s").map(|s| usize::from_str_radix(s, 10)) {
        Some(Ok(s)) => s,
        _ => return Ok(blank_status(StatusCode::BAD_REQUEST)),
    };

    let conn = match ctx.lock().await.conns.get_mut(id).map(|c| c.clone()) {
        Some(c) => c,
        None => return Ok(blank_status(StatusCode::NOT_FOUND)),
    };

    let mut conn_tx = conn.tx.lock().await;

    if seq > conn_tx.seq {
        return Ok(blank_status(StatusCode::RANGE_NOT_SATISFIABLE));
    }

    let mut consumed = 0usize; // either skipped as duplicate, or actually written

    'outer: while let Some(chunk_result) = req.body_mut().data().await {
        match chunk_result {
            Ok(chunk) => {
                let mut i = conn_tx.seq - seq;
                let skipped = min(i, chunk.len());
                consumed += skipped;
                while i < chunk.len() {
                    match conn_tx.stream.write(&chunk[i..]).await {
                        Ok(this_written) => {
                            i += this_written;
                            conn_tx.seq += this_written;
                            seq += this_written;
                        }
                        Err(_) => {
                            break 'outer;
                        }
                    }
                }
            }
            Err(_) => {
                break 'outer;
            }
        }
    }

    Ok(Response::builder()
        .status(StatusCode::NO_CONTENT)
        .header("w", consumed.to_string())
        .body(Body::empty())
        .unwrap())
}

#[tokio::main]
async fn main() -> Result<(), MainError> {
    let args = Args::parse();

    let http_bind: SocketAddr = args.http_bind.parse()?;
    let stream_bind: SocketAddr = args.stream_bind.parse()?;

    let cfg = Arc::new(Cfg { stream_bind });

    let ctx = Arc::new(Mutex::new(Ctx::new()));

    let make_service = make_service_fn(move |conn: &AddrStream| {
        let addr = conn.remote_addr();
        let cfg = cfg.clone();
        let ctx = ctx.clone();
        let service = service_fn(move |req| handle(cfg.clone(), ctx.clone(), addr, req));
        async move { Ok::<_, Infallible>(service) }
    });

    let server = Server::bind(&http_bind).serve(make_service);

    server.await?;

    Ok(())
}
