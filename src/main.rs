use std::collections::HashMap;
use std::convert::Infallible;
use std::net::{AddrParseError, SocketAddr};
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
use tokio::net::{TcpSocket, TcpStream};
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
    sock: TcpStream,
}

type ConnLcoked<'a> = MutexGuard<'a, Conn>;
type ConnShared = Arc<Mutex<Conn>>;

struct Ctx {
    conns: HashMap<String, ConnShared>,
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

impl Conn {
    fn new(sock: TcpStream) -> Self {
        Self { sock }
    }

    fn new_shared(sock: TcpStream) -> ConnShared {
        Arc::new(Mutex::new(Self::new(sock)))
    }
}

impl Ctx {
    fn new() -> Self {
        Self {
            conns: HashMap::default(),
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
        &Method::POST => handle_post(cfg, ctx, addr, req).await,
        _ => Ok(blank_status(StatusCode::METHOD_NOT_ALLOWED)),
    }
}

async fn handle_new_conn<'a>(
    cfg: Arc<Cfg>,
    ctx: CtxShared,
    addr: SocketAddr,
    mut req: Request<Body>,
) -> Result<Response<Body>, Infallible> {
    let d = match get_header_str(req.headers(), "d") {
        Some(h) => h,
        _ => return Ok(blank_status(StatusCode::BAD_REQUEST)),
    };

    let dst: SocketAddr = match d.parse() {
        Ok(a) => a,
        _ => return Ok(blank_status(StatusCode::BAD_REQUEST)),
    };

    let id = match get_header_str(req.headers(), "i") {
        Some(h) => h,
        None => return Ok(blank_status(StatusCode::BAD_REQUEST)),
    }
    .to_owned();

    let sock = match TcpSocket::new_v4() {
        Ok(s) => s,
        _ => return Ok(blank_status(StatusCode::INTERNAL_SERVER_ERROR)),
    };
    match sock.bind(cfg.stream_bind) {
        Ok(s) => s,
        _ => return Ok(blank_status(StatusCode::INTERNAL_SERVER_ERROR)),
    }

    let stream = match sock.connect(dst).await {
        Ok(s) => s,
        _ => return Ok(blank_status(StatusCode::BAD_GATEWAY)),
    };

    let conn = Conn::new_shared(stream);

    let mut written = 0usize;

    'outer: while let Some(chunk_result) = req.body_mut().data().await {
        match chunk_result {
            Ok(chunk) => {
                let mut i = 0usize;
                while i < chunk.len() {
                    match conn.lock().await.sock.write(&chunk[i..]).await {
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

    ctx.lock().await.conns.insert(id, conn);

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
    req: Request<Body>,
) -> Result<Response<Body>, Infallible> {
    if req.headers().contains_key("d") {
        return handle_new_conn(cfg, ctx, addr, req).await;
    };

    let id = match get_header_str(req.headers(), "i") {
        Some(h) => h,
        None => return Ok(blank_status(StatusCode::BAD_REQUEST)),
    };

    let conn = match ctx.lock().await.conns.get_mut(id).map(|c| c.clone()) {
        Some(c) => c,
        None => return Ok(blank_status(StatusCode::NOT_FOUND)),
    };

    Ok(blank_status(StatusCode::NO_CONTENT))
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
