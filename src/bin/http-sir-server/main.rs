#![feature(async_closure)]

use std::cmp::min;
use std::collections::{HashMap, HashSet, VecDeque};
use std::convert::Infallible;
use std::net::{AddrParseError, SocketAddr};
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use clap::Parser;
use hyper::body::{HttpBody, Sender};
use hyper::header::{AsHeaderName, HeaderMap, HeaderValue};
use hyper::http::Method;
use hyper::server::conn::AddrStream;
use hyper::service::{make_service_fn, service_fn};
use hyper::{Body, Request, Response, Server, StatusCode};
use thiserror::Error;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::TcpSocket;
use tokio::sync::watch;
use tokio::sync::Mutex;
use tokio::time::sleep;

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
    rx_pub: watch::Sender<usize>,
    rx_sub: watch::Receiver<usize>,
    tx: Mutex<ConnTx>,
}

struct ConnRx {
    buf: VecDeque<u8>,
    fin: bool,
    seq: usize,
}

struct ConnTx {
    fin: bool,
    seq: usize,
    stream: OwnedWriteHalf,
}

type ConnShared = Arc<Conn>;

struct Ctx {
    conns: HashMap<String, ConnShared>,
    reserved_ids: HashSet<String>,
}

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

async fn drainer(conn: ConnShared, mut sender: Sender, mut seq: usize) {
    let mut rx_sub = conn.rx_sub.clone();
    loop {
        let mut fin;
        let buf: Vec<u8> = {
            let conn_rx = conn.rx.lock().await;
            if seq < conn_rx.seq - conn_rx.buf.len() {
                break;
            }
            fin = conn_rx.fin;
            if seq <= conn_rx.seq {
                conn_rx
                    .buf
                    .range(conn_rx.buf.len() - (conn_rx.seq - seq)..)
                    .map(|b| *b)
                    .collect()
            } else {
                Vec::new()
            }
        };
        seq += buf.len();
        if buf.is_empty() {
            if fin {
                break;
            }
            if let Err(_) = rx_sub.changed().await {
                break;
            }
            rx_sub.borrow_and_update();
            continue;
        }
        if let Err(_) = sender.send_data(buf.into()).await {
            break;
        }
    }
}

// loop that reads from remote and fills buffer
async fn filler(conn: ConnShared, mut stream: OwnedReadHalf) {
    //eprintln!("filler called");
    let mut buf = [0u8; 65535];
    while let Ok(bytes_read) = stream.read(&mut buf).await {
        let valid_buf = &buf[..bytes_read];
        if valid_buf.is_empty() {
            break;
        }
        let mut conn_rx = conn.rx.lock().await;
        conn_rx.buf.extend(valid_buf);
        conn_rx.seq += valid_buf.len();
        let seq = conn_rx.seq;
        drop(conn_rx);
        if let Err(_) = conn.rx_pub.send(seq) {
            break;
        }
        // TODO: wait for buffer space before letting next iteration run
    }
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
    req: Request<Body>,
) -> Result<Response<Body>, Infallible> {
    let id = match get_header_str(req.headers(), "i") {
        Some(h) => h,
        None => return Ok(blank_status(StatusCode::BAD_REQUEST)),
    }
    .to_owned();

    let seq = match get_header_str(req.headers(), "s").map(|s| usize::from_str_radix(s, 10)) {
        Some(Ok(s)) => s,
        _ => return Ok(blank_status(StatusCode::BAD_REQUEST)),
    };

    let conn = match ctx.lock().await.conns.get_mut(&id).map(|c| c.clone()) {
        Some(c) => c,
        None => return Ok(blank_status(StatusCode::NOT_FOUND)),
    };

    let mut fin;

    {
        let conn_rx = conn.rx.lock().await;
        if seq < conn_rx.seq - conn_rx.buf.len() {
            return Ok(blank_status(StatusCode::RANGE_NOT_SATISFIABLE));
        }
        fin = conn_rx.fin;
    }

    let (sender, body) = Body::channel();

    tokio::spawn(drainer(conn, sender, seq));

    let res = Response::builder().status(StatusCode::OK);

    if fin {}

    Ok(res.body(body).unwrap())
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

    let (rx_pub, rx_sub) = watch::channel::<usize>(0);

    let conn = Arc::new(Conn {
        rx: Mutex::new(ConnRx {
            buf: VecDeque::default(),
            fin: false,
            seq: 0,
        }),
        rx_pub,
        rx_sub,
        tx: Mutex::new(ConnTx {
            fin: false,
            seq: written,
            stream: stream_tx,
        }),
    });

    let _ = tokio::spawn({
        let conn = conn.clone();
        let ctx = ctx.clone();
        let id = id.clone();
        async move {
            filler(conn.clone(), stream_rx).await;
            // give some time for the client to send it's own fin
            // but force stop if no data is sent for 2 seconds continuously
            let mut last_seq: Option<usize> = None;
            loop {
                let mut conn_tx = conn.tx.lock().await;
                if conn_tx.fin {
                    break;
                }
                if last_seq == Some(conn_tx.seq) {
                    let _ = conn_tx.stream.shutdown().await;
                    break;
                }
                last_seq = Some(conn_tx.seq);
                drop(conn_tx);
                sleep(Duration::from_millis(2000)).await;
            }
            let mut ctx_locked = ctx.lock().await;
            ctx_locked.conns.remove(&id);
        }
    });

    {
        let mut ctx_locked = ctx.lock().await;
        ctx_locked.reserved_ids.remove(&id);
        ctx_locked.conns.insert(id, conn);
    }

    Ok(Response::builder()
        .status(StatusCode::CREATED)
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

    let ack: usize = get_header_str(req.headers(), "a")
        .map(|a| usize::from_str_radix(a, 10).unwrap_or(0))
        .unwrap_or(0);

    let fin: bool = get_header_str(req.headers(), "f")
        .map(|f| f == "1")
        .unwrap_or(false);

    // trim rx buffer from ack
    {
        let mut conn_rx = conn.rx.lock().await;
        if ack <= conn_rx.seq {
            let bytes_to_keep = conn_rx.seq - ack;
            let old_len = conn_rx.buf.len();
            if bytes_to_keep <= old_len {
                conn_rx.buf.drain(..old_len - bytes_to_keep);
            }
        }
    }

    let mut conn_tx = conn.tx.lock().await;

    //eprintln!("handle_post seq {} expected {}", seq, conn_tx.seq);

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
                //eprintln!("i {} chunk.len {}", i, chunk.len());
                while i < chunk.len() {
                    match conn_tx.stream.write(&chunk[i..]).await {
                        Ok(this_written) => {
                            //eprintln!("this_written {}", this_written);
                            consumed += this_written;
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

    // if we got fin, and we managed to catch up, shutdown the writer
    if fin && seq == conn_tx.seq {
        let _ = conn_tx.stream.shutdown().await;
        //seq += 1;
        conn_tx.fin = true;
        conn_tx.seq += 1;
    }

    Ok(Response::builder()
        .status(StatusCode::OK)
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

    let server = Server::bind(&http_bind)
        .tcp_nodelay(true)
        .serve(make_service);

    server.await?;

    Ok(())
}
