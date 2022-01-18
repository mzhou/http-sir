#![feature(async_closure)]

use std::cmp::min;
use std::collections::{HashMap, HashSet, VecDeque};
use std::convert::Infallible;
use std::net::{AddrParseError, SocketAddr};
use std::str::FromStr;
use std::sync::Arc;

use clap::Parser;
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
}

type CfgShared = Arc<Cfg>;

struct Ctx {
    client: HttpsClient,
}

type CtxShared = Arc<Mutex<Ctx>>;

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
    let client = ctx.lock().await.client.clone();
}

#[tokio::main]
async fn main() -> Result<(), MainError> {
    let args = Args::parse();

    let listen: SocketAddr = args.listen.parse()?;

    let cfg = Arc::new(Cfg {
        connect: args.connect,
        listen,
    });

    let ctx = Arc::new(Mutex::new(Ctx::new()));

    service(cfg, ctx).await?;

    Ok(())
}

async fn service(cfg: CfgShared, ctx: CtxShared) -> Result<(), ServiceError> {
    let listener = TcpListener::bind(cfg.listen).await?;

    loop {
        let (stream, addr) = listener.accept().await?;
        tokio::spawn(handle(cfg.clone(), ctx.clone(), stream, addr));
    }
}
