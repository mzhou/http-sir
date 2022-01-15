use std::convert::Infallible;
use std::error::Error;
use std::net::SocketAddr;
use std::sync::Arc;

use clap::Parser;

use hyper::server::conn::AddrStream;
use hyper::service::{make_service_fn, service_fn};
use hyper::{Body, Request, Response, Server};

#[derive(Parser, Debug)]
#[clap()]
struct Args {
    #[clap(long)]
    http_bind: String,
    #[clap(long)]
    stream_bind: String,
}

struct Ctx {}

async fn handle(
    ctx: Arc<Ctx>,
    addr: SocketAddr,
    req: Request<Body>,
) -> Result<Response<Body>, Infallible> {
    Ok(Response::new(Body::from("Hello World")))
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let args = Args::parse();

    let http_bind: SocketAddr = args.http_bind.parse()?;
    let stream_bind: SocketAddr = args.stream_bind.parse()?;

    let ctx = Arc::new(Ctx {});

    let make_service = make_service_fn(move |conn: &AddrStream| {
        let addr = conn.remote_addr();
        let ctx = ctx.clone();
        let service = service_fn(move |req| handle(ctx.clone(), addr, req));
        async move { Ok::<_, Infallible>(service) }
    });

    let server = Server::bind(&http_bind).serve(make_service);

    server.await?;

    Ok(())
}
