use std::net::{IpAddr, SocketAddr};

use anyhow::{bail, Context};
use tokio::net::TcpListener;

use redis_starter_rust::{Connection, Server};

struct Config {
    host: IpAddr,
    port: u16,
}

fn parse_args() -> anyhow::Result<Config> {
    let mut args = std::env::args();
    args.next()
        .context("Expected first arg (path of executable)")?;

    let mut config = Config {
        host: "127.0.0.1".parse().unwrap(),
        port: 6379
    };

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--port" => {
                config.port = args
                    .next()
                    .context("Argument port is missing a value")?
                    .parse()
                    .context("Invalid value for port arg")?
            }
            _ => bail!("Unrecognized argument {arg}")
        }
    }
    Ok(config)
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let config = parse_args()?;
    let addr = SocketAddr::new(config.host, config.port);
    let listener = TcpListener::bind(addr).await?;
    let server = Server::new();

    loop {
        match listener.accept().await {
            Ok((mut stream, addr)) => {
                let server = server.clone();
                tokio::spawn(async move {
                    let conn = Connection::new(&mut stream, addr, server);
                    match conn.run_processing_loop().await {
                        Ok(_) => {}
                        Err(err) => eprintln!("Processing of stream failed: {}", err),
                    };
                });
            }
            Err(e) => eprintln!("couldn't get client: {:?}", e),
        }
    }
}
