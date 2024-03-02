use std::net::{IpAddr, SocketAddr};

use anyhow::{bail, Context};
use tokio::net::{TcpListener, TcpStream};

use redis_starter_rust::{Connection, ReplicationConnection, ReplicationMode, Server};

struct Config {
    host: IpAddr,
    port: u16,
    replication: ReplicationMode,
}

fn parse_args() -> anyhow::Result<Config> {
    let mut args = std::env::args();
    args.next()
        .context("Expected first arg (path of executable)")?;

    let mut config = Config {
        host: "127.0.0.1".parse().unwrap(),
        port: 6379,
        replication: ReplicationMode::Master,
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
            "--replicaof" => {
                let mut host = args
                    .next()
                    .context("Argument `replicaof` missing host value")?;
                let port = args
                    .next()
                    .context("Argument `replicaof` missing port value")?
                    .parse()
                    .context("Invalid value for port arg")?;
                if host == "localhost" {
                    // TODO: perform DNS resolution instead
                    host = String::from("127.0.0.1");
                }
                let addr = SocketAddr::new(host.parse()?, port);
                config.replication = ReplicationMode::Slave { addr };
            }
            _ => bail!("Unrecognized argument {arg}"),
        }
    }
    Ok(config)
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let config = parse_args()?;
    let addr = SocketAddr::new(config.host, config.port);
    let listener = TcpListener::bind(addr).await?;
    let server = Server::new(config.replication, addr);

    if let ReplicationMode::Slave { addr } = server.replication {
        let server = server.clone();
        let mut stream = TcpStream::connect(addr)
            .await
            .context("Connceting to master node failed")
            .unwrap();
        tokio::spawn(async move {
            let conn = ReplicationConnection::new(&mut stream, server);
            match conn.run_replication_loop().await {
                Ok(_) => {}
                Err(err) => eprintln!("Processing of stream failed: {}", err),
            };
        });
    }

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
