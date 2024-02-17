use tokio::net::TcpListener;

use redis_starter_rust::{Connection, Server};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let listener = TcpListener::bind("127.0.0.1:6379").await?;
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
