use tokio::net::TcpListener;

use redis_starter_rust::Server;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let listener = TcpListener::bind("127.0.0.1:6379").await?;

    loop {
        match listener.accept().await {
            Ok((mut stream, addr)) => {
                tokio::spawn(async move {
                    let server = Server::new(&mut stream, addr);
                    match server.run_processing_loop().await {
                        Ok(_) => {}
                        Err(err) => eprintln!("Processing of stream failed: {}", err),
                    };
                });
            }
            Err(e) => eprintln!("couldn't get client: {:?}", e),
        }
    }
}
