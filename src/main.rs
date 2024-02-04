use tokio::net::TcpListener;

use redis_starter_rust::process_stream;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let listener = TcpListener::bind("127.0.0.1:6379").await?;

    loop {
        match listener.accept().await {
            Ok((stream, addr)) => {
                tokio::spawn(async move {
                    eprintln!("new client: {:?}", addr);
                    match process_stream(stream).await {
                        Ok(_) => {}
                        Err(err) => eprintln!("Processing of stream failed: {}", err),
                    };
                });
            }
            Err(e) => eprintln!("couldn't get client: {:?}", e),
        }
    }
}
