use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;


pub async fn process_stream(mut stream: TcpStream) -> anyhow::Result<()> {
    let (mut reader, mut writer) = stream.split();
    let mut buf = [0u8; 1024];
    loop {
        reader.read(&mut buf).await?;
        writer.write_all(b"+PONG\r\n").await?;
    }
    
    // Ok(())
}

