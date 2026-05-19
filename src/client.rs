use futures_util::SinkExt;
use futures_util::stream::StreamExt;
use http::Uri;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio_websockets::{ClientBuilder, Message};

#[tokio::main]
async fn main() -> Result<(), tokio_websockets::Error> {
    let (ws_stream, _) =
        ClientBuilder::from_uri(Uri::from_static("ws://127.0.0.1:8080"))
            .connect()
            .await?;

    // FIX 1: Split the websocket stream into a sender and receiver
    // This allows us to read and write concurrently without borrow checker errors
    let (mut ws_sender, mut ws_receiver) = ws_stream.split();

    let stdin = tokio::io::stdin();
    let mut stdin = BufReader::new(stdin).lines();

    loop {
        tokio::select! {
            // Task 1: Read user message from stdin and send it to server
            line = stdin.next_line() => {
                match line {
                    Ok(Some(text)) => {
                        // Use ws_sender here
                        if ws_sender.send(Message::text(text)).await.is_err() {
                            break;
                        }
                    }
                    Ok(None) => break, // EOF
                    Err(e) => {
                        eprintln!("Error reading stdin: {:?}", e);
                        break;
                    }
                }
            }
            // Task 2: Receive message from server and display it
            // Use ws_receiver here
            msg = ws_receiver.next() => {
                match msg {
                    Some(Ok(msg)) => {
                        if let Some(text) = msg.as_text() {
                            println!("Ade's Computer - From server: {}", text);
                        }
                    }
                    Some(Err(e)) => {
                        eprintln!("Error receiving from server: {:?}", e);
                        break;
                    }
                    None => break, // Server disconnected
                }
            }
        }
    }
    Ok(())
}