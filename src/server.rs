use futures_util::sink::SinkExt;
use futures_util::stream::StreamExt;
use std::error::Error;
use std::net::SocketAddr;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::broadcast::{Sender, channel};
use tokio_websockets::{Message, ServerBuilder, WebSocketStream};

async fn handle_connection(
    addr: SocketAddr,
    ws_stream: WebSocketStream<TcpStream>,
    bcast_tx: Sender<String>,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    
    // FIX 1: Split the websocket stream
    let (mut ws_sender, mut ws_receiver) = ws_stream.split();
    let mut bcast_rx = bcast_tx.subscribe();

    // Send welcome message
    let _ = ws_sender.send(Message::text("Welcome to chat! Type a message")).await;

    loop {
        tokio::select! {
            // Task 1: Receive message from the client, and broadcast it
            incoming = ws_receiver.next() => {
                match incoming {
                    Some(Ok(msg)) => {
                        if let Some(text) = msg.as_text() {
                            println!("From client {} {:?}", addr, text);
                            let bcast_msg = format!("{}|{}", addr, text);
                            let _ = bcast_tx.send(bcast_msg);
                        }
                    }
                    Some(Err(e)) => {
                        eprintln!("Error from connection {}: {:?}", addr, e);
                        break;
                    }
                    None => break, // Connection closed
                }
            }
            // Task 2: Receive message from broadcast, and send to client
            bcast_msg = bcast_rx.recv() => {
                match bcast_msg {
                    Ok(msg_str) => {
                        if let Some((sender_addr_str, msg_content)) = msg_str.split_once('|') {
                            if sender_addr_str != addr.to_string() {
                                let formatted_msg = format!("{}: {}", sender_addr_str, msg_content);
                                // Use ws_sender here
                                if ws_sender.send(Message::text(formatted_msg)).await.is_err() {
                                    break;
                                }
                            }
                        }
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {}
                }
            }
        }
    }
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    let (bcast_tx, _) = channel(16);

    let listener = TcpListener::bind("127.0.0.1:8080").await?;
    println!("listening on port 8080");

    loop {
        let (socket, addr) = listener.accept().await?;
        println!("New connection from Ade's Computer{}", addr);
        let bcast_tx = bcast_tx.clone();
        
        tokio::spawn(async move {
            // FIX 2: Manually match the Results rather than using ? inside the tokio::spawn block
            match ServerBuilder::new().accept(socket).await {
                Ok((_req, ws_stream)) => {
                    if let Err(e) = handle_connection(addr, ws_stream, bcast_tx).await {
                        eprintln!("Connection error from {addr}: {e}");
                    }
                }
                Err(e) => {
                    eprintln!("Failed to accept websocket connection: {e}");
                }
            }
        });
    }
}