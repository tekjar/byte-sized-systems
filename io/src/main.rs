use std::error::Error;
use std::io::ErrorKind;
use std::{thread, time::Duration};

use bytes::{BufMut, BytesMut};
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt, AsyncReadExt, self};
use tokio::select;

const FRAME_LENGTH: usize = 100;

fn main() {
    let (x, y) = tokio::io::duplex(1024);

    thread::spawn(move || {
        b(Box::new(y));
    });

    a(Box::new(x)).unwrap();
}

#[tokio::main(flavor = "current_thread")]
async fn a(mut stream: Box<dyn IO>) -> Result<(), Box<dyn Error>>{
    let mut next_required = FRAME_LENGTH;
    let mut total_count = 0;

    let mut read = BytesMut::with_capacity(128);

    loop {
        select! {
            o = stream.read_buf(&mut read) => {
                let len = o?;
                if 0 == len {
                    return Ok(enrich_zero_read(&mut read)?);
                }

                if len < next_required {
                    println!("current read = {len}, required = {next_required}");
                    continue;
                }

                let mut parsed_count = 0;
                next_required = loop {
                    let o = read.split_to(FRAME_LENGTH);
                    parsed_count += 1;
                    total_count += 1;

                    let remaining = read.len();
                    if remaining < FRAME_LENGTH {
                        let required = FRAME_LENGTH - remaining;
                        let space = read.capacity();
                        println!("more required = {required:<4}, in buffer = {remaining:<4}, parsed count = {parsed_count:<4}, total count = {total_count:<3}, buf capacity = {space}");
                        break required
                    }
                };
            }
        }
    };
}

#[tokio::main(flavor = "current_thread")]
async fn b(mut stream: Box<dyn IO>) {
    let mut buffer = BytesMut::with_capacity(1024);
    let mut total_size = 0;
    for i in 0..100 {
        let payload = vec![1; FRAME_LENGTH];

        buffer.extend_from_slice(&payload);

        let len = stream.write_buf(&mut buffer).await.unwrap();
        total_size += len;
        stream.flush().await.unwrap();
        // println!("write len = {len}. total size = {total_size}");
    }
}

/// Extracts better error for 0 len network reads
/// A linux TcpStream read 0 always means that there is no more data after this
/// https://doc.rust-lang.org/std/io/trait.Read.html#tymethod.read
/// https://docs.rs/tokio/latest/tokio/io/trait.AsyncReadExt.html#method.read_buf
fn enrich_zero_read(buf: &mut BytesMut) -> io::Result<()> {
    let remaining = buf.len();
    if remaining > 0 {
        let error = format!("connection reset by peer. bytes in buffer = {}", remaining);
        Err(io::Error::new(ErrorKind::ConnectionReset, error))
    } else {
        Ok(())
    }
}

pub trait IO: AsyncRead + AsyncWrite + Send + Sync + Unpin {}
impl<T> IO for T where T: AsyncRead + AsyncWrite + Unpin + Send + Sync {}
