use std::pin::pin;
use std::sync::Arc;

use async_stream::stream;
use aws_sdk_s3::error::SdkError;
use aws_sdk_s3::operation::get_object::GetObjectError;
use aws_sdk_s3::primitives::{ByteStream, ByteStreamError};
use bytes::{Bytes, BytesMut};
use futures::stream::StreamExt;
use futures::Stream;
use thiserror::Error;
use tokio_stream::wrappers::ReceiverStream;

pub async fn download_vec<T: Copy + Default>(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    key: &str,
) -> Result<Option<Vec<T>>, aws_sdk_s3::Error> {
    let result = client.get_object().bucket(bucket).key(key).send().await;

    match result {
        Ok(o) => {
            let mut stream = o.body;
            let size = o.content_length.unwrap() as usize;
            let size_of_t = std::mem::size_of::<T>();
            let length = size / size_of_t;
            assert!(size % size_of_t == 0);

            let mut vec: Vec<T> = vec![T::default(); length];

            let mut offset = 0;
            while let Some(Ok(chunk)) = stream.next().await {
                unsafe {
                    let src_ptr = chunk.as_ptr();
                    let dst_ptr = vec.as_mut_ptr() as *mut u8;
                    let src_len = chunk.len();
                    assert!(offset + src_len <= vec.len() * size_of_t);
                    std::ptr::copy_nonoverlapping(src_ptr, dst_ptr.add(offset), src_len);
                    offset += src_len;
                }
            }
            Ok(Some(vec))
        }
        Err(e) => {
            let error: aws_sdk_s3::Error = e.into();
            match error {
                aws_sdk_s3::Error::NoSuchKey(_) => Ok(None),
                _ => Err(error),
            }
        }
    }
}

pub async fn stream_vecs(
    mut bytes: ByteStream,
    chunk_size: usize,
    mut count: Option<usize>,
) -> impl Stream<Item = Result<Bytes, ByteStreamError>> {
    stream! {
        let mut buf = BytesMut::new();
        loop {
            while buf.len() >= chunk_size {
                // there's already enough data to read the next vec
                let chunk = buf.split_to(chunk_size);
                yield Ok(chunk.freeze());
                if let Some(c) = count.as_mut() {
                    *c -= 1;
                    if *c == 0 {
                        break;
                    }
                }
            }

            if count == Some(0) {
                // no more returning!
                break;
            }

            match bytes.try_next().await {
                Ok(Some(next)) =>  {
                    buf.extend(next);
                    continue;
                }
                Ok(None) => {
                    // buf better be empty at this point or this is an unexpected eof
                    assert!(buf.is_empty(), "stream ended unexpectedly");
                    break;
                }
                Err(e) => {
                    yield Err(e);
                    break;
                }
            }
        }
    }
}

#[derive(Debug, Error)]
pub enum VecStreamError {
    #[error(transparent)]
    ByteStreamError(#[from] ByteStreamError),
    #[error(transparent)]
    StreamInitFailed(#[from] SdkError<GetObjectError>),
}

pub async fn stream_vecs_from(
    client: Arc<aws_sdk_s3::Client>,
    bucket: String,
    key: String,
    mut start_index: usize,
    end_index: Option<usize>,
    chunk_size: usize,
) -> impl Stream<Item = Result<Bytes, VecStreamError>> {
    stream! {
        let mut failure_count = 0;
        'outer: loop {
            let start_pos = start_index * chunk_size;
            let range = if let Some(end_index) = end_index.as_ref() {
                let end_pos = end_index * chunk_size - 1;
                format!("bytes={}-{}", start_pos, end_pos)
            } else {
                format!("bytes={}-", start_pos)
            };
            let result = client.get_object()
                .range(range)
                .bucket(&bucket)
                .key(&key)
                .send()
                .await?;

            let count = end_index.map(|e| e - start_index);
            let mut stream = pin!(stream_vecs(result.body, chunk_size, count).await);
            'inner: loop {
                match stream.next().await {
                    Some(Ok(vec)) =>  {
                        failure_count = 0;
                        start_index += 1;
                        yield Ok(vec);
                    }
                    Some(Err(e)) => {
                        failure_count += 1;
                        if failure_count >= 5 {
                            // 5 failures with no actual result read. time to just fail for real.
                            yield Err(e.into());
                            break 'outer;
                        } else {
                            // but if not, try again
                            eprintln!("read failed: {e}. retrying.. ({failure_count}");
                            break 'inner;
                        }
                    }
                    None => {
                        // done!!
                        break 'outer;
                    }
                }
            }
        }
    }
}

pub async fn concurrent_stream_vecs_from(
    client: Arc<aws_sdk_s3::Client>,
    bucket: String,
    key: String,
    start_index: usize,
    end_index: Option<usize>,
    chunk_size: usize,
) -> impl Stream<Item = Result<Bytes, VecStreamError>> {
    let (tx, rx) = tokio::sync::mpsc::channel(10);
    tokio::spawn(async move {
        let mut stream =
            pin!(stream_vecs_from(client, bucket, key, start_index, end_index, chunk_size).await);
        loop {
            let next = stream.next().await;
            let is_last = !matches!(next.as_ref(), Some(Ok(_)));
            tx.send(next).await.unwrap();
            if is_last {
                break;
            }
        }
    });

    ReceiverStream::new(rx)
        .take_while(|v| futures::future::ready(v.is_some()))
        .map(|v| v.unwrap())
}
