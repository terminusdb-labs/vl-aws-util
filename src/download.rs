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
