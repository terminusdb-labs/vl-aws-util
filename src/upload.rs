use std::sync::Arc;

use aws_sdk_s3::{
    types::{CompletedMultipartUpload, CompletedPart},
    Client,
};
use bytes::{Bytes, BytesMut};
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;

pub struct Upload {
    client: Arc<Client>,
    bucket: String,
    key: String,
    data: BytesMut,
    upload_id: String,
    size_per_upload: usize,
    parts: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct UploadInfo {
    bucket: String,
    key: String,
    size_per_upload: usize,

    upload_id: String,
    parts: Vec<String>,
    uploaded_bytes: usize,
}

impl Upload {
    async fn new(
        client: Arc<Client>,
        bucket: String,
        key: String,
    ) -> Result<Upload, aws_sdk_s3::Error> {
        const SIZE_PER_UPLOAD: usize = 512 << 20;
        let upload = client
            .create_multipart_upload()
            .bucket(&bucket)
            .key(&key)
            .send()
            .await?;
        let upload = Upload {
            client: client.clone(),
            bucket,
            key,
            data: BytesMut::new(),
            upload_id: upload.upload_id.unwrap(),
            parts: Vec::new(),
            size_per_upload: SIZE_PER_UPLOAD,
        };

        Ok(upload)
    }

    async fn new_with_size(
        client: Arc<Client>,
        bucket: String,
        key: String,
        size_per_upload: usize,
    ) -> Result<Upload, aws_sdk_s3::Error> {
        let upload = client
            .create_multipart_upload()
            .bucket(&bucket)
            .key(&key)
            .send()
            .await?;
        let upload = Upload {
            client: client.clone(),
            bucket,
            key,
            data: BytesMut::new(),
            upload_id: upload.upload_id.unwrap(),
            parts: Vec::new(),
            size_per_upload,
        };

        Ok(upload)
    }

    async fn send(&mut self, data: Bytes) -> Result<(), aws_sdk_s3::Error> {
        self.data.extend(data);
        while self.data.len() >= self.size_per_upload {
            let part_num = (self.parts.len() + 1) as i32;
            eprintln!(
                "uploading {} bytes to {} (part {})",
                self.size_per_upload, self.key, part_num
            );
            let to_send = self.data.split_to(self.size_per_upload);
            let part_upload = self
                .client
                .upload_part()
                .bucket(&self.bucket)
                .key(&self.key)
                .upload_id(&self.upload_id)
                .part_number(part_num)
                .body(to_send.freeze().into())
                .send()
                .await?;

            let e_tag = part_upload.e_tag.unwrap();
            self.parts.push(e_tag);
        }

        Ok(())
    }

    async fn send_final(&mut self) -> Result<(), aws_sdk_s3::Error> {
        if self.data.is_empty() {
            return Ok(());
        }
        let part_num = (self.parts.len() + 1) as i32;
        eprintln!(
            "uploading final {} bytes to {} (part {})",
            self.size_per_upload, self.key, part_num
        );
        let part_upload = self
            .client
            .upload_part()
            .bucket(&self.bucket)
            .key(&self.key)
            .upload_id(&self.upload_id)
            .part_number(part_num)
            .body(self.data.clone().freeze().into())
            .send()
            .await?;

        let e_tag = part_upload.e_tag.unwrap();
        self.parts.push(e_tag);

        Ok(())
    }

    pub async fn complete(mut self) -> Result<(), aws_sdk_s3::Error> {
        self.send_final().await?;
        let Self {
            client,
            bucket,
            key,
            upload_id,
            parts,
            ..
        } = self;

        let parts: Vec<_> = parts
            .into_iter()
            .enumerate()
            .map(|(ix, e_tag)| {
                CompletedPart::builder()
                    .part_number((ix + 1) as i32)
                    .e_tag(e_tag)
                    .build()
            })
            .collect();

        client
            .complete_multipart_upload()
            .bucket(bucket)
            .key(key)
            .upload_id(upload_id)
            .multipart_upload(
                CompletedMultipartUpload::builder()
                    .set_parts(Some(parts))
                    .build(),
            )
            .send()
            .await?;

        Ok(())
    }
}

pub struct Uploads {
    uploads: Vec<Mutex<Upload>>,
}

impl Uploads {
    pub async fn new(
        client: Arc<Client>,
        bucket: String,
        prefix: String,
        amount: usize,
        size_per_upload: usize,
    ) -> Result<Self, aws_sdk_s3::Error> {
        let mut uploads = Vec::with_capacity(amount);
        for index in 0..amount {
            let upload = Upload::new(
                client.clone(),
                bucket.clone(),
                format!("{prefix}{index}"),
                size_per_upload,
            )
            .await?;
            uploads.push(Mutex::new(upload));
        }

        Ok(Self { uploads })
    }

    pub async fn send(&self, index: usize, data: Bytes) -> Result<(), aws_sdk_s3::Error> {
        let mut upload = self.uploads[index].lock().await;

        upload.send(data).await?;

        Ok(())
    }

    pub async fn complete(self) -> Result<(), aws_sdk_s3::Error> {
        for lock in self.uploads {
            let upload = lock.into_inner();
            upload.complete().await?;
        }

        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MultiUploadInfo {
    uploads: Vec<UploadInfo>,
}
