use std::sync::Arc;

use aws_sdk_s3::{
    error::SdkError,
    operation::{
        complete_multipart_upload::CompleteMultipartUploadError,
        create_multipart_upload::CreateMultipartUploadError, upload_part::UploadPartError,
    },
    types::{CompletedMultipartUpload, CompletedPart},
    Client,
};
use bytes::{Bytes, BytesMut};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::{sync::Mutex, task::JoinHandle};

struct UploadResult {
    bytes_sent: usize,
    e_tag: String,
}

pub struct Upload {
    client: Arc<Client>,
    pub info: UploadInfo,
    data: BytesMut,
    upload_task: Option<JoinHandle<Result<UploadResult, SdkError<UploadPartError>>>>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct UploadInfo {
    bucket: String,
    key: String,
    size_per_upload: usize,

    upload_id: String,
    parts: Vec<String>,
    pub uploaded_bytes: usize,
}

#[derive(Debug, Error)]
pub enum UploadCompleteError {
    #[error("final part upload failed: {0}")]
    FinalPartFailed(SdkError<UploadPartError>),
    #[error("complete multipart upload failed: {0}")]
    CompletionFailed(SdkError<CompleteMultipartUploadError>),
}

impl Upload {
    pub fn new_from_info(client: Arc<Client>, info: UploadInfo) -> Upload {
        Self {
            client: client.clone(),
            data: BytesMut::new(),
            info,
            upload_task: None,
        }
    }

    pub async fn new(
        client: Arc<Client>,
        bucket: String,
        key: String,
    ) -> Result<Upload, SdkError<CreateMultipartUploadError>> {
        const SIZE_PER_UPLOAD: usize = 512 << 20;
        let upload = client
            .create_multipart_upload()
            .bucket(&bucket)
            .key(&key)
            .send()
            .await?;
        let upload = Upload {
            client: client.clone(),
            data: BytesMut::new(),
            info: UploadInfo {
                bucket,
                key,
                upload_id: upload.upload_id.unwrap(),
                parts: Vec::new(),
                size_per_upload: SIZE_PER_UPLOAD,
                uploaded_bytes: 0,
            },
            upload_task: None,
        };

        Ok(upload)
    }

    pub async fn new_with_size(
        client: Arc<Client>,
        bucket: String,
        key: String,
        size_per_upload: usize,
    ) -> Result<Upload, SdkError<CreateMultipartUploadError>> {
        let upload = client
            .create_multipart_upload()
            .bucket(&bucket)
            .key(&key)
            .send()
            .await?;
        let upload = Upload {
            client: client.clone(),
            data: BytesMut::new(),
            info: UploadInfo {
                bucket,
                key,
                upload_id: upload.upload_id.unwrap(),
                parts: Vec::new(),
                size_per_upload,
                uploaded_bytes: 0,
            },
            upload_task: None,
        };

        Ok(upload)
    }

    async fn start_part_upload(&mut self) -> Result<(), SdkError<UploadPartError>> {
        assert!(self.data.len() >= self.info.size_per_upload);
        let to_send = self.data.split_to(self.info.size_per_upload).freeze();
        let part_num = (self.info.parts.len() + 1) as i32;
        eprintln!(
            "uploading {} bytes to {} (part {})",
            self.info.size_per_upload, self.info.key, part_num
        );
        let bytes_sent = to_send.len();
        let bucket = self.info.bucket.clone();
        let key = self.info.key.clone();
        let upload_id = self.info.upload_id.clone();
        let client = self.client.clone();
        self.upload_task = Some(tokio::spawn(async move {
            let part_upload = client
                .upload_part()
                .bucket(&bucket)
                .key(&key)
                .upload_id(&upload_id)
                .part_number(part_num)
                .body(to_send.into())
                .send()
                .await?;

            Ok(UploadResult {
                bytes_sent,
                e_tag: part_upload.e_tag.unwrap(),
            })
        }));

        Ok(())
    }

    async fn finish_part_upload(&mut self) -> Result<bool, SdkError<UploadPartError>> {
        let mut upload_task = None;
        std::mem::swap(&mut upload_task, &mut self.upload_task);
        if let Some(upload_task) = upload_task {
            let UploadResult { bytes_sent, e_tag } =
                upload_task.await.expect("join failed on upload task")?;

            self.info.uploaded_bytes += bytes_sent;
            self.info.parts.push(e_tag);
            Ok(true)
        } else {
            Ok(false)
        }
    }

    pub async fn send(&mut self, data: Bytes) -> Result<bool, SdkError<UploadPartError>> {
        let mut something_happened = false;
        self.data.extend(data);
        if self.upload_task.is_some() && self.upload_task.as_ref().unwrap().is_finished() {
            something_happened = self.finish_part_upload().await?;
        }
        while self.data.len() >= self.info.size_per_upload {
            something_happened = something_happened || self.finish_part_upload().await?;
            self.start_part_upload().await?;
        }

        Ok(something_happened)
    }

    async fn send_final(&mut self) -> Result<(), SdkError<UploadPartError>> {
        self.finish_part_upload().await?;
        if self.data.is_empty() {
            return Ok(());
        }
        let part_num = (self.info.parts.len() + 1) as i32;
        eprintln!(
            "uploading final {} bytes to {} (part {})",
            self.info.size_per_upload, self.info.key, part_num
        );
        let part_upload = self
            .client
            .upload_part()
            .bucket(&self.info.bucket)
            .key(&self.info.key)
            .upload_id(&self.info.upload_id)
            .part_number(part_num)
            .body(self.data.clone().freeze().into())
            .send()
            .await?;

        let e_tag = part_upload.e_tag.unwrap();
        self.info.parts.push(e_tag);

        Ok(())
    }

    pub async fn complete(mut self) -> Result<(), UploadCompleteError> {
        self.send_final()
            .await
            .map_err(UploadCompleteError::FinalPartFailed)?;
        let Self {
            client,
            info:
                UploadInfo {
                    bucket,
                    key,
                    upload_id,
                    parts,
                    ..
                },
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
            .await
            .map_err(UploadCompleteError::CompletionFailed)?;

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
    ) -> Result<Self, aws_sdk_s3::Error> {
        let mut uploads = Vec::with_capacity(amount);
        for index in 0..amount {
            let upload =
                Upload::new(client.clone(), bucket.clone(), format!("{prefix}{index}")).await?;
            uploads.push(Mutex::new(upload));
        }

        Ok(Self { uploads })
    }

    pub async fn new_with_size(
        client: Arc<Client>,
        bucket: String,
        prefix: String,
        amount: usize,
        size_per_upload: usize,
    ) -> Result<Self, aws_sdk_s3::Error> {
        let mut uploads = Vec::with_capacity(amount);
        for index in 0..amount {
            let upload = Upload::new_with_size(
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

    pub async fn complete(self) -> Result<(), UploadCompleteError> {
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
