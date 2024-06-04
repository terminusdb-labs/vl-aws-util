use serde::{Deserialize, Serialize};
use thiserror::Error;
use vectorlink_task::task::TaskStateError;

#[derive(Debug, Serialize, Deserialize, Error)]
pub enum TaskError {
    #[error("An AWS error occurred {0}")]
    AWSError(String),
    #[error("A Task error occurred {0}")]
    TaskStateError(String),
    #[error("An IO error occurred {0}")]
    IoError(String),
}

impl From<aws_sdk_s3::Error> for TaskError {
    fn from(value: aws_sdk_s3::Error) -> Self {
        Self::AWSError(value.to_string())
    }
}

impl From<std::io::Error> for TaskError {
    fn from(value: std::io::Error) -> Self {
        Self::IoError(value.to_string())
    }
}

impl From<TaskStateError> for TaskError {
    fn from(value: TaskStateError) -> Self {
        Self::TaskStateError(value.to_string())
    }
}
