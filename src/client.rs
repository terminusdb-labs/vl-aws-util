use aws_config::BehaviorVersion;
use aws_sdk_s3::config::StalledStreamProtectionConfig;

pub async fn default_client() -> aws_sdk_s3::Client {
    let config = aws_config::load_defaults(BehaviorVersion::latest()).await;
    let config = config
        .into_builder()
        .stalled_stream_protection(StalledStreamProtectionConfig::disabled())
        .build();
    aws_sdk_s3::Client::new(&config)
}
