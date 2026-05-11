pub mod local;
#[cfg(feature = "s3")]
pub mod s3;
pub mod media_service;
#[cfg(feature = "image-processing")]
pub mod image_processor;
#[cfg(feature = "image-processing")]
pub mod image_worker;

pub use local::LocalStorageBackend;
#[cfg(feature = "s3")]
pub use s3::S3Backend;
pub use media_service::MediaServiceImpl;
#[cfg(feature = "image-processing")]
pub use image_processor::{DefaultImageProcessor, ImageProcessor};
#[cfg(feature = "image-processing")]
pub use image_worker::run_media_worker;
