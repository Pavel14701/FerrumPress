#[cfg(feature = "image-processing")]
use async_trait::async_trait;

#[cfg(feature = "image-processing")]
use ferrumpress_core::traits::{ImageProcessor, ImageVariant, ProcessedVariant};

#[cfg(feature = "image-processing")]
use std::io::Cursor;

#[cfg(feature = "image-processing")]
use std::time::Duration;

#[cfg(feature = "image-processing")]
use tokio::time::timeout;

// Maximum image dimensions
const MAX_WIDTH: u32 = 4096;
const MAX_HEIGHT: u32 = 4096;

// Processing timeout
const PROCESS_TIMEOUT_SECS: u64 = 300;

#[cfg(feature = "image-processing")]
pub struct DefaultImageProcessor;

#[cfg(feature = "image-processing")]
#[async_trait]
impl ImageProcessor for DefaultImageProcessor {
    async fn process_image(&self, data: Vec<u8>, source_mime: &str) -> Result<Vec<ProcessedVariant>, String> {
        // Apply timeout to prevent hanging on large images
        let result = timeout(
            Duration::from_secs(PROCESS_TIMEOUT_SECS),
            self.process_image_inner(data, source_mime)
        ).await;

        match result {
            Ok(Ok(variants)) => Ok(variants),
            Ok(Err(e)) => Err(format!("image processing failed: {}", e)),
            Err(_) => Err("image processing timed out".to_string()),
        }
    }
}

#[cfg(feature = "image-processing")]
impl DefaultImageProcessor {
    async fn process_image_inner(&self, data: Vec<u8>, source_mime: &str) -> Result<Vec<ProcessedVariant>, String> {
        let format = match source_mime {
            "image/jpeg" => image::ImageFormat::Jpeg,
            "image/png"  => image::ImageFormat::Png,
            "image/webp" => image::ImageFormat::WebP,
            "image/avif" => image::ImageFormat::Avif,
            _ => return Err("Unsupported source format".into()),
        };

        let img = image::io::Reader::with_format(Cursor::new(data), format)
            .decode()
            .map_err(|e| e.to_string())?;

        let (w, h) = (img.width(), img.height());

        // Validate dimensions
        if w > MAX_WIDTH || h > MAX_HEIGHT {
            return Err(format!("image dimensions {}x{} exceed maximum {}x{}", w, h, MAX_WIDTH, MAX_HEIGHT));
        }

        let rgba = img.to_rgba8();

        let mut variants = Vec::new();

        // WebP
        let webp_data = {
            let encoder = webp::Encoder::from_rgba(&rgba, w, h);
            encoder.encode(80.0).to_vec()
        };
        variants.push(ProcessedVariant {
            meta: ImageVariant {
                format: "webp".into(),
                key: String::new(),
                size: webp_data.len() as u64,
                width: w,
                height: h,
            },
            data: webp_data,
        });

        // AVIF
        let avif_data = {
            use ravif::prelude::*;
            let encoded = Encoder::new()
                .with_quality(80.0)
                .with_num_threads(num_cpus::get())
                .encode_rgba(rgba, w, h)
                .map_err(|e| e.to_string())?;
            encoded.avif_file
        };
        variants.push(ProcessedVariant {
            meta: ImageVariant {
                format: "avif".into(),
                key: String::new(),
                size: avif_data.len() as u64,
                width: w,
                height: h,
            },
            data: avif_data,
        });

        Ok(variants)
    }
}
