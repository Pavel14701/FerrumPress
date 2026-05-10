#[cfg(feature = "image-processing")]
use async_trait::async_trait;
#[cfg(feature = "image-processing")]
use ferrumpress_core::traits::{ImageProcessor, ImageVariant};
#[cfg(feature = "image-processing")]
use crate::ProcessedVariant;
#[cfg(feature = "image-processing")]
use std::io::Cursor;

#[cfg(feature = "image-processing")]
pub struct DefaultImageProcessor;

#[cfg(feature = "image-processing")]
#[async_trait]
impl ImageProcessor for DefaultImageProcessor {
    async fn process_image(&self, data: Vec<u8>, source_mime: &str) -> Result<Vec<ProcessedVariant>, String> {
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