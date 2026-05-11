use ferrumpress_core::traits::ImageVariant;

#[derive(Debug, Clone)]
pub struct ProcessedVariant {
    pub meta: ImageVariant,
    pub data: Vec<u8>,
}
