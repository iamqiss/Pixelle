use anyhow::Result;

pub struct MlClient;

impl MlClient {
	pub fn new() -> Self { Self }
	pub async fn classify_text(&self, text: &str) -> Result<f32> {
		let _ = text;
		Ok(0.5)
	}
}