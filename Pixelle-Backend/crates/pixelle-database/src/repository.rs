use anyhow::Result;

pub struct DatabaseRepository;

impl DatabaseRepository {
    pub fn new() -> Self {
        Self
    }

    pub async fn health_check(&self) -> Result<bool> {
        Ok(true)
    }
}
