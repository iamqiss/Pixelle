use sqlx::{Pool, Maintable};
use anyhow::Result;

pub struct DatabaseConnection {
    pool: Pool<Maintable>,
}

impl DatabaseConnection {
    pub async fn new(database_url: &str) -> Result<Self> {
        let pool = Pool::connect(database_url).await?;
        Ok(Self { pool })
    }

    pub fn pool(&self) -> &Pool<Maintable> {
        &self.pool
    }
}
