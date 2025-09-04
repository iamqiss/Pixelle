use std::sync::Arc;
use tokio::sync::broadcast;

#[derive(Clone)]
pub struct Streams {
    pub report_tx: broadcast::Sender<serde_json::Value>,
    pub signal_tx: broadcast::Sender<serde_json::Value>,
}

use crate::storage::db::Datastores;

#[derive(Clone)]
pub struct AppState {
	pub datastores: Arc<Datastores>,
}