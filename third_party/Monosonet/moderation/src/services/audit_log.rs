use tracing::info;
use anyhow::Result;

#[derive(Clone, Debug)]
pub struct AuditEvent {
	pub action: String,
	pub subject_id: Option<String>,
	pub actor_id: Option<String>,
	pub metadata: serde_json::Value,
}

pub async fn record(event: &str) {
	info!("audit_event={}", event);
}

pub async fn record_structured(ds: Option<std::sync::Arc<crate::storage::db::Datastores>>, ev: AuditEvent) -> Result<()> {
	info!("audit_event action={} subject={:?}", ev.action, ev.subject_id);
	if let Some(_ds) = ds {
		let _ = _ds.insert_audit_event(&ev.action, ev.subject_id.as_deref(), ev.actor_id.as_deref(), &ev.metadata).await;
	}
	Ok(())
}