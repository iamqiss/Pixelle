use anyhow::Result;
use std::time::Duration;
use sqlx::{postgres::PgPoolOptions, PgPool};
use deadpool_redis::{Config as RedisConfig, Pool as RedisPool, Runtime};

pub struct Datastores {
	pub pg: PgPool,
	pub redis: RedisPool,
}

impl Datastores {
	pub async fn connect(pg_url: &str, redis_url: &str) -> Result<Self> {
		let pg = PgPoolOptions::new()
			.max_connections(32)
			.acquire_timeout(Duration::from_secs(2))
			.max_lifetime(Duration::from_secs(600))
			.idle_timeout(Duration::from_secs(60))
			.connect(pg_url)
			.await?;

		let mut cfg = RedisConfig::default();
		cfg.url = Some(redis_url.to_string());
		let redis = cfg.create_pool(Some(Runtime::Tokio1))?;

		Ok(Self { pg, redis })
	}

	pub async fn readiness(&self) -> bool {
		let pg_ok = sqlx::query_scalar::<_, i32>("SELECT 1").fetch_one(&self.pg).await.is_ok();
		let mut conn_ok = false;
		if let Ok(mut conn) = self.redis.get().await {
			use deadpool_redis::redis::AsyncCommands;
			conn_ok = conn.ping().await.is_ok();
		}
		pg_ok && conn_ok
	}

	// Ensure moderation tables exist
	pub async fn ensure_moderation_schema(&self) -> Result<()> {
		let create_reports = r#"
		CREATE TABLE IF NOT EXISTS user_reports (
			id UUID PRIMARY KEY,
			reporter_id UUID NOT NULL,
			target_id UUID NOT NULL,
			content_id TEXT,
			report_type TEXT NOT NULL,
			reason TEXT NOT NULL,
			description TEXT,
			status TEXT NOT NULL,
			priority TEXT NOT NULL,
			assigned_specialist UUID,
			created_at TIMESTAMPTZ NOT NULL,
			updated_at TIMESTAMPTZ NOT NULL,
			resolved_at TIMESTAMPTZ
		);
		CREATE INDEX IF NOT EXISTS idx_user_reports_status ON user_reports(status);
		CREATE INDEX IF NOT EXISTS idx_user_reports_priority ON user_reports(priority);
		CREATE INDEX IF NOT EXISTS idx_user_reports_target ON user_reports(target_id);
		"#;
		sqlx::query(create_reports).execute(&self.pg).await?;

		let create_investigations = r#"
		CREATE TABLE IF NOT EXISTS report_investigations (
			id UUID PRIMARY KEY,
			report_id UUID NOT NULL REFERENCES user_reports(id) ON DELETE CASCADE,
			investigator_id UUID NOT NULL,
			status TEXT NOT NULL,
			started_at TIMESTAMPTZ NOT NULL,
			completed_at TIMESTAMPTZ,
			time_spent_minutes INTEGER NOT NULL DEFAULT 0
		);
		CREATE INDEX IF NOT EXISTS idx_report_investigations_report ON report_investigations(report_id);
		"#;
		sqlx::query(create_investigations).execute(&self.pg).await?;

		let create_notes = r#"
		CREATE TABLE IF NOT EXISTS investigation_notes (
			id UUID PRIMARY KEY,
			investigation_id UUID NOT NULL REFERENCES report_investigations(id) ON DELETE CASCADE,
			author_id UUID NOT NULL,
			content TEXT NOT NULL,
			is_internal BOOLEAN NOT NULL DEFAULT FALSE,
			created_at TIMESTAMPTZ NOT NULL
		);
		CREATE INDEX IF NOT EXISTS idx_investigation_notes_investigation ON investigation_notes(investigation_id);
		"#;
		sqlx::query(create_notes).execute(&self.pg).await?;

		Ok(())
	}

	// Audit log schema
	pub async fn ensure_audit_schema(&self) -> Result<()> {
		let create_audit = r#"
		CREATE TABLE IF NOT EXISTS audit_events (
			id UUID PRIMARY KEY,
			action TEXT NOT NULL,
			subject_id TEXT,
			actor_id TEXT,
			metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
			created_at TIMESTAMPTZ NOT NULL
		);
		CREATE INDEX IF NOT EXISTS idx_audit_action ON audit_events(action);
		CREATE INDEX IF NOT EXISTS idx_audit_created_at ON audit_events(created_at);
		"#;
		sqlx::query(create_audit).execute(&self.pg).await?;
		Ok(())
	}

	pub async fn insert_audit_event(&self, action: &str, subject_id: Option<&str>, actor_id: Option<&str>, metadata: &serde_json::Value) -> Result<()> {
		let q = r#"INSERT INTO audit_events (id, action, subject_id, actor_id, metadata, created_at) VALUES ($1,$2,$3,$4,$5,NOW())"#;
		sqlx::query(q)
			.bind(uuid::Uuid::new_v4())
			.bind(action)
			.bind(subject_id)
			.bind(actor_id)
			.bind(metadata)
			.execute(&self.pg)
			.await?;
		Ok(())
	}

	pub async fn list_audit_events(&self, action: Option<&str>, limit: i64, offset: i64) -> Result<Vec<serde_json::Value>> {
		let mut base = String::from("SELECT id, action, subject_id, actor_id, metadata, created_at FROM audit_events");
		if action.is_some() { base.push_str(" WHERE action = $1"); }
		base.push_str(" ORDER BY created_at DESC LIMIT $2 OFFSET $3");
		let mut q = sqlx::query(&base);
		if let Some(a) = action { q = q.bind(a); }
		q = q.bind(limit).bind(offset);
		let rows = q.fetch_all(&self.pg).await?;
		let data = rows.into_iter().map(|r| serde_json::json!({
			"id": r.try_get::<uuid::Uuid,_>("id").ok().map(|v| v.to_string()),
			"action": r.try_get::<String,_>("action").unwrap_or_default(),
			"subject_id": r.try_get::<String,_>("subject_id").ok(),
			"actor_id": r.try_get::<String,_>("actor_id").ok(),
			"metadata": r.try_get::<serde_json::Value,_>("metadata").unwrap_or(serde_json::json!({})),
			"created_at": r.try_get::<chrono::DateTime<chrono::Utc>,_>("created_at").unwrap(),
		})).collect();
		Ok(data)
	}

	// Reports CRUD
	pub async fn insert_user_report(&self, report: &crate::core::reports::UserReport) -> Result<()> {
		let q = r#"INSERT INTO user_reports (
			id, reporter_id, target_id, content_id, report_type, reason, description,
			status, priority, assigned_specialist, created_at, updated_at, resolved_at
		) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)
		ON CONFLICT (id) DO NOTHING
		"#;
		sqlx::query(q)
			.bind(report.id)
			.bind(report.reporter_id)
			.bind(report.target_id)
			.bind(report.content_id.clone())
			.bind(format!("{:?}", report.report_type))
			.bind(&report.reason)
			.bind(report.description.clone())
			.bind(format!("{:?}", report.status))
			.bind(format!("{:?}", report.priority))
			.bind(report.assigned_specialist)
			.bind(report.created_at)
			.bind(report.updated_at)
			.bind(report.resolved_at)
			.execute(&self.pg)
			.await?;
		Ok(())
	}

	pub async fn fetch_user_report(&self, id: uuid::Uuid) -> Result<Option<crate::core::reports::UserReport>> {
		let q = r#"SELECT id, reporter_id, target_id, content_id, report_type, reason, description,
			status, priority, assigned_specialist, created_at, updated_at, resolved_at
			FROM user_reports WHERE id=$1"#;
		let row = sqlx::query(q).bind(id).fetch_optional(&self.pg).await?;
		Ok(row.map(|r| crate::storage::models::row_to_user_report(r)))
	}

	pub async fn list_user_reports(&self, status: Option<String>, priority: Option<String>, limit: i64, offset: i64, updated_since: Option<chrono::DateTime<chrono::Utc>>) -> Result<Vec<crate::core::reports::UserReport>> {
		let mut base = String::from("SELECT id, reporter_id, target_id, content_id, report_type, reason, description, status, priority, assigned_specialist, created_at, updated_at, resolved_at FROM user_reports");
		let mut where_clauses: Vec<String> = Vec::new();
		if status.is_some() { where_clauses.push("status = $1".to_string()); }
		if priority.is_some() { where_clauses.push("priority = $2".to_string()); }
		if updated_since.is_some() { where_clauses.push("updated_at > $3".to_string()); }
		if !where_clauses.is_empty() { base.push_str(" WHERE "); base.push_str(&where_clauses.join(" AND ")); }
		base.push_str(" ORDER BY created_at DESC LIMIT $4 OFFSET $5");
		let mut q = sqlx::query(&base);
		if let Some(s) = status.clone() { q = q.bind(s); }
		if let Some(p) = priority.clone() { q = q.bind(p); }
		if let Some(ts) = updated_since { q = q.bind(ts); }
		q = q.bind(limit).bind(offset);
		let rows = q.fetch_all(&self.pg).await?;
		Ok(rows.into_iter().map(crate::storage::models::row_to_user_report).collect())
	}

	pub async fn update_user_report_status(&self, id: uuid::Uuid, status: String) -> Result<()> {
		let q = r#"UPDATE user_reports SET status=$2, updated_at=NOW(), resolved_at=CASE WHEN $2 IN ('Resolved','Dismissed') THEN NOW() ELSE resolved_at END WHERE id=$1"#;
		sqlx::query(q).bind(id).bind(status).execute(&self.pg).await?;
		Ok(())
	}
}