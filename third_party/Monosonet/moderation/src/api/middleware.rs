use std::task::{Context, Poll};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use std::{future::Future, pin::Pin};

use axum::http::{Request, StatusCode};
use deadpool_redis::{redis::Script, Pool};
use futures_util::future::BoxFuture;
use tower::{Layer, Service};

const LUA_SLIDING_WINDOW: &str = r#"
-- KEYS[1] = key
-- ARGV[1] = now_ms
-- ARGV[2] = window_ms
-- ARGV[3] = limit
local key = KEYS[1]
local now_ms = tonumber(ARGV[1])
local window_ms = tonumber(ARGV[2])
local limit = tonumber(ARGV[3])
local min_ms = now_ms - window_ms
redis.call('ZREMRANGEBYSCORE', key, 0, min_ms)
local count = redis.call('ZCARD', key)
if count >= limit then
  return {0, count}
end
redis.call('ZADD', key, now_ms, tostring(now_ms))
redis.call('PEXPIRE', key, window_ms)
count = count + 1
return {1, count}
"#;

#[derive(Clone)]
pub struct RateLimitLayer {
	pub redis: Pool,
	pub window: Duration,
	pub limit: u32,
}

impl<S> Layer<S> for RateLimitLayer {
	type Service = RateLimitService<S>;
	fn layer(&self, inner: S) -> Self::Service {
		RateLimitService { inner, redis: self.redis.clone(), window: self.window, limit: self.limit }
	}
}

#[derive(Clone)]
pub struct RateLimitService<S> {
	inner: S,
	redis: Pool,
	window: Duration,
	limit: u32,
}

impl<S, B> Service<Request<B>> for RateLimitService<S>
where
	S: Service<Request<B>, Response = axum::response::Response> + Clone + Send + 'static,
	S::Future: Send + 'static,
	B: Send + 'static,
{
	type Response = S::Response;
	type Error = S::Error;
	type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

	fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
		self.inner.poll_ready(cx)
	}

	fn call(&mut self, req: Request<B>) -> Self::Future {
		let mut inner = self.inner.clone();
		let pool = self.redis.clone();
		let window = self.window;
		let limit = self.limit;
		let key = format!("rate:{}", client_key(&req));

		Box::pin(async move {
			let allowed = check_allow(&pool, &key, window, limit).await.unwrap_or(true);
			if !allowed {
				let mut resp = axum::response::Response::new(axum::body::Body::empty());
				*resp.status_mut() = StatusCode::TOO_MANY_REQUESTS;
				return Ok(resp);
			}
			inner.call(req).await
		})
	}
}

fn millis() -> u128 {
	SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis()
}

fn client_key<B>(req: &Request<B>) -> String {
	if let Some(ip) = req.headers().get("x-forwarded-for").and_then(|v| v.to_str().ok()) {
		ip.split(',').next().unwrap_or("unknown").trim().to_string()
	} else {
		req.extensions().get::<std::net::SocketAddr>().map(|a| a.ip().to_string()).unwrap_or_else(|| "unknown".to_string())
	}
}

async fn check_allow(pool: &Pool, key: &str, window: Duration, limit: u32) -> Result<bool, anyhow::Error> {
	let mut conn = pool.get().await?;
	let script = Script::new(LUA_SLIDING_WINDOW);
	let now = millis() as i64;
	let win_ms = window.as_millis() as i64;
	let limit_i = limit as i64;
	let res: (i64, i64) = script.key(key).arg(now).arg(win_ms).arg(limit_i).invoke_async(&mut conn).await?;
	Ok(res.0 == 1)
}