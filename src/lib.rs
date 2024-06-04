use std::{
    collections::HashMap,
    time::Duration,
};

use anyhow::{format_err, Result};

pub type PgManager  = bb8_postgres::PostgresConnectionManager<tokio_postgres::NoTls>;
pub type PgPool     = bb8::Pool<PgManager>;
pub type PgConn<'a> = bb8::PooledConnection<'a, PgManager>;
pub type PgError    = tokio_postgres::Error;

pub trait DbConfig {
    fn uri(&self) -> String;
    fn min(&self) -> u32;
    fn max(&self) -> u32;
    fn lifetime(&self) -> Duration;
    fn idle_timeout(&self) -> Duration;
    fn connect_timeout(&self) -> Duration;
}


pub async fn build_pool(cfg: &impl DbConfig) -> Result<PgPool> {
    let pg_mgr = bb8_postgres::PostgresConnectionManager::new(cfg.uri().as_str().parse()?, tokio_postgres::NoTls);

    bb8::Builder::new()
        .min_idle(Some(cfg.min()))
        .max_size(cfg.max())
        .max_lifetime(Some(cfg.lifetime()))
        .idle_timeout(Some(cfg.idle_timeout()))
        .connection_timeout(cfg.connect_timeout())
        .build(pg_mgr)
        .await
        .map_err(|e| anyhow::format_err!("DB POOL FAIL: {}", e))
}

#[derive(Clone)]
pub struct ScopedPool {
    pool: HashMap<String, PgPool>,
}

impl ScopedPool {
    pub fn new() -> Self {
        Self {
            pool: HashMap::new()
        }
    }

    pub async fn reg(&mut self, scope: &str, cfg: &impl DbConfig) -> Result<()> {
        let pool = build_pool(cfg).await?;

        self.pool.insert(scope.into(), pool);

        Ok(())
    }

    pub async fn get(&self, scope: &str) -> Result<PgConn> {

        let pool = self.pool.get(scope)
            .ok_or_else(|| format_err!("SCOPE '{}' ABSENT", scope))?;

        pool.get().await
            .map_err(|e| e.into())
    }
}
