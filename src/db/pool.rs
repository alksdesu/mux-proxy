//! PgPool 封装：Db 是 Arc<PgPool> 的 newtype，Clone 廉价。启动期 sqlx::migrate! 跑迁移。

use crate::error::AppResult;
use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;
use std::ops::Deref;
use std::sync::Arc;
use std::time::Duration;

static MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!("./migrations");

#[derive(Clone)]
pub struct Db {
    inner: Arc<PgPool>,
}

impl Db {
    pub fn pool(&self) -> &PgPool {
        &self.inner
    }

    pub fn from_pool(pool: PgPool) -> Self {
        Self { inner: Arc::new(pool) }
    }
}

impl Deref for Db {
    type Target = PgPool;
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

pub async fn init_pool(database_url: &str) -> AppResult<Db> {
    let pool = PgPoolOptions::new()
        .max_connections(80)
        .acquire_timeout(Duration::from_secs(5))
        .idle_timeout(Duration::from_secs(30))
        .connect(database_url)
        .await?;

    MIGRATOR.run(&pool).await?;

    Ok(Db::from_pool(pool))
}
