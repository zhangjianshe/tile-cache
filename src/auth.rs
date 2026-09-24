use anyhow::{bail, Context, Result};
use argon2::{
    password_hash::{rand_core::OsRng, PasswordHash, PasswordHasher, PasswordVerifier, SaltString},
    Argon2,
};
use sqlx::{
    sqlite::{SqliteConnectOptions, SqlitePoolOptions},
    SqlitePool,
};
use std::{
    path::Path,
    str::FromStr,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::sync::Semaphore;

const SESSION_SECONDS: u64 = 2 * 60 * 60;

#[derive(Clone)]
pub struct AuthService {
    database: SqlitePool,
    login_slots: Arc<Semaphore>,
}

impl AuthService {
    pub async fn open(config_dir: &Path) -> Result<Self> {
        tokio::fs::create_dir_all(config_dir).await?;
        let path = config_dir.join("tile-cache-meta.db");
        let options = SqliteConnectOptions::from_str(&format!("sqlite://{}", path.display()))?
            .create_if_missing(true)
            .journal_mode(sqlx::sqlite::SqliteJournalMode::Wal)
            .synchronous(sqlx::sqlite::SqliteSynchronous::Normal)
            .busy_timeout(Duration::from_secs(5));
        let database = SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(options)
            .await?;
        sqlx::query("CREATE TABLE IF NOT EXISTS admin_user(username TEXT PRIMARY KEY,password_hash TEXT NOT NULL)").execute(&database).await?;
        sqlx::query("CREATE TABLE IF NOT EXISTS admin_sessions(token TEXT PRIMARY KEY,last_seen INTEGER NOT NULL)").execute(&database).await?;
        Ok(Self {
            database,
            login_slots: Arc::new(Semaphore::new(4)),
        })
    }

    pub async fn configured(&self) -> Result<bool> {
        Ok(
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM admin_user")
                .fetch_one(&self.database)
                .await?
                > 0,
        )
    }

    pub async fn login(&self, password: String) -> Result<Option<String>> {
        let _permit = self
            .login_slots
            .acquire()
            .await
            .map_err(|_| anyhow::anyhow!("login limiter stopped"))?;
        let hash = sqlx::query_scalar::<_, String>(
            "SELECT password_hash FROM admin_user WHERE username='admin'",
        )
        .fetch_optional(&self.database)
        .await?;
        let Some(hash) = hash else { return Ok(None) };
        let valid = tokio::task::spawn_blocking(move || verify(&password, &hash)).await?;
        if !valid {
            tokio::time::sleep(Duration::from_millis(500)).await;
            return Ok(None);
        }
        let token = uuid::Uuid::new_v4().to_string();
        sqlx::query("INSERT INTO admin_sessions(token,last_seen) VALUES(?,?)")
            .bind(&token)
            .bind(now() as i64)
            .execute(&self.database)
            .await?;
        Ok(Some(token))
    }

    pub async fn valid_session(&self, token: &str) -> Result<bool> {
        let seen =
            sqlx::query_scalar::<_, i64>("SELECT last_seen FROM admin_sessions WHERE token=?")
                .bind(token)
                .fetch_optional(&self.database)
                .await?;
        let Some(seen) = seen else { return Ok(false) };
        if now().saturating_sub(seen.max(0) as u64) > SESSION_SECONDS {
            sqlx::query("DELETE FROM admin_sessions WHERE token=?")
                .bind(token)
                .execute(&self.database)
                .await?;
            return Ok(false);
        }
        sqlx::query("UPDATE admin_sessions SET last_seen=? WHERE token=?")
            .bind(now() as i64)
            .bind(token)
            .execute(&self.database)
            .await?;
        Ok(true)
    }

    pub async fn logout(&self, token: &str) -> Result<()> {
        sqlx::query("DELETE FROM admin_sessions WHERE token=?")
            .bind(token)
            .execute(&self.database)
            .await?;
        Ok(())
    }
}

pub async fn reset_password(config_dir: &Path, password: Option<String>) -> Result<()> {
    let auth = AuthService::open(config_dir).await?;
    let generated = password.is_none();
    let password =
        password.unwrap_or_else(|| uuid::Uuid::new_v4().simple().to_string()[..16].to_owned());
    if password.len() < 8 {
        bail!("密码至少需要 8 个字符")
    }
    let value = password.clone();
    let hash = tokio::task::spawn_blocking(move || hash(&value)).await??;
    sqlx::query("INSERT INTO admin_user(username,password_hash) VALUES('admin',?) ON CONFLICT(username) DO UPDATE SET password_hash=excluded.password_hash")
        .bind(hash).execute(&auth.database).await.context("保存管理员密码")?;
    sqlx::query("DELETE FROM admin_sessions")
        .execute(&auth.database)
        .await?;
    eprintln!("已重置用户 admin 的密码，旧登录会话已全部失效。");
    if generated {
        println!("{password}");
        eprintln!("请立即登录。以上密码只显示一次。");
    }
    Ok(())
}

fn hash(password: &str) -> Result<String> {
    Ok(Argon2::default()
        .hash_password(password.as_bytes(), &SaltString::generate(&mut OsRng))?
        .to_string())
}
fn verify(password: &str, hash: &str) -> bool {
    PasswordHash::new(hash).ok().is_some_and(|parsed| {
        Argon2::default()
            .verify_password(password.as_bytes(), &parsed)
            .is_ok()
    })
}
fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}
pub fn cookie_token(headers: &axum::http::HeaderMap) -> Option<&str> {
    headers
        .get(axum::http::header::COOKIE)?
        .to_str()
        .ok()?
        .split(';')
        .map(str::trim)
        .find_map(|part| part.strip_prefix("tile_cache_session="))
}
pub type SharedAuth = Arc<AuthService>;

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn login_creates_a_valid_session() {
        let directory = tempfile::tempdir().unwrap();
        let auth = AuthService::open(directory.path()).await.unwrap();
        let password_hash = hash("password123").unwrap();
        sqlx::query("INSERT INTO admin_user(username,password_hash) VALUES('admin',?)")
            .bind(password_hash)
            .execute(&auth.database)
            .await
            .unwrap();
        assert!(auth.login("wrong-password".into()).await.unwrap().is_none());
        let token = auth.login("password123".into()).await.unwrap().unwrap();
        assert!(auth.valid_session(&token).await.unwrap());
        auth.logout(&token).await.unwrap();
        assert!(!auth.valid_session(&token).await.unwrap());
    }
}
