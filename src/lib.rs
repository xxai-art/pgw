#![feature(async_fn_in_trait)]
use std::{cmp::min, sync::Arc};

use tokio::{sync::RwLock, time};
pub use tokio_postgres::{
  self, connect, error::SqlState, types::ToSql, Client, Error, NoTls, Row, Statement, ToStatement,
};

fn hidden_password(s: &str) -> String {
  let at_index = s.rfind('@').unwrap();
  let password_start = s[..at_index].rfind(':').unwrap() + 1;
  let password_end = at_index;
  let mut result = s.to_string();
  result.replace_range(
    password_start..password_end,
    &"*".repeat(min(3, password_end - password_start)),
  );
  result
}

#[derive(Clone)]
pub struct Sql(Arc<_Sql>);

pub trait IntoStatement<T: ToStatement> {
  async fn into(self) -> Result<T, Error>;
}

impl<T: ToStatement> IntoStatement<T> for T {
  async fn into(self) -> Result<T, Error> {
    Ok(self)
  }
}

async fn into_statement(sql: &Sql) -> Result<Statement, Error> {
  let sql = &sql.0;
  loop {
    if let Some(st) = sql.st.read().await.as_ref() {
      return Ok(st.clone());
    }

    let st = sql.pg.prepare(&*sql.sql).await?;
    *sql.st.write().await = Some(st);
  }
}

impl IntoStatement<Statement> for &Sql {
  async fn into(self) -> Result<Statement, Error> {
    into_statement(self).await
  }
}

impl<T: AsRef<Sql>> IntoStatement<Statement> for T {
  async fn into(self) -> Result<Statement, Error> {
    into_statement(self.as_ref()).await
  }
}

pub struct _Pg {
  pub uri: String,
  pub sql_li: Vec<Sql>,
  _client: Option<Client>,
}

#[derive(Clone)]
pub struct Pg(Arc<RwLock<_Pg>>);

fn is_close(err: &Error) -> bool {
  let code = err.code();
  code.is_none() || code == Some(&SqlState::ADMIN_SHUTDOWN) || err.is_closed()
}

fn err_code<'a>(err: &'a Error) -> &'a str {
  let err_code = err.code();
  match err_code {
    Some(code) => code.code(),
    None => "",
  }
}

macro_rules! client {
  ($self:ident, $body:ident) => {{
    let pg = &$self.0;
    'outer: loop {
      {
        let pg = &pg.read().await;
        if let Some(client) = &pg._client {
          loop {
            match $body!(client).await {
              Ok(r) => return Ok(r),
              Err(err) => {
                let hidden_password_uri = hidden_password(&pg.uri);
                let code = err_code(&err);
                tracing::error!("❌ {hidden_password_uri} ERROR {code} : {err}");
                if is_close(&err) {
                  break;
                }
                return Err(err);
              }
            }
          }
        }
      }

      let mut _pg = pg.write().await;
      if _pg._client.is_some() {
        time::sleep(std::time::Duration::from_millis(100)).await;
        if _pg._client.is_some() {
          continue 'outer;
        }
      }
      let mut n = 0u64;
      let uri = _pg.uri.clone();
      loop {
        match connect(&format!("postgres://{}", uri), NoTls).await {
          Ok((client, connection)) => {
            _pg._client = Some(client);

            let pg = pg.clone();
            tokio::spawn(async move {
              if let Err(e) = connection.await {
                let code = err_code(&e);
                let hidden_password_uri = hidden_password(&uri);
                tracing::error!("❌ {hidden_password_uri} ERROR {code} → {e}");

                if is_close(&e) {
                  let mut pg = pg.write().await;
                  pg._client = None;
                  for i in &mut pg.sql_li {
                    *i.0.st.write().await = None;
                  }
                  return;
                }
              }
            });
            break;
          }
          Err(err) => {
            n += 1;
            let hidden_password_uri = hidden_password(&uri);
            tracing::error!("❌ RETRY {n} → {hidden_password_uri} : {err}");
            time::sleep(std::time::Duration::from_secs(1)).await;
          }
        }
      }
    }
  }};
}

pub struct _Sql {
  sql: String,
  st: RwLock<Option<Statement>>,
  pg: Pg,
}

impl Pg {
  pub fn new(uri: impl Into<String>) -> Self {
    Self(Arc::new(RwLock::new(_Pg {
      uri: uri.into(),
      _client: None,
      sql_li: Vec::new(),
    })))
  }

  pub fn new_with_env(env: impl Into<String>) -> Self {
    let uri = std::env::var(env.into()).unwrap();
    Self::new(uri)
  }

  pub async fn query_one<T: ToStatement>(
    &self,
    statement: impl IntoStatement<T>,
    params: &[&(dyn ToSql + Sync)],
  ) -> Result<Row, Error> {
    let statement = statement.into().await?;
    macro_rules! query_one {
      ($client:ident) => {
        $client.query_one(&statement, params)
      };
    }
    client!(self, query_one)
  }

  pub async fn query<T: ToStatement>(
    &self,
    statement: impl IntoStatement<T>,
    params: &[&(dyn ToSql + Sync)],
  ) -> Result<Vec<Row>, Error> {
    let statement = statement.into().await?;
    macro_rules! query {
      ($client:ident) => {
        $client.query(&statement, params)
      };
    }
    client!(self, query)
  }

  pub async fn query_opt<T: ToStatement>(
    &self,
    statement: impl IntoStatement<T>,
    params: &[&(dyn ToSql + Sync)],
  ) -> Result<Option<Row>, Error> {
    let statement = statement.into().await?;
    macro_rules! query_opt {
      ($client:ident) => {
        $client.query_opt(&statement, params)
      };
    }
    client!(self, query_opt)
  }

  pub async fn execute<T: ToStatement>(
    &self,
    statement: impl IntoStatement<T>,
    params: &[&(dyn ToSql + Sync)],
  ) -> Result<u64, Error> {
    let statement = statement.into().await?;
    macro_rules! execute {
      ($client:ident) => {
        $client.execute(&statement, params)
      };
    }
    client!(self, execute)
  }

  pub async fn prepare(&self, query: impl AsRef<str>) -> Result<Statement, Error> {
    macro_rules! prepare {
      ($client:ident) => {
        $client.prepare(query.as_ref())
      };
    }
    client!(self, prepare)
  }

  pub fn sql(&self, query: impl Into<String>) -> Sql {
    let sql = Sql(Arc::new(_Sql {
      sql: query.into(),
      st: RwLock::new(None),
      pg: self.clone(),
    }));
    let sql_clone = sql.clone();
    let me = self.clone();
    futures::executor::block_on(async move {
      me.0.write().await.sql_li.push(sql_clone);
    });
    sql
  }
}
