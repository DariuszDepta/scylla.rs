/*
 * ScyllaDB, Rust and Actix Web
 *
 * MIT license
 *
 * Copyright (c) 2022 Dariusz Depta Engos Software
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

//! Implementation of the access to storage.

use crate::entity::RowEntity;
use crate::errors::*;
use scylla::transport::session::PoolSize;
use scylla::{IntoTypedRows, Session, SessionBuilder};
use std::env;
use std::num::NonZeroUsize;

/// Storage.
pub struct Storage {
  uri: String,
  session: Option<Session>,
}

impl Storage {
  /// Creates a new storage.
  pub fn new() -> Self {
    let uri = env::var("SCYLLA_URI").unwrap_or_else(|_| "127.0.0.1:9042".to_string());
    Self { uri, session: None }
  }
  /// Connects to database.
  pub async fn connect(&mut self) -> Result<()> {
    let session: Session = SessionBuilder::new()
      .known_node(&self.uri)
      .pool_size(PoolSize::PerHost(NonZeroUsize::new(4).unwrap()))
      .build()
      .await
      .map_err(err_new_session)?;
    self.session = Some(session);
    println!("Database connection established.");
    Ok(())
  }
  /// Initializes the database.
  pub async fn initialize(&self) -> Result<()> {
    let session = self.session.as_ref().ok_or_else(err_no_session)?;
    session
      .query(
        "CREATE KEYSPACE IF NOT EXISTS ks WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor' : 1}",
        &[],
      )
      .await
      .map_err(err_query)?;
    session
      .query("CREATE TABLE IF NOT EXISTS ks.t (a int, b int, c text, primary key (a, b))", &[])
      .await
      .map_err(err_query)?;
    println!("Database structure initialized.");

    session
      .query("INSERT INTO ks.t (a, b, c) VALUES (?, ?, ?)", (3, 4, "def"))
      .await
      .map_err(err_query)?;

    Ok(())
  }
  /// Retrieve all rows.
  pub async fn get_rows(&self) -> Result<Vec<RowEntity>> {
    let mut rows = vec![];
    let session = self.session.as_ref().ok_or_else(err_no_session)?;
    if let Some(data_rows) = session.query("SELECT a, b, c FROM ks.t", &[]).await.map_err(err_query)?.rows {
      for row_data in data_rows.into_typed::<RowEntity>() {
        let row_entity = row_data.map_err(err_from_row)?;
        rows.push(row_entity);
      }
    }
    Ok(rows)
  }
}

// let uri = env::var("SCYLLA_URI").unwrap_or_else(|_| "127.0.0.1:9042".to_string());
// println!("Connecting to ScyllaDB: {} ...", uri);
// let session: Session = SessionBuilder::new()
//   .known_node(uri)
//   .pool_size(PoolSize::PerHost(NonZeroUsize::new(4).unwrap()))
//   .build()
//   .await?;
// println!("Established session with ScyllaDB ...");
// session
//   .query(
//     "CREATE KEYSPACE IF NOT EXISTS ks WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor' : 1}",
//     &[],
//   )
//   .await?;
// session
//   .query("CREATE TABLE IF NOT EXISTS ks.t (a int, b int, c text, primary key (a, b))", &[])
//   .await?;
// session.query("INSERT INTO ks.t (a, b, c) VALUES (?, ?, ?)", (3, 4, "def")).await?;
// session.query("INSERT INTO ks.t (a, b, c) VALUES (1, 2, 'abc')", &[]).await?;
// let prepared = session.prepare("INSERT INTO ks.t (a, b, c) VALUES (?, 7, ?)").await?;
// session.execute(&prepared, (42_i32, "I'm prepared 1!")).await?;
// session.execute(&prepared, (43_i32, "I'm prepared 2!")).await?;
// session.execute(&prepared, (44_i32, "I'm prepared 3!")).await?;
// if let Some(rows) = session.query("SELECT a, b, c FROM ks.t", &[]).await?.rows {
//   for row in rows.into_typed::<(i32, i32, String)>() {
//     let (a, b, c) = row?;
//     println!("a, b, c: {}, {}, {}", a, b, c);
//   }
// }
