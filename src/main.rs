/*
 * ScyllaDB and Rust
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

extern crate anyhow;
extern crate scylla;
extern crate tokio;

use anyhow::Result;
use scylla::transport::session::PoolSize;
use scylla::{IntoTypedRows, Session, SessionBuilder};
use std::env;
use std::num::NonZeroUsize;

#[tokio::main]
async fn main() -> Result<()> {
  let uri = env::var("SCYLLA_URI").unwrap_or_else(|_| "127.0.0.1:9042".to_string());
  println!("Connecting to ScyllaDB: {} ...", uri);
  let session: Session = SessionBuilder::new()
    .known_node(uri)
    .pool_size(PoolSize::PerHost(NonZeroUsize::new(4).unwrap()))
    .build()
    .await?;
  println!("Established session with ScyllaDB ...");
  session
    .query(
      "CREATE KEYSPACE IF NOT EXISTS ks WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor' : 1}",
      &[],
    )
    .await?;
  session
    .query("CREATE TABLE IF NOT EXISTS ks.t (a int, b int, c text, primary key (a, b))", &[])
    .await?;
  session.query("INSERT INTO ks.t (a, b, c) VALUES (?, ?, ?)", (3, 4, "def")).await?;
  session.query("INSERT INTO ks.t (a, b, c) VALUES (1, 2, 'abc')", &[]).await?;
  let prepared = session.prepare("INSERT INTO ks.t (a, b, c) VALUES (?, 7, ?)").await?;
  session.execute(&prepared, (42_i32, "I'm prepared 1!")).await?;
  session.execute(&prepared, (43_i32, "I'm prepared 2!")).await?;
  session.execute(&prepared, (44_i32, "I'm prepared 3!")).await?;
  if let Some(rows) = session.query("SELECT a, b, c FROM ks.t", &[]).await?.rows {
    for row in rows.into_typed::<(i32, i32, String)>() {
      let (a, b, c) = row?;
      println!("a, b, c: {}, {}, {}", a, b, c);
    }
  }
  Ok(())
}
