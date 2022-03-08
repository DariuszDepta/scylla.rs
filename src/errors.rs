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

use scylla::cql_to_rust::FromRowError;
use scylla::transport::errors::{NewSessionError, QueryError};

/// Common result type.
pub type Result<T, E = ServerError> = std::result::Result<T, E>;

/// Common error definition.
#[derive(Debug)]
pub struct ServerError(String);

impl std::fmt::Display for ServerError {
  /// Implementation of [Display](std::fmt::Display) trait for [ServerError].
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    write!(f, "{}", self.0)
  }
}

impl From<std::io::Error> for ServerError {
  /// Converts [std::io::Error] into [ServerError].
  fn from(e: std::io::Error) -> Self {
    Self(e.to_string())
  }
}

///
pub fn err_internal(e: std::io::Error) -> ServerError {
  e.into()
}

///
pub fn err_no_session() -> ServerError {
  ServerError("session not initialized".to_string())
}

///
pub fn err_new_session(e: NewSessionError) -> ServerError {
  ServerError(format!("{:?}", e))
}

///
pub fn err_query(e: QueryError) -> ServerError {
  ServerError(format!("{:?}", e))
}

///
pub fn err_from_row(e: FromRowError) -> ServerError {
  ServerError(format!("{:?}", e))
}

///
pub fn err_no_access_to_storage() -> ServerError {
  ServerError("no access to storage".to_string())
}
