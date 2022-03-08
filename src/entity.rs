use scylla::macros::FromRow;

#[derive(Debug, FromRow)]
pub struct RowEntity {
  pub a: i32,
  pub b: i32,
  pub c: String,
}
