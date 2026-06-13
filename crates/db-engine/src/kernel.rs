use db_btree::BTree;
use db_value::Row;
use uuid::Uuid;

pub trait EngineKernel {
  type TableBTree: BTree<Uuid, Row>;
  type IndexBTree: BTree<Row, Uuid>;

  fn table_btree(&self, name: &str) -> impl Future<Output = Self::TableBTree>;
  fn index_btree(&self, name: &str) -> impl Future<Output = Self::IndexBTree>;
}
