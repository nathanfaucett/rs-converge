use db::{
  engine::{DefaultBTreeManager, Engine},
  redb::RedbBTreeFactory,
  sql_translator::SqlTranslator,
};
use uuid::Uuid;

#[tokio::main]
async fn main() {
  let tmp_dir = std::env::temp_dir();
  let tmp_file = tmp_dir.join(format!("redb-{}.db", Uuid::now_v7()));
  let db = redb::Database::create(tmp_file).expect("failed to create redb database");

  db_examples_util::run(
    Engine::new(DefaultBTreeManager::new(RedbBTreeFactory::new(db))),
    SqlTranslator,
  )
  .await;
}
