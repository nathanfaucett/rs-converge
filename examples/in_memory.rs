use db::{
  engine::{DefaultBTreeManager, Engine},
  sql_translator::SqlTranslator,
};

#[tokio::main]
async fn main() {
  db_examples_util::run(
    Engine::new(DefaultBTreeManager::with_in_memory_factory()),
    SqlTranslator,
  )
  .await;
}
