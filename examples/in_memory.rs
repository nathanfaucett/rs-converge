use db::{
  engine::{Engine, InMemoryBTreeManager},
  sql_translator::SqlTranslator,
};

#[tokio::main]
async fn main() {
  db_examples_util::run(Engine::new(InMemoryBTreeManager::new()), SqlTranslator).await;
}
