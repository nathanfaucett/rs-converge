use db::{engine::Engine, sql_translator::SqlTranslator};

#[tokio::main]
async fn main() {
    db_examples_util::cli(Engine::new(unimplemented!()), SqlTranslator).await;
}
