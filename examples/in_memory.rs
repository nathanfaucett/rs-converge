use db::{
    engine::{DirectRowCodec, Engine, InMemoryKernel},
    sql_translator::SqlTranslator,
};

#[tokio::main]
async fn main() {
    db_examples_util::run(
        Engine::new(InMemoryKernel::new(), DirectRowCodec),
        SqlTranslator,
    )
    .await;
}
