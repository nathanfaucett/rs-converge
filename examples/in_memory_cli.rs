use db::{DirectRowCodec, Engine, InMemoryKernel, SqlTranslator};

fn main() {
    futures::executor::block_on(db_examples_util::cli(
        Engine::new(InMemoryKernel::new(), DirectRowCodec),
        SqlTranslator,
    ));
}
