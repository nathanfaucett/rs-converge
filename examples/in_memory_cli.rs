use converge::{DirectRowCodec, Engine, InMemoryKernel, SqlTranslator};

fn main() {
    futures::executor::block_on(examples_util::cli(
        Engine::new(InMemoryKernel::new(), DirectRowCodec),
        SqlTranslator,
    ));
}
