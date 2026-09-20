use converge::{Database, SqlTranslator};

fn main() {
    futures::executor::block_on(examples_util::cli(
        Database::in_memory().into_inner(),
        SqlTranslator,
    ));
}
