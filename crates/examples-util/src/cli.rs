use engine::{Engine, Kernel, RowCodec};
use query::Translator;
use rustyline::DefaultEditor;
use rustyline::error::ReadlineError;

pub async fn cli<K, R, T>(engine: Engine<K, R>, translator: T)
where
    K: Kernel,
    R: RowCodec<K::Transaction>,
    T: Translator,
{
    println!("=== DB Engine CLI Interface (with Arrow Key History) ===");
    println!("Type SQL below. Type 'exit' to quit.\n");
    let mut editor = DefaultEditor::new().expect("Failed to initialize line reader");
    while read_command(&mut editor, &engine, &translator).await {}
}

async fn read_command<K, R, T>(
    editor: &mut DefaultEditor,
    engine: &Engine<K, R>,
    translator: &T,
) -> bool
where
    K: Kernel,
    R: RowCodec<K::Transaction>,
    T: Translator,
{
    match editor.readline("engine> ") {
        Ok(line) => run_command(editor, engine, translator, line.trim()).await,
        Err(ReadlineError::Interrupted) => {
            println!("Ctrl-C detected. Type 'exit' or press Ctrl-D to quit.");
            true
        }
        Err(ReadlineError::Eof) => {
            println!("Goodbye!");
            false
        }
        Err(error) => {
            eprintln!("Error reading line: {error:?}");
            false
        }
    }
}

async fn run_command<K, R, T>(
    editor: &mut DefaultEditor,
    engine: &Engine<K, R>,
    translator: &T,
    query: &str,
) -> bool
where
    K: Kernel,
    R: RowCodec<K::Transaction>,
    T: Translator,
{
    if query.eq_ignore_ascii_case("exit") {
        println!("Goodbye!");
        return false;
    }
    if query.is_empty() {
        return true;
    }
    execute(editor, engine, translator, query).await;
    true
}

async fn execute<K, R, T>(
    editor: &mut DefaultEditor,
    engine: &Engine<K, R>,
    translator: &T,
    query: &str,
) where
    K: Kernel,
    R: RowCodec<K::Transaction>,
    T: Translator,
{
    let _ = editor.add_history_entry(query);
    match engine.translate_and_execute(query, translator).await {
        Ok(results) => print_results(results),
        Err(error) => eprintln!("SQL Error: {error:?}"),
    }
    println!();
}

fn print_results(results: Vec<query::QueryResult>) {
    for result in results {
        println!("Execution successful. Rows returned: {}", result.rows.len());
        for (index, row) in result.rows.iter().enumerate() {
            println!("[Row {}]: {row:?}", index + 1);
        }
    }
}
