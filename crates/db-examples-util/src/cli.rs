use db_btree::{BTreeFactory, BTreeManager};
use db_engine::Engine;
use db_query::Translator;
use rustyline::DefaultEditor;
use rustyline::error::ReadlineError;

pub async fn cli<M, F, T>(engine: Engine<M, F>, translator: T)
where
  M: BTreeManager<F>,
  F: BTreeFactory,
  T: Translator,
{
  println!("=== DB Engine CLI Interface (with Arrow Key History) ===");
  println!(
    "Type your SQL queries below. Press Up/Down arrows to scroll history. Type 'exit' to quit.\n"
  );

  // 2. Initialize the rustyline editor
  let mut rl = DefaultEditor::new().expect("Failed to initialize line reader");

  // Loop indefinitely
  loop {
    // 3. Prompt user and await input (rustyline handles the stdout flush automatically)
    let readline = rl.readline("db_engine> ");

    match readline {
      Ok(line) => {
        let query = line.trim();

        // Exit condition
        if query.eq_ignore_ascii_case("exit") {
          println!("Goodbye!");
          break;
        }

        if query.is_empty() {
          continue;
        }

        // 4. Save the valid query to history so Up/Down arrows can recall it
        let _ = rl.add_history_entry(query);

        // 5. Execute the dynamic query
        match engine.translate_and_execute(query, &translator).await {
          Ok(res) => {
            println!("Execution successful. Rows returned: {}", res.rows.len());
            for (idx, row) in res.rows.iter().enumerate() {
              println!("[Row {}]: {:?}", idx + 1, row);
            }
          }
          Err(err) => {
            eprintln!("SQL Error: {:?}", err);
          }
        }
        println!();
      }
      // Handle Ctrl-C (interrupt) gracefully
      Err(ReadlineError::Interrupted) => {
        println!("Ctrl-C detected. Type 'exit' or press Ctrl-D to quit.");
      }
      // Handle Ctrl-D (EOF / end of input) gracefully
      Err(ReadlineError::Eof) => {
        println!("Goodbye!");
        break;
      }
      Err(err) => {
        eprintln!("Error reading line: {:?}", err);
        break;
      }
    }
  }
}
