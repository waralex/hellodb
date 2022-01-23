use std::env;
use rustyline::error::ReadlineError;
use rustyline::Editor;
use hellodb::execute::Plan;
use hellodb::db::DB;
use hellodb::DBResult;
use cli_table::print_stdout;

fn on_line(db:&DB, l:&str) -> DBResult<()>
{
    if l.len() > 0
    {
        let mut plan = Plan::from_sql(&db, &l)?;
        plan.execute()?;
        print_stdout(plan.result_cli_table(100)).unwrap();
    }
    Ok(())
}

fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() != 2
    {
        println!("Simple console client. Usage: hellodb <path_to_database>");
    }
    let mut rl = Editor::<()>::new();
    let db = DB::open(&args[1]).unwrap();
    loop {
        let readline = rl.readline(">> ");
        match readline {
            Ok(line) => {
                rl.add_history_entry(line.as_str());

                match on_line(&db, &line)
                {
                    Err(e) => println!("{}", e),
                    _ => {}
                }
            },
            Err(ReadlineError::Interrupted) => {
                break
            },
            Err(ReadlineError::Eof) => {
                break
            },
            Err(err) => {
                println!("Error: {:?}", err);
                break
            }
        }
    }
    println!("Thanks!");
}