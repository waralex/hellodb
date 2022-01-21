use std::env;
use rustyline::error::ReadlineError;
use rustyline::Editor;
use hellodb::execute::constructor::*;
use hellodb::db::DB;


fn on_line(db:&DB, l:&str)
{
    match make_plan_by_sql(&db, &l)  {
        Ok(_) => {},
        Err(e) => {println!("{}", e)}
    }
}

fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() != 2
    {
        println!("Simple console client. Usage: hellodb <path_to_database>");
    }
    let mut rl = Editor::<()>::new();
    let mut db = DB::open(&args[1]).unwrap();
    loop {
        let readline = rl.readline(">> ");
        match readline {
            Ok(line) => {
                rl.add_history_entry(line.as_str());

                on_line(&db, &line);
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