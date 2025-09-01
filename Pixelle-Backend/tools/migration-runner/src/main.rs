use clap::Parser;

#[derive(Parser)]
#[command(name = "migration-runner")]
#[command(about = "Pixelle migration-runner utility")]
struct Args {
    #[arg(short, long)]
    verbose: bool,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    
    if args.verbose {
        println!("Running migration-runner in verbose mode");
    }
    
    println!("migration-runner completed successfully");
    Ok(())
}
