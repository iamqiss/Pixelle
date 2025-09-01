use clap::Parser;

#[derive(Parser)]
#[command(name = "chaos-monkey")]
#[command(about = "Pixelle chaos-monkey utility")]
struct Args {
    #[arg(short, long)]
    verbose: bool,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    
    if args.verbose {
        println!("Running chaos-monkey in verbose mode");
    }
    
    println!("chaos-monkey completed successfully");
    Ok(())
}
