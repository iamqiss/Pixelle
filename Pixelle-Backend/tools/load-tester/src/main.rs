use clap::Parser;

#[derive(Parser)]
#[command(name = "load-tester")]
#[command(about = "Pixelle load-tester utility")]
struct Args {
    #[arg(short, long)]
    verbose: bool,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    
    if args.verbose {
        println!("Running load-tester in verbose mode");
    }
    
    println!("load-tester completed successfully");
    Ok(())
}
