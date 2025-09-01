use clap::Parser;

#[derive(Parser)]
#[command(name = "schema-generator")]
#[command(about = "Pixelle schema-generator utility")]
struct Args {
    #[arg(short, long)]
    verbose: bool,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    
    if args.verbose {
        println!("Running schema-generator in verbose mode");
    }
    
    println!("schema-generator completed successfully");
    Ok(())
}
