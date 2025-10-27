use clap::Parser;

#[derive(Parser, Debug)]
#[command(name = "Nostr stream data processing", version, about, long_about = None)]
pub struct Cli {
    #[arg(short, long, default_value_t = 4)]
    pub workers: usize,
}
