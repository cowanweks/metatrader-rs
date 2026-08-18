use metatrader_rs::{try_discover_mt5_pipe, Mt5Client};
use std::sync::mpsc::TryRecvError;
use std::time::{Duration, Instant};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let pipe = try_discover_mt5_pipe()?;
    let mut client = Mt5Client::new();
    client.initialize(Some(&pipe))?;

    let symbols = vec![
        "EURUSD".to_string(),
        "USDJPY".to_string(),
        "GBPUSD".to_string(),
    ];

    for symbol in &symbols {
        let _ = client.symbol_select(symbol, true);
    }

    let mut poll = client.poll_quotes(&symbols, Duration::from_millis(500))?;

    let started = Instant::now();
    while started.elapsed() < Duration::from_secs(10) {
        if let Some(err) = poll.try_recv_error() {
            return Err(Box::new(err));
        }

        match poll.try_recv() {
            Ok(update) => {
                for (symbol, tick) in update {
                    println!(
                        "{} bid={} ask={} last={} t_msc={}",
                        symbol, tick.bid, tick.ask, tick.last, tick.time_msc
                    );
                }
            }
            Err(TryRecvError::Empty) => {
                if let Ok(update) = poll.recv() {
                    for (symbol, tick) in update {
                        println!(
                            "{} bid={} ask={} last={} t_msc={}",
                            symbol, tick.bid, tick.ask, tick.last, tick.time_msc
                        );
                    }
                }
            }
            Err(TryRecvError::Disconnected) => {
                break;
            }
        }
    }

    poll.stop();
    Ok(())
}
