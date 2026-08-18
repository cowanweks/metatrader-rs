use metatrader_rs::{try_discover_mt5_pipe, Mt5Client};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let pipe = try_discover_mt5_pipe()?;
    let mut client = Mt5Client::new();
    client.initialize(Some(&pipe))?;

    let symbols = client.symbols_get_by_group(Some("*USD*,!XAUUSD"))?;
    println!("Matched symbols: {}", symbols.len());

    for symbol in symbols.iter().take(10) {
        println!(
            "{} bid={} ask={} spread={} selected={}",
            symbol.name, symbol.bid, symbol.ask, symbol.spread, symbol.select
        );
    }

    if let Some(symbol) = symbols.first() {
        client.symbol_select(&symbol.name, true)?;

        if let Some(tick) = client.symbol_info_tick(&symbol.name)? {
            println!(
                "Tick {} bid={} ask={} last={} time_msc={}",
                symbol.name, tick.bid, tick.ask, tick.last, tick.time_msc
            );
        } else {
            println!("No tick available for {}", symbol.name);
        }

        let rates = client.copy_rates_from_pos(&symbol.name, 1, 0, 5)?;
        println!("Recent M1 bars for {}: {}", symbol.name, rates.len());
        for rate in rates {
            println!(
                "t={} o={} h={} l={} c={} vol={}",
                rate.time, rate.open, rate.high, rate.low, rate.close, rate.tick_volume
            );
        }
    }

    Ok(())
}
