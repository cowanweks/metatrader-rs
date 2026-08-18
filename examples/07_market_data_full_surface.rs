use metatrader_rs::{Mt5Client, try_discover_mt5_pipe};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let pipe = try_discover_mt5_pipe()?;
    let mut client = Mt5Client::new();
    client.initialize(Some(&pipe))?;

    let total = client.symbols_total()?;
    println!("symbols_total={}", total);

    let all = client.symbols_get()?;
    println!("symbols_get count={}", all.len());

    let symbol = std::env::var("MT5_SYMBOL")
        .ok()
        .or_else(|| all.first().map(|s| s.name.clone()))
        .ok_or("no symbols available")?;

    let info = client
        .symbol_info(&symbol)?
        .ok_or_else(|| format!("symbol not found: {symbol}"))?;

    println!(
        "symbol={} digits={} point={} spread={}",
        info.name, info.digits, info.point, info.spread
    );

    let selected = client.symbol_select(&symbol, true)?;
    println!("symbol_select({}, true)={}", symbol, selected);

    if let Some(tick) = client.symbol_info_tick(&symbol)? {
        println!(
            "symbol_info_tick {} bid={} ask={} last={}",
            symbol, tick.bid, tick.ask, tick.last
        );
    }

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_secs() as i64;

    let from_rates = now - 2 * 60 * 60;
    let from_ticks = now - 10 * 60;

    let rates_pos = client.copy_rates_from_pos(&symbol, 1, 0, 5)?;
    println!("copy_rates_from_pos count={}", rates_pos.len());

    let rates_from = client.copy_rates_from(&symbol, 1, from_rates, 5)?;
    println!("copy_rates_from count={}", rates_from.len());

    let rates_range = client.copy_rates_range(&symbol, 1, from_rates, now)?;
    println!("copy_rates_range count={}", rates_range.len());

    let ticks_from = client.copy_ticks_from(&symbol, from_ticks, 50, 0)?;
    println!("copy_ticks_from count={}", ticks_from.len());

    let ticks_range = client.copy_ticks_range(&symbol, from_ticks, now, 0)?;
    println!("copy_ticks_range count={}", ticks_range.len());

    Ok(())
}
