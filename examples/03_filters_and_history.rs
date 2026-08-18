use metatrader_rs::{try_discover_mt5_pipe, HistoryFilter, Mt5Client, OrderFilter, PositionFilter};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let pipe = try_discover_mt5_pipe()?;
    let mut client = Mt5Client::new();
    client.initialize(Some(&pipe))?;

    let total_positions = client.positions_total()?;
    let total_orders = client.orders_total()?;
    println!(
        "positions_total={} orders_total={}",
        total_positions, total_orders
    );

    let positions = client.positions_get_filtered(Some(&PositionFilter {
        symbol: "EURUSD".to_string(),
        ..PositionFilter::default()
    }))?;
    println!("EURUSD positions={}", positions.len());

    let orders = client.orders_get_filtered(Some(&OrderFilter {
        group: "*USD*,!USDJPY".to_string(),
        ..OrderFilter::default()
    }))?;
    println!("group-filtered pending orders={}", orders.len());

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_secs() as i64;
    let from = now - 7 * 24 * 60 * 60;

    let history_orders = client.history_orders_get_filtered(Some(&HistoryFilter {
        date_from: from,
        date_to: now,
        group: "*USD*".to_string(),
        ..HistoryFilter::default()
    }))?;
    println!(
        "weekly history orders (group=*USD*)={}",
        history_orders.len()
    );

    let history_deals = client.history_deals_get_filtered(Some(&HistoryFilter {
        date_from: from,
        date_to: now,
        symbol: "EURUSD".to_string(),
        ..HistoryFilter::default()
    }))?;
    println!("weekly EURUSD deals={}", history_deals.len());

    Ok(())
}
