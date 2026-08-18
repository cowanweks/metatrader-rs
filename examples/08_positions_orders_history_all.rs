use metatrader_rs::{HistoryFilter, Mt5Client, OrderFilter, PositionFilter, try_discover_mt5_pipe};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let pipe = try_discover_mt5_pipe()?;
    let mut client = Mt5Client::new();
    client.initialize(Some(&pipe))?;

    let symbols = client.symbols_get_by_group(Some("*USD*"))?;
    let fallback_symbol = symbols
        .first()
        .map(|s| s.name.clone())
        .unwrap_or_else(|| "EURUSD".to_string());
    let symbol = std::env::var("MT5_SYMBOL").unwrap_or(fallback_symbol);

    println!("positions_total={}", client.positions_total()?);
    println!("orders_total={}", client.orders_total()?);

    let positions_all = client.positions_get(None)?;
    println!("positions_get(None)={}", positions_all.len());

    let positions_symbol = client.positions_get(Some(&symbol))?;
    println!("positions_get(Some({}))={}", symbol, positions_symbol.len());

    let positions_filtered = client.positions_get_filtered(Some(&PositionFilter {
        group: "*USD*,!USDJPY".to_string(),
        ..PositionFilter::default()
    }))?;
    println!("positions_get_filtered(group)={}", positions_filtered.len());

    if let Some(position) = positions_all.first() {
        let by_ticket = client.positions_get_filtered(Some(&PositionFilter {
            ticket: position.ticket,
            ..PositionFilter::default()
        }))?;
        println!("positions_get_filtered(ticket={})={}", position.ticket, by_ticket.len());
    }

    let orders_all = client.orders_get(None)?;
    println!("orders_get(None)={}", orders_all.len());

    let orders_symbol = client.orders_get(Some(&symbol))?;
    println!("orders_get(Some({}))={}", symbol, orders_symbol.len());

    let orders_filtered = client.orders_get_filtered(Some(&OrderFilter {
        group: "*USD*,!USDJPY".to_string(),
        ..OrderFilter::default()
    }))?;
    println!("orders_get_filtered(group)={}", orders_filtered.len());

    if let Some(order) = orders_all.first() {
        let by_ticket = client.orders_get_filtered(Some(&OrderFilter {
            ticket: order.ticket,
            ..OrderFilter::default()
        }))?;
        println!("orders_get_filtered(ticket={})={}", order.ticket, by_ticket.len());
    }

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_secs() as i64;
    let from = now - 7 * 24 * 60 * 60;

    println!(
        "history_deals_total={}, history_orders_total={}",
        client.history_deals_total(from, now)?,
        client.history_orders_total(from, now)?
    );

    let deals = client.history_deals_get(from, now)?;
    println!("history_deals_get={}", deals.len());

    let deals_filtered = client.history_deals_get_filtered(Some(&HistoryFilter {
        date_from: from,
        date_to: now,
        symbol: symbol.clone(),
        group: "*USD*".to_string(),
        ..HistoryFilter::default()
    }))?;
    println!("history_deals_get_filtered={}", deals_filtered.len());

    if let Some(deal) = deals.first() {
        let by_ticket = client.history_deals_get_filtered(Some(&HistoryFilter {
            ticket: deal.ticket,
            ..HistoryFilter::default()
        }))?;
        println!("history_deals_get_filtered(ticket={})={}", deal.ticket, by_ticket.len());
    }

    let history_orders = client.history_orders_get(from, now)?;
    println!("history_orders_get={}", history_orders.len());

    let history_orders_filtered = client.history_orders_get_filtered(Some(&HistoryFilter {
        date_from: from,
        date_to: now,
        symbol,
        group: "*USD*".to_string(),
        ..HistoryFilter::default()
    }))?;
    println!("history_orders_get_filtered={}", history_orders_filtered.len());

    if let Some(order) = history_orders.first() {
        let by_ticket = client.history_orders_get_filtered(Some(&HistoryFilter {
            ticket: order.ticket,
            ..HistoryFilter::default()
        }))?;
        println!("history_orders_get_filtered(ticket={})={}", order.ticket, by_ticket.len());
    }

    Ok(())
}
