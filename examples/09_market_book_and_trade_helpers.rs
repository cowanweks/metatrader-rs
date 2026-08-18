use metatrader_rs::{
    Mt5Client, OrderFilling, OrderTime, OrderType, TradeAction, TradeRequest, try_discover_mt5_pipe,
};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let pipe = try_discover_mt5_pipe()?;
    let mut client = Mt5Client::new();
    client.initialize(Some(&pipe))?;

    let symbol = std::env::var("MT5_SYMBOL").unwrap_or_else(|_| "EURUSD".to_string());
    let _ = client.symbol_select(&symbol, true)?;

    let subscribed = client.market_book_add(&symbol)?;
    println!("market_book_add={} for {}", subscribed, symbol);

    let book = client.market_book_get(&symbol)?;
    println!("market_book_get levels={}", book.len());

    let released = client.market_book_release(&symbol)?;
    println!("market_book_release={} for {}", released, symbol);

    let tick = client
        .symbol_info_tick(&symbol)?
        .ok_or_else(|| format!("tick unavailable for {symbol}"))?;

    let request = TradeRequest {
        action: TradeAction::Deal,
        symbol: symbol.clone(),
        volume: 0.01,
        price: tick.ask,
        order_type: OrderType::Buy,
        type_filling: OrderFilling::Ioc,
        type_time: OrderTime::Gtc,
        deviation: 20,
        comment: "metatrader-rs helper example".to_string(),
        ..TradeRequest::default()
    };

    request.validate()?;

    let check = client.order_check(&request)?;
    println!("order_check retcode={} comment={}", check.retcode, check.comment);

    let margin = client.order_calc_margin(TradeAction::Deal as i32, &symbol, request.volume, request.price)?;
    let profit = client.order_calc_profit(
        TradeAction::Deal as i32,
        &symbol,
        request.volume,
        request.price,
        request.price + 5.0 * 0.0001,
    )?;

    println!("order_calc_margin={} order_calc_profit(+5 points)={}", margin, profit);

    if std::env::var("MT5_SEND_ORDER_OPEN").ok().as_deref() == Some("1") {
        let sent = client.open_order(&symbol, 0.01, OrderType::Buy, tick.ask)?;
        println!("open_order retcode={} ok={}", sent.retcode, sent.is_ok());
    }

    if std::env::var("MT5_SEND_ORDER_BUY").ok().as_deref() == Some("1") {
        let sent = client.buy(&symbol, 0.01, tick.ask)?;
        println!("buy retcode={} ok={}", sent.retcode, sent.is_ok());
    }

    if std::env::var("MT5_SEND_ORDER_SELL").ok().as_deref() == Some("1") {
        let sent = client.sell(&symbol, 0.01, tick.bid)?;
        println!("sell retcode={} ok={}", sent.retcode, sent.is_ok());
    }

    Ok(())
}
