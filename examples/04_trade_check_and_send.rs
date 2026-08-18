use metatrader_rs::{
    Mt5Client, OrderFilling, OrderTime, OrderType, TradeAction, TradeRequest, try_discover_mt5_pipe,
};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let pipe = try_discover_mt5_pipe()?;
    let mut client = Mt5Client::new();
    client.initialize(Some(&pipe))?;

    let symbol = "EURUSD";
    let info = client
        .symbol_info(symbol)?
        .ok_or_else(|| format!("symbol not found: {symbol}"))?;

    client.symbol_select(symbol, true)?;

    let tick = client
        .symbol_info_tick(symbol)?
        .ok_or_else(|| format!("tick unavailable for {symbol}"))?;

    let request = TradeRequest {
        action: TradeAction::Deal,
        symbol: symbol.to_string(),
        volume: 0.01,
        price: tick.ask,
        order_type: OrderType::Buy,
        type_filling: OrderFilling::Ioc,
        type_time: OrderTime::Gtc,
        deviation: 20,
        comment: "metatrader-rs example check".to_string(),
        ..TradeRequest::default()
    };

    let check = client.order_check(&request)?;
    println!(
        "order_check retcode={} comment={} margin={} free_margin={}",
        check.retcode, check.comment, check.margin, check.margin_free
    );

    let margin_est = client.order_calc_margin(TradeAction::Deal as i32, symbol, request.volume, request.price)?;
    println!("local margin estimate={}", margin_est);

    let profit_est = client.order_calc_profit(
        TradeAction::Deal as i32,
        symbol,
        request.volume,
        tick.ask,
        tick.ask + 10.0 * info.point,
    )?;
    println!("local profit estimate for +10 points={}", profit_est);

    if std::env::var("MT5_SEND_ORDER").ok().as_deref() == Some("1") {
        let result = client.order_send(&request)?;
        println!(
            "order_send retcode={} deal={} order={} comment={} request_id={}",
            result.retcode, result.deal, result.order, result.comment, result.request_id
        );
    } else {
        println!("Set MT5_SEND_ORDER=1 to submit the order for real execution.");
    }

    Ok(())
}
