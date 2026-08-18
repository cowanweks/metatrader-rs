use crate::types::{OrderFilling, OrderType, TradeAction, TradeRequest, TradeResult};
use crate::{Mt5Client, Result};

impl Mt5Client {
    pub fn open_order(
        &self,
        symbol: &str,
        volume: f64,
        order_type: OrderType,
        price: f64,
    ) -> Result<TradeResult> {
        let request = TradeRequest {
            action: TradeAction::Deal,
            symbol: symbol.to_string(),
            volume,
            order_type,
            price,
            type_filling: OrderFilling::Ioc,
            ..TradeRequest::default()
        };

        self.order_send(&request)
    }

    pub fn buy(&self, symbol: &str, volume: f64, price: f64) -> Result<TradeResult> {
        self.open_order(symbol, volume, OrderType::Buy, price)
    }

    pub fn sell(&self, symbol: &str, volume: f64, price: f64) -> Result<TradeResult> {
        self.open_order(symbol, volume, OrderType::Sell, price)
    }
}
