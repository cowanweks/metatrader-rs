# mt5-rs

A pure Rust library for MetaTrader 5 IPC communication. No Python dependency.

Compatible with Python `MetaTrader5` library API.

## Features

- Pure Rust implementation, no Python or C++ dependency
- Windows named pipe IPC communication with MT5 terminal
- Full compatibility with Python `MetaTrader5` library (30/32 functions)
- Support MT5 Build 5836+

## Quick Start

```rust
use mt5_rs::{Mt5Client, discover_mt5_pipe};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Auto-discover MT5 pipe
    let pipe_name = discover_mt5_pipe();
    
    // Initialize client
    let mut client = Mt5Client::new();
    client.initialize(Some(&pipe_name))?;
    
    // Get account info
    let account = client.account_info()?;
    println!("Balance: {}", account.balance);
    println!("Equity: {}", account.equity);
    println!("Margin Free: {}", account.margin_free);
    
    Ok(())
}
```

## Examples

Run examples from repository root:

```bash
cargo run -p metatrader-rs --example 01_connect_and_account
cargo run -p metatrader-rs --example 02_symbols_and_ticks
cargo run -p metatrader-rs --example 03_filters_and_history
cargo run -p metatrader-rs --example 04_trade_check_and_send
cargo run -p metatrader-rs --example 05_poll_quotes
cargo run -p metatrader-rs --example 06_connection_diagnostics_and_login
cargo run -p metatrader-rs --example 07_market_data_full_surface
cargo run -p metatrader-rs --example 08_positions_orders_history_all
cargo run -p metatrader-rs --example 09_market_book_and_trade_helpers
cargo run -p metatrader-rs --example 10_discover_pipe_apis
```

To allow real order submission in `04_trade_check_and_send`:

```bash
MT5_SEND_ORDER=1 cargo run -p metatrader-rs --example 04_trade_check_and_send
MT5_LOGIN=123456 MT5_PASSWORD=secret MT5_SERVER=Broker-Server cargo run -p metatrader-rs --example 06_connection_diagnostics_and_login
MT5_SEND_ORDER_OPEN=1 cargo run -p metatrader-rs --example 09_market_book_and_trade_helpers
MT5_SEND_ORDER_BUY=1 cargo run -p metatrader-rs --example 09_market_book_and_trade_helpers
MT5_SEND_ORDER_SELL=1 cargo run -p metatrader-rs --example 09_market_book_and_trade_helpers
MT5_USE_PANIC_DISCOVER=1 cargo run -p metatrader-rs --example 10_discover_pipe_apis
```

## API Reference

### Initialization

| Function | Description |
|----------|-------------|
| `Mt5Client::new()` | Create a new client |
| `try_discover_mt5_pipe()` | Discover MT5 pipe without panicking |
| `initialize(pipe_name)` | Initialize connection to MT5 |
| `shutdown()` | Close connection |
| `close()` | Alias of shutdown |
| `login(login, password, server)` | Login to MT5 account |

### Account & Terminal

| Function | Description |
|----------|-------------|
| `account_info()` | Get account information |
| `terminal_info()` | Get terminal information |
| `version()` | Get MT5 version |
| `last_error()` | Get last error code and message |

### Symbols

| Function | Description |
|----------|-------------|
| `symbol_info(symbol)` | Get symbol information |
| `symbol_info_tick(symbol)` | Get current tick for symbol |
| `symbols_get()` | Get all available symbols |
| `symbol_select(symbol, enable)` | Select/deselect symbol in Market Watch |

### Market Data

| Function | Description |
|----------|-------------|
| `copy_rates_from_pos(symbol, timeframe, start_pos, count)` | Copy bars from position |
| `copy_rates_from(symbol, timeframe, date_from, count)` | Copy bars from date |
| `copy_rates_range(symbol, timeframe, date_from, date_to)` | Copy bars in range |
| `copy_ticks_from(symbol, from, count, flags)` | Copy ticks from time |
| `copy_ticks_range(symbol, from, to, flags)` | Copy ticks in range |
| `poll_quotes(symbols, interval)` | Poll quote updates as a stream-like channel |

### Positions & Orders

| Function | Description |
|----------|-------------|
| `positions_total()` | Get total number of open positions |
| `positions_get(symbol)` | Get open positions (legacy convenience API) |
| `positions_get_filtered(filter)` | Get positions by ticket/symbol/group filter |
| `orders_total()` | Get total number of pending orders |
| `orders_get(symbol)` | Get pending orders (legacy convenience API) |
| `orders_get_filtered(filter)` | Get orders by ticket/symbol/group filter |

### History

| Function | Description |
|----------|-------------|
| `history_deals_total(from, to)` | Get total number of deals in range |
| `history_deals_get(from, to)` | Get deals in range |
| `history_deals_get_filtered(filter)` | Get deals by date/symbol/ticket/group filter |
| `history_orders_total(from, to)` | Get total number of orders in range |
| `history_orders_get(from, to)` | Get orders in range |
| `history_orders_get_filtered(filter)` | Get orders by date/symbol/ticket/group filter |

### Market Depth

| Function | Description |
|----------|-------------|
| `market_book_add(symbol)` | Subscribe to market depth |
| `market_book_get(symbol)` | Get market depth data |
| `market_book_release(symbol)` | Unsubscribe from market depth |

### Trading Calculations

| Function | Description |
|----------|-------------|
| `order_calc_margin(action, symbol, volume, price)` | Calculate required margin (local calculation) |
| `order_calc_profit(action, symbol, volume, price_open, price_close)` | Calculate expected profit (local calculation) |
| `order_check(request)` | Validate trade request against server |
| `order_send(request)` | Submit trade request to server |

### High-Level Trade Helpers

| Function | Description |
|----------|-------------|
| `open_order(symbol, volume, order_type, price)` | Send a market order-style request |
| `buy(symbol, volume, price)` | Convenience wrapper for buy order |
| `sell(symbol, volume, price)` | Convenience wrapper for sell order |

## Implementation Notes

### Local Calculation for `order_calc_margin` and `order_calc_profit`

Unlike `go-mt5` which sends commands 202/203 via named pipe, this library uses **local calculation** (same as Python `MetaTrader5` library):

- `order_calc_margin`: `margin = volume × price × margin_initial / 4`
- `order_calc_profit`: `profit = volume × (price_close - price_open) × trade_contract_size`

This approach avoids the "pipe closed" error that occurs with MT5 Build 5836+ when using IPC for these commands.

## Requirements

- Rust 2021 edition
- Windows OS (named pipe IPC is Windows-specific)
- MT5 terminal must be running

## License

MIT
