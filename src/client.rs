use crate::error::{Mt5Error, Result};
use crate::protocol::NamedPipeClient;
use crate::types::*;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{mpsc, Arc};
use std::thread;
use std::time::Duration;

pub struct Mt5Client {
    pipe: Option<NamedPipeClient>,
    pipe_name: Option<String>,
    build: i32,
}

pub struct QuotePoll {
    updates_rx: mpsc::Receiver<HashMap<String, Tick>>,
    errors_rx: mpsc::Receiver<Mt5Error>,
    stop: Arc<AtomicBool>,
    handle: Option<thread::JoinHandle<()>>,
}

impl QuotePoll {
    pub fn recv(&self) -> std::result::Result<HashMap<String, Tick>, mpsc::RecvError> {
        self.updates_rx.recv()
    }

    pub fn try_recv(&self) -> std::result::Result<HashMap<String, Tick>, mpsc::TryRecvError> {
        self.updates_rx.try_recv()
    }

    pub fn try_recv_error(&self) -> Option<Mt5Error> {
        self.errors_rx.try_recv().ok()
    }

    pub fn stop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

impl Drop for QuotePoll {
    fn drop(&mut self) {
        self.stop();
    }
}

impl Mt5Client {
    pub fn new() -> Self {
        Self {
            pipe: None,
            pipe_name: None,
            build: 0,
        }
    }

    pub fn initialize(&mut self, pipe_name: Option<&str>) -> Result<()> {
        let name = pipe_name.ok_or_else(|| {
            Mt5Error::ConnectionFailed("Pipe name must be provided for initialization".into())
        })?;
        self.pipe_name = Some(name.to_string());
        self.pipe = Some(NamedPipeClient::new(Some(name))?);

        let pipe = self.pipe()?;
        let mut data = Vec::new();
        data.extend_from_slice(&3u32.to_le_bytes());
        data.extend_from_slice(&encode_string("Go"));

        let resp = pipe.send(4, &data)?;
        if resp.len() >= 4 {
            let build = u32::from_le_bytes([resp[0], resp[1], resp[2], resp[3]]);
            self.build = build as i32;
        }

        Ok(())
    }

    pub fn shutdown(&mut self) {
        self.pipe = None;
    }

    pub fn close(&mut self) {
        self.shutdown();
    }

    pub fn build(&self) -> i32 {
        self.build
    }

    pub fn is_connected(&self) -> bool {
        self.pipe.is_some()
    }

    fn pipe(&self) -> Result<&NamedPipeClient> {
        self.pipe.as_ref().ok_or(Mt5Error::NotInitialized)
    }

    pub fn login(&self, login: i64, password: &str, server: &str) -> Result<()> {
        let pipe = self.pipe()?;
        let mut data = Vec::new();
        data.extend_from_slice(&login.to_le_bytes());
        data.extend_from_slice(&encode_string(password));
        data.extend_from_slice(&encode_string(server));

        let resp = pipe.send(100, &data)?;
        if resp.len() < 4 {
            return Err(Mt5Error::InvalidResponse("Response too short".into()));
        }

        let status = u32::from_le_bytes([resp[0], resp[1], resp[2], resp[3]]);
        if status != 0 {
            return Err(Mt5Error::CommandFailed {
                cmd: 100,
                error: format!("Login failed with status: {}", status),
            });
        }

        Ok(())
    }

    pub fn account_info(&self) -> Result<AccountInfo> {
        let pipe = self.pipe()?;
        let resp = pipe.send(190, &[])?;

        if resp.len() < 8 {
            return Err(Mt5Error::InvalidResponse("Response too short".into()));
        }

        let mut reader = Reader::new(&resp);

        // 按照 Python 输出和二进制数据验证的精确位置解析
        // Pos 0-7: login (i64)
        let login = reader.read_i64();

        // Pos 8-11: trade_mode (i32)
        let trade_mode = reader.read_i32() as i64;

        // Pos 12-15: leverage (i32)
        let leverage = reader.read_i32() as i64;

        // Pos 16-19: limit_orders (i32)
        let limit_orders = reader.read_i32() as i64;

        // Pos 20-23: margin_so_mode (i32)
        let margin_so_mode = reader.read_i32() as i64;

        // Pos 24: trade_allowed (bool, 1字节)
        let trade_allowed = reader.read_bool1();

        // Pos 25: trade_expert (bool, 1字节)
        let trade_expert = reader.read_bool1();

        // Pos 26-29: margin_mode (i32)
        let margin_mode = reader.read_i32() as i64;

        // Pos 30-33: currency_digits (i32)
        let currency_digits = reader.read_i32() as i64;

        // Pos 34: fifo_close (bool, 1字节)
        let fifo_close = reader.read_bool1();

        // Pos 35-42: balance (f64)
        let balance = reader.read_f64();

        // Pos 43-50: credit (f64)
        let credit = reader.read_f64();

        // Pos 51-58: profit (f64)
        let profit = reader.read_f64();

        // Pos 59-66: equity (f64)
        let equity = reader.read_f64();

        // Pos 67-74: margin (f64)
        let margin = reader.read_f64();

        // Pos 75-82: margin_free (f64)
        let free_margin = reader.read_f64();

        // Pos 83-90: margin_level (f64)
        let margin_level = reader.read_f64();

        // Pos 91-98: margin_so_call (f64)
        let margin_so_call = reader.read_f64();

        // Pos 99-106: margin_so_so (f64)
        let margin_so_so = reader.read_f64();

        // Pos 107-114: margin_initial (f64)
        let margin_initial = reader.read_f64();

        // Pos 115-122: margin_maintenance (f64)
        let margin_maintenance = reader.read_f64();

        // Pos 123-130: assets (f64)
        let assets = reader.read_f64();

        // Pos 131-138: liabilities (f64)
        let liabilities = reader.read_f64();

        // Pos 139-146: commission_blocked (f64)
        let commission_blocked = reader.read_f64();

        // 读取字符串字段 (从 pos 147 开始)
        let strings_offset = 147;
        if strings_offset >= resp.len() {
            return Err(Mt5Error::InvalidResponse(format!(
                "Response too short for strings: {} < {}",
                resp.len(),
                strings_offset
            )));
        }

        let mut sr = Reader::new(&resp[strings_offset..]);
        let name = sr.read_fixed_string(256);
        let server = sr.read_fixed_string(128);
        let currency = sr.read_fixed_string(64);
        let company = sr.read_fixed_string(256);

        if sr.has_error() {
            return Err(Mt5Error::InvalidResponse("Failed to read strings".into()));
        }

        Ok(AccountInfo {
            login,
            trade_mode,
            leverage,
            limit_orders,
            margin_so_mode,
            trade_allowed,
            trade_expert,
            margin_mode,
            currency_digits,
            fifo_close,
            balance,
            credit,
            profit,
            equity,
            margin,
            free_margin,
            margin_level,
            margin_so_call,
            margin_so_so,
            margin_initial,
            margin_maintenance,
            assets,
            liabilities,
            commission_blocked,
            name,
            server,
            currency,
            company,
        })
    }

    pub fn terminal_info(&self) -> Result<TerminalInfo> {
        let pipe = self.pipe()?;
        let resp = pipe.send(180, &[])?;

        if resp.len() < 40 {
            return Err(Mt5Error::InvalidResponse("Response too short".into()));
        }

        let community_account = resp[2] != 0;
        let community_connection = resp[3] != 0;
        let connected = resp[6] != 0;
        let dlls_allowed = resp[7] != 0;
        let trade_allowed = resp[8] != 0;
        let trade_api_disabled = resp[9] != 0;
        let email_enabled = resp[10] != 0;
        let ftp_enabled = resp[11] != 0;
        let notifications_enabled = resp[4] != 0;
        let mqid = resp[5] != 0;

        let build = u16::from_le_bytes([resp[0], resp[1]]) as i64;
        let max_bars = u32::from_le_bytes([resp[12], resp[13], resp[14], resp[15]]) as i64;
        let code_page = u16::from_le_bytes([resp[17], resp[18]]) as i64;
        let ping_last = u16::from_le_bytes([resp[21], resp[22]]) as i64;
        let community_balance = f64::from_le_bytes([
            resp[24], resp[25], resp[26], resp[27], resp[28], resp[29], resp[30], resp[31],
        ]);
        let retransmission = f64::from_le_bytes([
            resp[32], resp[33], resp[34], resp[35], resp[36], resp[37], resp[38], resp[39],
        ]);

        let company = read_string_at_offset(&resp, 41);
        let name = read_string_at_offset(&resp, 561);
        let language = read_string_at_offset(&resp, 1081);
        let path = read_string_at_offset(&resp, 1601);
        let data_path = read_string_at_offset(&resp, 2121);
        let common_data_path = read_string_at_offset(&resp, 2641);

        Ok(TerminalInfo {
            community_account,
            community_connection,
            connected,
            dlls_allowed,
            trade_allowed,
            trade_api_disabled,
            email_enabled,
            ftp_enabled,
            notifications_enabled,
            mqid,
            build,
            max_bars,
            code_page,
            ping_last,
            community_balance,
            retransmission,
            company,
            name,
            language,
            path,
            data_path,
            common_data_path,
        })
    }

    pub fn version(&self) -> Result<VersionInfo> {
        let info = self.terminal_info()?;
        Ok(VersionInfo {
            version: info.build as i32,
            build: info.build as i32,
            build_date: format!("{} ({})", info.company, info.name),
        })
    }

    pub fn symbols_total(&self) -> Result<i64> {
        let pipe = self.pipe()?;
        let resp = pipe.send(173, &[])?;

        if resp.len() < 4 {
            return Err(Mt5Error::InvalidResponse("Response too short".into()));
        }

        let total = u32::from_le_bytes([resp[0], resp[1], resp[2], resp[3]]);
        Ok(total as i64)
    }

    pub fn symbols_get(&self) -> Result<Vec<SymbolInfo>> {
        self.symbols_get_by_group(None)
    }

    pub fn symbols_get_by_group(&self, group: Option<&str>) -> Result<Vec<SymbolInfo>> {
        let pipe = self.pipe()?;
        let (cmd, data) = match group {
            Some(expr) if !expr.trim().is_empty() => (175_u32, encode_string(expr)),
            _ => (174_u32, Vec::new()),
        };

        let resp = pipe.send(cmd, &data)?;

        if resp.len() < 4 {
            return Err(Mt5Error::InvalidResponse("Response too short".into()));
        }

        let mut reader = Reader::new(&resp);
        let count = reader.read_u32() as usize;

        let mut symbols = Vec::with_capacity(count);

        for _ in 0..count {
            let sym = Self::decode_symbol_info(&mut reader)?;
            symbols.push(sym);
        }

        Ok(symbols)
    }

    fn decode_symbol_info(reader: &mut Reader) -> Result<SymbolInfo> {
        // 严格按照 go-mt5 decodeSymbolInfo 的字段顺序和类型解析
        // 参考：https://github.com/Mukbeast4/go-mt5/blob/main/symbols.go
        let custom = reader.read_bool1();
        let chart_mode = reader.read_u32() as i64;
        let select = reader.read_bool1();
        let visible = reader.read_bool1();
        let session_deals = reader.read_i64();
        let session_buy_orders = reader.read_i64();
        let session_sell_orders = reader.read_i64();
        let volume = reader.read_i64();
        let volume_high = reader.read_i64();
        let volume_low = reader.read_i64();
        let time = reader.read_i64();
        let digits = reader.read_u32() as i64;
        let spread = reader.read_u32() as i64;
        let spread_float = reader.read_bool1();
        let ticks_book_depth = reader.read_u32() as i64;
        let trade_calc_mode = reader.read_u32() as i64;
        let trade_mode = reader.read_u32() as i64;
        let start_time = reader.read_i64();
        let expiration_time = reader.read_i64();
        let trade_stops_level = reader.read_u32() as i64;
        let trade_freeze_level = reader.read_u32() as i64;
        let trade_exe_mode = reader.read_u32() as i64;
        let swap_mode = reader.read_u32() as i64;
        let swap_rollover3days = reader.read_u32() as i64;
        let margin_hedged_use_leg = reader.read_bool1();
        let expiration_mode = reader.read_u32() as i64;
        let filling_mode = reader.read_u32() as i64;
        let order_mode = reader.read_u32() as i64;
        let order_gtc_mode = reader.read_u32() as i64;
        let option_mode = reader.read_u32() as i64;
        let option_right = reader.read_u32() as i64;
        let bid = reader.read_f64();
        let bid_high = reader.read_f64();
        let bid_low = reader.read_f64();
        let ask = reader.read_f64();
        let ask_high = reader.read_f64();
        let ask_low = reader.read_f64();
        let last = reader.read_f64();
        let last_high = reader.read_f64();
        let last_low = reader.read_f64();
        let volume_real = reader.read_f64();
        let volume_high_real = reader.read_f64();
        let volume_low_real = reader.read_f64();
        let option_strike = reader.read_f64();
        let point = reader.read_f64();
        let trade_tick_value = reader.read_f64();
        let trade_tick_value_profit = reader.read_f64();
        let trade_tick_value_loss = reader.read_f64();
        let trade_tick_size = reader.read_f64();
        let trade_contract_size = reader.read_f64();
        let trade_accrued_interest = reader.read_f64();
        let trade_face_value = reader.read_f64();
        let trade_liquidity_rate = reader.read_f64();
        let volume_min = reader.read_f64();
        let volume_max = reader.read_f64();
        let volume_step = reader.read_f64();
        let volume_limit = reader.read_f64();
        let swap_long = reader.read_f64();
        let swap_short = reader.read_f64();
        let margin_initial = reader.read_f64();
        let margin_maintenance = reader.read_f64();
        let session_volume = reader.read_f64();
        let session_turnover = reader.read_f64();
        let session_interest = reader.read_f64();
        let session_buy_orders_volume = reader.read_f64();
        let session_sell_orders_volume = reader.read_f64();
        let session_open = reader.read_f64();
        let session_close = reader.read_f64();
        let session_aw = reader.read_f64();
        let session_price_settlement = reader.read_f64();
        let session_price_limit_min = reader.read_f64();
        let session_price_limit_max = reader.read_f64();
        let margin_hedged = reader.read_f64();
        let price_change = reader.read_f64();
        let price_volatility = reader.read_f64();
        let price_theoretical = reader.read_f64();
        let price_greeks_delta = reader.read_f64();
        let price_greeks_theta = reader.read_f64();
        let price_greeks_gamma = reader.read_f64();
        let price_greeks_vega = reader.read_f64();
        let price_greeks_rho = reader.read_f64();
        let price_greeks_omega = reader.read_f64();
        let price_sensitivity = reader.read_f64();

        // 字符串字段：固定宽度 UTF-16LE 槽（go-mt5 PR#3 验证）
        // 总字符串区域 = 2432 字节
        let basis = reader.read_fixed_string(64);
        let category = reader.read_fixed_string(128);
        let currency_base = reader.read_fixed_string(32);
        let currency_profit = reader.read_fixed_string(32);
        let currency_margin = reader.read_fixed_string(32);
        let bank = reader.read_fixed_string(512);
        let description = reader.read_fixed_string(64);
        let exchange = reader.read_fixed_string(64);
        let formula = reader.read_fixed_string(1024);
        let isin = reader.read_fixed_string(32);
        let page = reader.read_fixed_string(128);
        let path = reader.read_fixed_string(256);
        let symbol_name = reader.read_fixed_string(64);

        if reader.has_error() {
            return Err(Mt5Error::InvalidResponse(
                "Failed to read symbol info".into(),
            ));
        }

        Ok(SymbolInfo {
            custom,
            chart_mode,
            select,
            visible,
            session_deals,
            session_buy_orders,
            session_sell_orders,
            volume,
            volume_high,
            volume_low,
            time,
            digits,
            spread,
            spread_float,
            ticks_book_depth,
            trade_calc_mode,
            trade_mode,
            start_time,
            expiration_time,
            trade_stops_level,
            trade_freeze_level,
            trade_exe_mode,
            swap_mode,
            swap_rollover3days,
            margin_hedged_use_leg,
            expiration_mode,
            filling_mode,
            order_mode,
            order_gtc_mode,
            option_mode,
            option_right,
            bid,
            bidhigh: bid_high,
            bidlow: bid_low,
            ask,
            askhigh: ask_high,
            asklow: ask_low,
            last,
            lasthigh: last_high,
            lastlow: last_low,
            volume_real,
            volumehigh_real: volume_high_real,
            volumelow_real: volume_low_real,
            option_strike,
            point,
            trade_tick_value,
            trade_tick_value_profit,
            trade_tick_value_loss,
            trade_tick_size,
            trade_contract_size,
            trade_accrued_interest,
            trade_face_value,
            trade_liquidity_rate,
            volume_min,
            volume_max,
            volume_step,
            volume_limit,
            swap_long,
            swap_short,
            margin_initial,
            margin_maintenance,
            session_volume,
            session_turnover,
            session_interest,
            session_buy_orders_volume,
            session_sell_orders_volume,
            session_open,
            session_close,
            session_aw,
            session_price_settlement,
            session_price_limit_min,
            session_price_limit_max,
            margin_hedged,
            price_change,
            price_volatility,
            price_theoretical,
            price_greeks_delta,
            price_greeks_theta,
            price_greeks_gamma,
            price_greeks_vega,
            price_greeks_rho,
            price_greeks_omega,
            price_sensitivity,
            basis,
            category,
            currency_base,
            currency_profit,
            currency_margin,
            bank,
            description,
            exchange,
            formula,
            isin,
            name: symbol_name,
            page,
            path,
        })
    }

    pub fn symbol_info(&self, symbol: &str) -> Result<Option<SymbolInfo>> {
        let pipe = self.pipe()?;
        let mut data = Vec::new();
        data.extend_from_slice(&encode_string(symbol));

        let resp = pipe.send(170, &data)?;

        if resp.is_empty() {
            return Ok(None);
        }

        let mut reader = Reader::new(&resp);
        let info = Self::decode_symbol_info(&mut reader)?;
        Ok(Some(info))
    }

    pub fn symbol_info_tick(&self, symbol: &str) -> Result<Option<Tick>> {
        let pipe = self.pipe()?;
        let mut data = Vec::new();
        data.extend_from_slice(&encode_string(symbol));

        let resp = pipe.send(172, &data)?;

        if resp.is_empty() {
            return Ok(None);
        }

        let mut reader = Reader::new(&resp);

        // 严格按照 go-mt5 decodeTick 的字段顺序和类型解析
        let time = reader.read_i64();
        let bid = reader.read_f64();
        let ask = reader.read_f64();
        let last = reader.read_f64();
        let volume = reader.read_u64();
        let time_msc = reader.read_i64();
        let flags = reader.read_u32();
        let volume_real = reader.read_f64();

        if reader.has_error() {
            return Err(Mt5Error::InvalidResponse("Failed to read tick info".into()));
        }

        Ok(Some(Tick {
            time,
            bid,
            ask,
            last,
            volume,
            time_msc,
            flags,
            volume_real,
        }))
    }

    pub fn symbol_select(&self, symbol: &str, enable: bool) -> Result<bool> {
        let pipe = self.pipe()?;
        let mut data = Vec::new();
        data.extend_from_slice(&encode_string(symbol));
        data.push(if enable { 1u8 } else { 0u8 });

        let resp = pipe.send(171, &data)?;

        // 空响应表示成功（MT5只返回8字节的头部，没有额外数据）
        if resp.is_empty() {
            return Ok(true);
        }

        if resp.len() < 4 {
            return Err(Mt5Error::InvalidResponse("Response too short".into()));
        }

        let status = i32::from_le_bytes([resp[0], resp[1], resp[2], resp[3]]);
        Ok(status != 0)
    }

    pub fn poll_quotes(&self, symbols: &[String], interval: Duration) -> Result<QuotePoll> {
        if symbols.is_empty() {
            return Err(Mt5Error::ProtocolError(
                "symbols list cannot be empty".into(),
            ));
        }

        let pipe_name = self.pipe_name.clone().ok_or(Mt5Error::NotInitialized)?;

        let (updates_tx, updates_rx) = mpsc::channel();
        let (errors_tx, errors_rx) = mpsc::channel();
        let stop = Arc::new(AtomicBool::new(false));
        let stop_flag = Arc::clone(&stop);
        let symbols = symbols.to_vec();

        let handle = thread::spawn(move || {
            let mut client = Mt5Client::new();
            if let Err(err) = client.initialize(Some(&pipe_name)) {
                let _ = errors_tx.send(err);
                return;
            }

            let group_expr = symbols.join(",");
            let mut previous = HashMap::<String, Tick>::new();

            while !stop_flag.load(Ordering::SeqCst) {
                match client.symbols_get_by_group(Some(&group_expr)) {
                    Ok(symbol_infos) => {
                        let mut batch = HashMap::new();

                        for requested in &symbols {
                            let maybe_symbol = symbol_infos
                                .iter()
                                .find(|item| item.name.eq_ignore_ascii_case(requested));

                            let Some(info) = maybe_symbol else {
                                let _ = errors_tx.send(Mt5Error::ProtocolError(format!(
                                    "unknown symbol in poll_quotes: {}",
                                    requested
                                )));
                                return;
                            };

                            if !info.select {
                                let _ = errors_tx.send(Mt5Error::ProtocolError(format!(
                                    "symbol not selected in Market Watch: {}",
                                    requested
                                )));
                                return;
                            }

                            let tick = Tick {
                                time: info.time,
                                bid: info.bid,
                                ask: info.ask,
                                last: info.last,
                                volume: info.volume as u64,
                                time_msc: info.time.saturating_mul(1000),
                                flags: 0,
                                volume_real: info.volume_real,
                            };

                            let should_emit = previous
                                .get(&info.name)
                                .map(|prev| {
                                    prev.bid.to_bits() != tick.bid.to_bits()
                                        || prev.ask.to_bits() != tick.ask.to_bits()
                                        || prev.last.to_bits() != tick.last.to_bits()
                                })
                                .unwrap_or(true);

                            if should_emit {
                                batch.insert(info.name.clone(), tick.clone());
                            }

                            previous.insert(info.name.clone(), tick);
                        }

                        if !batch.is_empty() && updates_tx.send(batch).is_err() {
                            return;
                        }
                    }
                    Err(err) => {
                        let _ = errors_tx.send(err);
                        return;
                    }
                }

                thread::sleep(interval);
            }
        });

        Ok(QuotePoll {
            updates_rx,
            errors_rx,
            stop,
            handle: Some(handle),
        })
    }

    pub fn positions_total(&self) -> Result<i64> {
        let pipe = self.pipe()?;
        let resp = pipe.send(120, &[])?;

        if resp.len() < 4 {
            return Err(Mt5Error::InvalidResponse("Response too short".into()));
        }

        let total = u32::from_le_bytes([resp[0], resp[1], resp[2], resp[3]]);
        Ok(total as i64)
    }

    pub fn orders_total(&self) -> Result<i64> {
        let pipe = self.pipe()?;
        let resp = pipe.send(130, &[])?;

        if resp.len() < 4 {
            return Err(Mt5Error::InvalidResponse("Response too short".into()));
        }

        let total = u32::from_le_bytes([resp[0], resp[1], resp[2], resp[3]]);
        Ok(total as i64)
    }

    pub fn positions_get(&self, symbol: Option<&str>) -> Result<Vec<TradePosition>> {
        let filter = symbol.map(|sym| PositionFilter {
            symbol: sym.to_string(),
            ..PositionFilter::default()
        });
        self.positions_get_filtered(filter.as_ref())
    }

    pub fn positions_get_filtered(
        &self,
        filter: Option<&PositionFilter>,
    ) -> Result<Vec<TradePosition>> {
        let pipe = self.pipe()?;

        let (cmd, data) = match filter {
            Some(f) if f.ticket != 0 => {
                let mut payload = Vec::new();
                payload.extend_from_slice(&f.ticket.to_le_bytes());
                (123_u32, payload)
            }
            Some(f) if !f.symbol.trim().is_empty() => (122_u32, encode_string(&f.symbol)),
            _ => (121_u32, Vec::new()),
        };

        let resp = pipe.send(cmd, &data)?;
        let mut positions = parse_positions_response(&resp)?;

        if let Some(f) = filter {
            if !f.group.trim().is_empty() {
                positions.retain(|position| match_group(&position.symbol, &f.group));
            }
        }

        Ok(positions)
    }

    pub fn orders_get(&self, symbol: Option<&str>) -> Result<Vec<TradeOrder>> {
        let filter = symbol.map(|sym| OrderFilter {
            symbol: sym.to_string(),
            ..OrderFilter::default()
        });
        self.orders_get_filtered(filter.as_ref())
    }

    pub fn orders_get_filtered(&self, filter: Option<&OrderFilter>) -> Result<Vec<TradeOrder>> {
        let pipe = self.pipe()?;

        let (cmd, data) = match filter {
            Some(f) if f.ticket != 0 => {
                let mut payload = Vec::new();
                payload.extend_from_slice(&f.ticket.to_le_bytes());
                (133_u32, payload)
            }
            Some(f) if !f.symbol.trim().is_empty() => (132_u32, encode_string(&f.symbol)),
            _ => (131_u32, Vec::new()),
        };

        let resp = pipe.send(cmd, &data)?;
        let mut orders = parse_orders_response(&resp)?;

        if let Some(f) = filter {
            if !f.group.trim().is_empty() {
                orders.retain(|order| match_group(&order.symbol, &f.group));
            }
        }

        Ok(orders)
    }

    pub fn send_raw_command(&self, cmd: u32, data: &[u8]) -> Result<Vec<u8>> {
        let pipe = self.pipe()?;
        pipe.send(cmd, data)
    }

    pub fn copy_rates_from_pos(
        &self,
        symbol: &str,
        timeframe: i32,
        start_pos: i64,
        count: i32,
    ) -> Result<Vec<Rate>> {
        let pipe = self.pipe()?;
        // 根据go-mt5源码，命令代码108，参数使用u32编码
        let cmd = 108;

        let mut data = Vec::new();
        data.extend_from_slice(&encode_string(symbol));
        data.extend_from_slice(&(timeframe as u32).to_le_bytes());
        data.extend_from_slice(&(start_pos as u32).to_le_bytes());
        data.extend_from_slice(&(count as u32).to_le_bytes());

        let resp = pipe.send(cmd, &data)?;
        parse_rates_response(&resp)
    }

    pub fn copy_rates_from(
        &self,
        symbol: &str,
        timeframe: i32,
        date_from: i64,
        count: i32,
    ) -> Result<Vec<Rate>> {
        let pipe = self.pipe()?;
        let cmd = 106;

        let mut data = Vec::new();
        data.extend_from_slice(&encode_string(symbol));
        data.extend_from_slice(&(timeframe as u32).to_le_bytes());
        data.extend_from_slice(&date_from.to_le_bytes());
        data.extend_from_slice(&(count as u32).to_le_bytes());

        let resp = pipe.send(cmd, &data)?;
        parse_rates_response(&resp)
    }

    pub fn copy_rates_range(
        &self,
        symbol: &str,
        timeframe: i32,
        date_from: i64,
        date_to: i64,
    ) -> Result<Vec<Rate>> {
        let pipe = self.pipe()?;
        let cmd = 107;

        let mut data = Vec::new();
        data.extend_from_slice(&encode_string(symbol));
        data.extend_from_slice(&(timeframe as u32).to_le_bytes());
        data.extend_from_slice(&date_from.to_le_bytes());
        data.extend_from_slice(&date_to.to_le_bytes());

        let resp = pipe.send(cmd, &data)?;
        parse_rates_response(&resp)
    }

    pub fn copy_ticks_from(
        &self,
        symbol: &str,
        from: i64,
        count: i32,
        flags: i32,
    ) -> Result<Vec<Tick>> {
        let pipe = self.pipe()?;
        let cmd = 104;

        let mut data = Vec::new();
        data.extend_from_slice(&encode_string(symbol));
        data.extend_from_slice(&from.to_le_bytes());
        data.extend_from_slice(&(count as u32).to_le_bytes());
        data.extend_from_slice(&(flags as u32).to_le_bytes());

        let resp = pipe.send(cmd, &data)?;
        parse_ticks_response(&resp)
    }

    pub fn copy_ticks_range(
        &self,
        symbol: &str,
        from: i64,
        to: i64,
        flags: i32,
    ) -> Result<Vec<Tick>> {
        let pipe = self.pipe()?;
        let cmd = 105;

        let mut data = Vec::new();
        data.extend_from_slice(&encode_string(symbol));
        data.extend_from_slice(&from.to_le_bytes());
        data.extend_from_slice(&to.to_le_bytes());
        data.extend_from_slice(&(flags as u32).to_le_bytes());

        let resp = pipe.send(cmd, &data)?;
        parse_ticks_response(&resp)
    }

    pub fn history_deals_total(&self, from: i64, to: i64) -> Result<i64> {
        let pipe = self.pipe()?;
        let cmd = 150;

        let mut data = Vec::new();
        data.extend_from_slice(&from.to_le_bytes());
        data.extend_from_slice(&to.to_le_bytes());

        let resp = pipe.send(cmd, &data)?;
        if resp.len() < 4 {
            return Err(Mt5Error::InvalidResponse("Response too short".into()));
        }

        let total = u32::from_le_bytes([resp[0], resp[1], resp[2], resp[3]]);
        Ok(total as i64)
    }

    pub fn history_deals_get(&self, from: i64, to: i64) -> Result<Vec<TradeDeal>> {
        let filter = HistoryFilter {
            date_from: from,
            date_to: to,
            ..HistoryFilter::default()
        };
        self.history_deals_get_filtered(Some(&filter))
    }

    pub fn history_deals_get_filtered(
        &self,
        filter: Option<&HistoryFilter>,
    ) -> Result<Vec<TradeDeal>> {
        let pipe = self.pipe()?;

        let (cmd, data) = match filter {
            Some(f) if f.ticket != 0 => {
                let mut payload = Vec::new();
                payload.extend_from_slice(&f.ticket.to_le_bytes());
                (153_u32, payload)
            }
            Some(f) if !f.symbol.trim().is_empty() => {
                let mut payload = Vec::new();
                payload.extend_from_slice(&f.date_from.to_le_bytes());
                payload.extend_from_slice(&f.date_to.to_le_bytes());
                payload.extend_from_slice(&encode_string(&f.symbol));
                (152_u32, payload)
            }
            Some(f) => {
                let mut payload = Vec::new();
                payload.extend_from_slice(&f.date_from.to_le_bytes());
                payload.extend_from_slice(&f.date_to.to_le_bytes());
                (151_u32, payload)
            }
            None => {
                let mut payload = Vec::new();
                payload.extend_from_slice(&0_i64.to_le_bytes());
                payload.extend_from_slice(&0_i64.to_le_bytes());
                (151_u32, payload)
            }
        };

        let resp = pipe.send(cmd, &data)?;
        let mut deals = parse_deals_response(&resp)?;

        if let Some(f) = filter {
            if !f.group.trim().is_empty() {
                deals.retain(|deal| match_group(&deal.symbol, &f.group));
            }
        }

        Ok(deals)
    }

    pub fn history_orders_total(&self, from: i64, to: i64) -> Result<i64> {
        let pipe = self.pipe()?;
        let cmd = 140;

        let mut data = Vec::new();
        data.extend_from_slice(&from.to_le_bytes());
        data.extend_from_slice(&to.to_le_bytes());

        let resp = pipe.send(cmd, &data)?;
        if resp.len() < 4 {
            return Err(Mt5Error::InvalidResponse("Response too short".into()));
        }

        let total = u32::from_le_bytes([resp[0], resp[1], resp[2], resp[3]]);
        Ok(total as i64)
    }

    pub fn history_orders_get(&self, from: i64, to: i64) -> Result<Vec<TradeOrder>> {
        let filter = HistoryFilter {
            date_from: from,
            date_to: to,
            ..HistoryFilter::default()
        };
        self.history_orders_get_filtered(Some(&filter))
    }

    pub fn history_orders_get_filtered(
        &self,
        filter: Option<&HistoryFilter>,
    ) -> Result<Vec<TradeOrder>> {
        let pipe = self.pipe()?;

        let (cmd, data) = match filter {
            Some(f) if f.ticket != 0 => {
                let mut payload = Vec::new();
                payload.extend_from_slice(&f.ticket.to_le_bytes());
                (143_u32, payload)
            }
            Some(f) if !f.symbol.trim().is_empty() => {
                let mut payload = Vec::new();
                payload.extend_from_slice(&f.date_from.to_le_bytes());
                payload.extend_from_slice(&f.date_to.to_le_bytes());
                payload.extend_from_slice(&encode_string(&f.symbol));
                (142_u32, payload)
            }
            Some(f) => {
                let mut payload = Vec::new();
                payload.extend_from_slice(&f.date_from.to_le_bytes());
                payload.extend_from_slice(&f.date_to.to_le_bytes());
                (141_u32, payload)
            }
            None => {
                let mut payload = Vec::new();
                payload.extend_from_slice(&0_i64.to_le_bytes());
                payload.extend_from_slice(&0_i64.to_le_bytes());
                (141_u32, payload)
            }
        };

        let resp = pipe.send(cmd, &data)?;
        let mut orders = parse_orders_response(&resp)?;

        if let Some(f) = filter {
            if !f.group.trim().is_empty() {
                orders.retain(|order| match_group(&order.symbol, &f.group));
            }
        }

        Ok(orders)
    }

    pub fn market_book_add(&self, symbol: &str) -> Result<bool> {
        let pipe = self.pipe()?;
        let cmd = 191;

        let data = encode_string(symbol);
        let resp = pipe.send(cmd, &data)?;

        if resp.is_empty() {
            return Ok(true);
        }

        if resp.len() < 4 {
            return Err(Mt5Error::InvalidResponse("Response too short".into()));
        }

        let status = u32::from_le_bytes([resp[0], resp[1], resp[2], resp[3]]);
        Ok(status == 0)
    }

    pub fn market_book_get(&self, symbol: &str) -> Result<Vec<BookInfo>> {
        let pipe = self.pipe()?;
        let cmd = 193;

        let data = encode_string(symbol);
        let resp = pipe.send(cmd, &data)?;
        parse_book_response(&resp)
    }

    pub fn market_book_release(&self, symbol: &str) -> Result<bool> {
        let pipe = self.pipe()?;
        let cmd = 192;

        let data = encode_string(symbol);
        let resp = pipe.send(cmd, &data)?;

        if resp.is_empty() {
            return Ok(true);
        }

        if resp.len() < 4 {
            return Err(Mt5Error::InvalidResponse("Response too short".into()));
        }

        let status = u32::from_le_bytes([resp[0], resp[1], resp[2], resp[3]]);
        Ok(status == 0)
    }

    /// 计算订单所需保证金（本地计算，不使用IPC）
    /// 根据Python测试验证的公式：margin = volume × price × margin_initial / 4
    pub fn order_calc_margin(
        &self,
        _action: i32,
        symbol: &str,
        volume: f64,
        price: f64,
    ) -> Result<f64> {
        // 获取symbol info以获取margin_initial
        let symbol_info = self.symbol_info(symbol)?;

        // 根据Python测试验证的公式计算
        // margin = volume × price × margin_initial / 4
        let margin_initial = symbol_info.unwrap().margin_initial;
        let margin = volume * price * margin_initial / 4.0;

        Ok(margin)
    }

    /// 计算订单预期利润（本地计算，不使用IPC）
    /// 公式：profit = volume × (price_close - price_open) × contract_size
    pub fn order_calc_profit(
        &self,
        _action: i32,
        symbol: &str,
        volume: f64,
        price_open: f64,
        price_close: f64,
    ) -> Result<f64> {
        // 获取symbol info以获取contract_size
        let symbol_info = self.symbol_info(symbol)?;

        // 计算利润
        let profit = volume * (price_close - price_open) * symbol_info.unwrap().trade_contract_size;

        Ok(profit)
    }

    pub fn order_check(&self, request: &TradeRequest) -> Result<CheckResult> {
        let pipe = self.pipe()?;
        request.validate().map_err(Mt5Error::ProtocolError)?;

        let payload = encode_trade_request(request);
        let resp = pipe.send(160, &payload)?;

        decode_check_result(&resp)
    }

    pub fn order_send(&self, request: &TradeRequest) -> Result<TradeResult> {
        let pipe = self.pipe()?;
        request.validate().map_err(Mt5Error::ProtocolError)?;

        let payload = encode_trade_request(request);
        let resp = pipe.send(161, &payload)?;

        decode_trade_result(&resp)
    }

    pub fn last_error(&self) -> Result<(i32, String)> {
        let pipe = self.pipe()?;
        let cmd = 3;

        let resp = pipe.send(cmd, &[])?;
        if resp.len() < 4 {
            return Err(Mt5Error::InvalidResponse("Response too short".into()));
        }

        let mut reader = Reader::new(&resp);
        let code = reader.read_i32();
        let message = reader.read_string();

        Ok((code, message))
    }
}

fn parse_positions_response(data: &[u8]) -> Result<Vec<TradePosition>> {
    if data.len() < 4 {
        return Err(Mt5Error::InvalidResponse("Response too short".into()));
    }

    let mut reader = Reader::new(data);
    let count = reader.read_u32() as usize;

    let mut positions = Vec::with_capacity(count);

    for _ in 0..count {
        let ticket = reader.read_i64();
        let time = reader.read_i64();
        let time_msc = reader.read_i64();
        let time_update = reader.read_i64();
        let time_update_msc = reader.read_i64();
        let r#type = reader.read_u32() as i32;
        let magic = reader.read_i64();
        let identifier = reader.read_i64();
        let reason = reader.read_u32() as i32;
        let volume = reader.read_f64();
        let price_open = reader.read_f64();
        let price_current = reader.read_f64();
        let price_sl = reader.read_f64();
        let price_tp = reader.read_f64();
        let swap = reader.read_f64();
        let profit = reader.read_f64();
        let symbol = reader.read_fixed_string(64);
        let comment = reader.read_fixed_string(64);
        let external_id = reader.read_fixed_string(64);

        if reader.has_error() {
            break;
        }

        positions.push(TradePosition {
            ticket,
            time,
            time_msc,
            time_update,
            time_update_msc,
            r#type,
            magic,
            identifier,
            reason,
            volume,
            price_open,
            price_current,
            price_sl,
            price_tp,
            swap,
            profit,
            symbol,
            comment,
            external_id,
        });
    }

    Ok(positions)
}

fn parse_orders_response(data: &[u8]) -> Result<Vec<TradeOrder>> {
    if data.len() < 4 {
        return Err(Mt5Error::InvalidResponse("Response too short".into()));
    }

    let mut reader = Reader::new(data);
    let count = reader.read_u32() as usize;

    let mut orders = Vec::with_capacity(count);

    for _ in 0..count {
        let ticket = reader.read_i64();
        let time_setup = reader.read_i64();
        let time_setup_msc = reader.read_i64();
        let time_done = reader.read_i64();
        let time_done_msc = reader.read_i64();
        let time_expiration = reader.read_i64();
        let r#type = reader.read_u32() as i32;
        let type_time = reader.read_u32() as i32;
        let type_filling = reader.read_u32() as i32;
        let state = reader.read_u32() as i32;
        let magic = reader.read_i64();
        let position_id = reader.read_i64();
        let position_by_id = reader.read_i64();
        let reason = reader.read_u32() as i32;
        let volume_initial = reader.read_f64();
        let volume_current = reader.read_f64();
        let price_open = reader.read_f64();
        let price_current = reader.read_f64();
        let price_sl = reader.read_f64();
        let price_tp = reader.read_f64();
        let price_stoplimit = reader.read_f64();
        let symbol = reader.read_fixed_string(64);
        let comment = reader.read_fixed_string(64);
        let external_id = reader.read_fixed_string(64);

        if reader.has_error() {
            break;
        }

        orders.push(TradeOrder {
            ticket,
            time_setup,
            time_setup_msc,
            time_done,
            time_done_msc,
            time_expiration,
            r#type,
            type_time,
            type_filling,
            state,
            magic,
            position_id,
            position_by_id,
            reason,
            volume_initial,
            volume_current,
            price_open,
            price_current,
            price_sl,
            price_tp,
            price_stoplimit,
            symbol,
            comment,
            external_id,
        });
    }

    Ok(orders)
}

fn parse_deals_response(data: &[u8]) -> Result<Vec<TradeDeal>> {
    if data.len() < 4 {
        return Err(Mt5Error::InvalidResponse("Response too short".into()));
    }

    let mut reader = Reader::new(data);
    let count = reader.read_u32() as usize;

    let mut deals = Vec::with_capacity(count);

    for _ in 0..count {
        let ticket = reader.read_i64();
        let order = reader.read_i64();
        let time = reader.read_i64();
        let time_msc = reader.read_i64();
        let r#type = reader.read_u32() as i32;
        let entry = reader.read_u32() as i32;
        let magic = reader.read_i64();
        let position_id = reader.read_i64();
        let reason = reader.read_u32() as i32;
        let volume = reader.read_f64();
        let price = reader.read_f64();
        let commission = reader.read_f64();
        let swap = reader.read_f64();
        let profit = reader.read_f64();
        let fee = reader.read_f64();
        let symbol = reader.read_fixed_string(64);
        let comment = reader.read_fixed_string(64);
        let external_id = reader.read_fixed_string(64);

        if reader.has_error() {
            break;
        }

        deals.push(TradeDeal {
            ticket,
            order,
            time,
            time_msc,
            r#type,
            entry,
            magic,
            position_id,
            reason,
            volume,
            price,
            commission,
            swap,
            profit,
            fee,
            symbol,
            comment,
            external_id,
        });
    }

    Ok(deals)
}

fn parse_rates_response(data: &[u8]) -> Result<Vec<Rate>> {
    if data.len() < 4 {
        return Err(Mt5Error::InvalidResponse("Response too short".into()));
    }

    let mut reader = Reader::new(data);
    let count = reader.read_u32() as usize;

    let mut rates = Vec::with_capacity(count);

    for _ in 0..count {
        let time = reader.read_i64();
        let open = reader.read_f64();
        let high = reader.read_f64();
        let low = reader.read_f64();
        let close = reader.read_f64();
        let tick_volume = reader.read_u64();
        let spread = reader.read_i32();
        let real_volume = reader.read_u64();

        if reader.has_error() {
            break;
        }

        rates.push(Rate {
            time,
            open,
            high,
            low,
            close,
            tick_volume,
            spread,
            real_volume,
        });
    }

    Ok(rates)
}

fn parse_ticks_response(data: &[u8]) -> Result<Vec<Tick>> {
    if data.len() < 4 {
        return Err(Mt5Error::InvalidResponse("Response too short".into()));
    }

    let mut reader = Reader::new(data);
    let count = reader.read_u32() as usize;

    let mut ticks = Vec::with_capacity(count);

    for _ in 0..count {
        let time = reader.read_i64();
        let bid = reader.read_f64();
        let ask = reader.read_f64();
        let last = reader.read_f64();
        let volume = reader.read_u64();
        let time_msc = reader.read_i64();
        let flags = reader.read_u32();
        let volume_real = reader.read_f64();

        if reader.has_error() {
            break;
        }

        ticks.push(Tick {
            time,
            bid,
            ask,
            last,
            volume,
            time_msc,
            flags,
            volume_real,
        });
    }

    Ok(ticks)
}

fn parse_book_response(data: &[u8]) -> Result<Vec<BookInfo>> {
    if data.len() < 4 {
        return Err(Mt5Error::InvalidResponse("Response too short".into()));
    }

    let mut reader = Reader::new(data);
    let count = reader.read_u32() as usize;

    let mut books = Vec::with_capacity(count);

    for _ in 0..count {
        let r#type = reader.read_i64();
        let price = reader.read_f64();
        let volume = reader.read_i64();
        let volume_real = reader.read_f64();

        if reader.has_error() {
            break;
        }

        books.push(BookInfo {
            r#type,
            price,
            volume,
            volume_real,
        });
    }

    Ok(books)
}

fn encode_string(s: &str) -> Vec<u8> {
    let chars: Vec<u16> = s.encode_utf16().collect();
    let mut data = Vec::with_capacity(4 + chars.len() * 2);
    data.extend_from_slice(&(chars.len() as u32).to_le_bytes());
    for c in chars {
        data.extend_from_slice(&c.to_le_bytes());
    }
    data
}

struct Reader<'a> {
    data: &'a [u8],
    pos: usize,
    error: bool,
}

impl<'a> Reader<'a> {
    fn new(data: &'a [u8]) -> Self {
        Self {
            data,
            pos: 0,
            error: false,
        }
    }

    fn has_error(&self) -> bool {
        self.error
    }

    fn read_i64(&mut self) -> i64 {
        if self.error || self.pos + 8 > self.data.len() {
            self.error = true;
            return 0;
        }
        let bytes = [
            self.data[self.pos],
            self.data[self.pos + 1],
            self.data[self.pos + 2],
            self.data[self.pos + 3],
            self.data[self.pos + 4],
            self.data[self.pos + 5],
            self.data[self.pos + 6],
            self.data[self.pos + 7],
        ];
        self.pos += 8;
        i64::from_le_bytes(bytes)
    }

    fn read_u64(&mut self) -> u64 {
        if self.error || self.pos + 8 > self.data.len() {
            self.error = true;
            return 0;
        }
        let bytes = [
            self.data[self.pos],
            self.data[self.pos + 1],
            self.data[self.pos + 2],
            self.data[self.pos + 3],
            self.data[self.pos + 4],
            self.data[self.pos + 5],
            self.data[self.pos + 6],
            self.data[self.pos + 7],
        ];
        self.pos += 8;
        u64::from_le_bytes(bytes)
    }

    fn read_i32(&mut self) -> i32 {
        if self.error || self.pos + 4 > self.data.len() {
            self.error = true;
            return 0;
        }
        let bytes = [
            self.data[self.pos],
            self.data[self.pos + 1],
            self.data[self.pos + 2],
            self.data[self.pos + 3],
        ];
        self.pos += 4;
        i32::from_le_bytes(bytes)
    }

    fn read_u32(&mut self) -> u32 {
        if self.error || self.pos + 4 > self.data.len() {
            self.error = true;
            return 0;
        }
        let bytes = [
            self.data[self.pos],
            self.data[self.pos + 1],
            self.data[self.pos + 2],
            self.data[self.pos + 3],
        ];
        self.pos += 4;
        u32::from_le_bytes(bytes)
    }

    fn read_f64(&mut self) -> f64 {
        if self.error || self.pos + 8 > self.data.len() {
            self.error = true;
            return 0.0;
        }
        let bytes = [
            self.data[self.pos],
            self.data[self.pos + 1],
            self.data[self.pos + 2],
            self.data[self.pos + 3],
            self.data[self.pos + 4],
            self.data[self.pos + 5],
            self.data[self.pos + 6],
            self.data[self.pos + 7],
        ];
        self.pos += 8;
        f64::from_le_bytes(bytes)
    }

    fn read_bool1(&mut self) -> bool {
        if self.error || self.pos + 1 > self.data.len() {
            self.error = true;
            return false;
        }
        let b = self.data[self.pos];
        self.pos += 1;
        b != 0
    }

    fn read_string(&mut self) -> String {
        if self.error || self.pos + 4 > self.data.len() {
            self.error = true;
            return String::new();
        }
        let char_count = self.read_i32() as usize;
        let byte_count = char_count * 2;
        if self.pos + byte_count > self.data.len() {
            self.error = true;
            return String::new();
        }
        let mut chars = Vec::with_capacity(char_count);
        for _ in 0..char_count {
            let c = u16::from_le_bytes([self.data[self.pos], self.data[self.pos + 1]]);
            self.pos += 2;
            chars.push(c);
        }
        String::from_utf16_lossy(&chars)
    }

    fn read_fixed_string(&mut self, slot_bytes: usize) -> String {
        if self.error || self.pos + slot_bytes > self.data.len() {
            self.error = true;
            return String::new();
        }
        let end = self.pos + slot_bytes;
        let buf = &self.data[self.pos..end];

        let mut chars = Vec::with_capacity(slot_bytes / 2);
        let mut i = 0;
        while i + 1 < buf.len() {
            let c = u16::from_le_bytes([buf[i], buf[i + 1]]);
            if c == 0 {
                break;
            }
            chars.push(c);
            i += 2;
        }
        self.pos = end;
        String::from_utf16_lossy(&chars)
    }
}

fn read_string_at_offset(data: &[u8], offset: usize) -> String {
    if offset >= data.len() {
        return String::new();
    }

    let mut chars = Vec::new();
    let mut pos = offset;
    while pos + 1 < data.len() {
        let c = u16::from_le_bytes([data[pos], data[pos + 1]]);
        pos += 2;
        if c == 0 {
            break;
        }
        chars.push(c);
    }
    String::from_utf16_lossy(&chars)
}

fn encode_fixed_utf16_slot(value: &str, slot_bytes: usize) -> Vec<u8> {
    let mut slot = vec![0u8; slot_bytes];
    let utf16: Vec<u16> = value.encode_utf16().collect();
    let max_units = slot_bytes / 2;

    for (i, c) in utf16.iter().take(max_units.saturating_sub(1)).enumerate() {
        let bytes = c.to_le_bytes();
        slot[i * 2] = bytes[0];
        slot[i * 2 + 1] = bytes[1];
    }

    slot
}

fn encode_trade_request(request: &TradeRequest) -> Vec<u8> {
    let mut data = Vec::with_capacity(232);
    data.extend_from_slice(&(request.action as u32).to_le_bytes());
    data.extend_from_slice(&request.magic.to_le_bytes());
    data.extend_from_slice(&request.order.to_le_bytes());
    data.extend_from_slice(&encode_fixed_utf16_slot(&request.symbol, 64));
    data.extend_from_slice(&request.volume.to_le_bytes());
    data.extend_from_slice(&request.price.to_le_bytes());
    data.extend_from_slice(&request.stop_limit.to_le_bytes());
    data.extend_from_slice(&request.sl.to_le_bytes());
    data.extend_from_slice(&request.tp.to_le_bytes());
    data.extend_from_slice(&request.deviation.to_le_bytes());
    data.extend_from_slice(&(request.order_type as u32).to_le_bytes());
    data.extend_from_slice(&(request.type_filling as u32).to_le_bytes());
    data.extend_from_slice(&(request.type_time as u32).to_le_bytes());
    data.extend_from_slice(&request.expiration.to_le_bytes());
    data.extend_from_slice(&encode_fixed_utf16_slot(&request.comment, 64));
    data.extend_from_slice(&request.position.to_le_bytes());
    data.extend_from_slice(&request.position_by.to_le_bytes());
    data
}

fn decode_check_result(data: &[u8]) -> Result<CheckResult> {
    const COMMENT_SLOT: usize = 200;
    const TOTAL_BYTES: usize = 252;

    if data.len() < TOTAL_BYTES {
        return Err(Mt5Error::InvalidResponse(format!(
            "Check result too short: {} < {}",
            data.len(),
            TOTAL_BYTES
        )));
    }

    let read_f64 = |offset: usize| {
        f64::from_le_bytes([
            data[offset],
            data[offset + 1],
            data[offset + 2],
            data[offset + 3],
            data[offset + 4],
            data[offset + 5],
            data[offset + 6],
            data[offset + 7],
        ])
    };

    let mut sr = Reader::new(&data[52..(52 + COMMENT_SLOT)]);
    let comment = sr.read_fixed_string(COMMENT_SLOT);
    if sr.has_error() {
        return Err(Mt5Error::InvalidResponse(
            "Failed to decode check result comment".into(),
        ));
    }

    Ok(CheckResult {
        retcode: u32::from_le_bytes([data[0], data[1], data[2], data[3]]),
        balance: read_f64(4),
        equity: read_f64(12),
        profit: read_f64(20),
        margin: read_f64(28),
        margin_free: read_f64(36),
        margin_level: read_f64(44),
        comment,
    })
}

fn decode_trade_result(data: &[u8]) -> Result<TradeResult> {
    const COMMENT_SLOT: usize = 200;
    const TOTAL_BYTES: usize = 260;

    if data.len() < TOTAL_BYTES {
        return Err(Mt5Error::InvalidResponse(format!(
            "Trade result too short: {} < {}",
            data.len(),
            TOTAL_BYTES
        )));
    }

    let read_f64 = |offset: usize| {
        f64::from_le_bytes([
            data[offset],
            data[offset + 1],
            data[offset + 2],
            data[offset + 3],
            data[offset + 4],
            data[offset + 5],
            data[offset + 6],
            data[offset + 7],
        ])
    };

    let mut sr = Reader::new(&data[52..(52 + COMMENT_SLOT)]);
    let comment = sr.read_fixed_string(COMMENT_SLOT);
    if sr.has_error() {
        return Err(Mt5Error::InvalidResponse(
            "Failed to decode trade result comment".into(),
        ));
    }

    Ok(TradeResult {
        retcode: u32::from_le_bytes([data[0], data[1], data[2], data[3]]),
        deal: u64::from_le_bytes([
            data[4], data[5], data[6], data[7], data[8], data[9], data[10], data[11],
        ]),
        order: u64::from_le_bytes([
            data[12], data[13], data[14], data[15], data[16], data[17], data[18], data[19],
        ]),
        volume: read_f64(20),
        price: read_f64(28),
        bid: read_f64(36),
        ask: read_f64(44),
        comment,
        request_id: u32::from_le_bytes([data[252], data[253], data[254], data[255]]),
        retcode_ext: i32::from_le_bytes([data[256], data[257], data[258], data[259]]),
    })
}

fn match_group(symbol: &str, group: &str) -> bool {
    let rules = group
        .split(',')
        .map(str::trim)
        .filter(|rule| !rule.is_empty())
        .collect::<Vec<_>>();

    if rules.is_empty() {
        return true;
    }

    let mut included = false;
    let mut has_positive_rule = false;
    for rule in &rules {
        let excluded = rule.starts_with('!');
        let pattern = if excluded { &rule[1..] } else { rule };
        let matched = wildcard_match(symbol, pattern);

        if excluded && matched {
            return false;
        }

        if !excluded {
            has_positive_rule = true;
            if matched {
                included = true;
            }
        }
    }

    if has_positive_rule {
        included
    } else {
        true
    }
}

fn wildcard_match(value: &str, pattern: &str) -> bool {
    let value = value.to_ascii_lowercase();
    let pattern = pattern.to_ascii_lowercase();

    if pattern == "*" {
        return true;
    }

    if !pattern.contains('*') {
        return value == pattern;
    }

    let mut parts = pattern.split('*').collect::<Vec<_>>();
    if parts.len() == 1 {
        return value == pattern;
    }

    let starts_with_wild = pattern.starts_with('*');
    let ends_with_wild = pattern.ends_with('*');

    if !starts_with_wild {
        let first = parts.remove(0);
        if !value.starts_with(first) {
            return false;
        }
    }

    if !ends_with_wild {
        if let Some(last) = parts.pop() {
            if !value.ends_with(last) {
                return false;
            }
        }
    }

    let mut cursor = 0usize;
    for part in parts {
        if part.is_empty() {
            continue;
        }
        if let Some(found) = value[cursor..].find(part) {
            cursor += found + part.len();
        } else {
            return false;
        }
    }

    true
}
