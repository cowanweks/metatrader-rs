use metatrader_rs::{try_discover_mt5_pipe, Mt5Client};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let pipe = try_discover_mt5_pipe()?;

    let mut client = Mt5Client::new();
    client.initialize(Some(&pipe))?;

    println!("Connected: {}", client.is_connected());
    println!("Terminal build: {}", client.build());

    let version = client.version()?;
    println!("Version: {}", version.version);
    println!("Build: {}", version.build);

    let terminal = client.terminal_info()?;
    println!("Terminal name: {}", terminal.name);
    println!("Company: {}", terminal.company);
    println!("Trade allowed: {}", terminal.trade_allowed);

    let account = client.account_info()?;
    println!("Login: {}", account.login);
    println!("Server: {}", account.server);
    println!("Balance: {}", account.balance);
    println!("Equity: {}", account.equity);
    println!("Margin free: {}", account.free_margin);

    client.close();
    Ok(())
}
