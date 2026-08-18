use metatrader_rs::{try_discover_mt5_pipe, Mt5Client};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let pipe = try_discover_mt5_pipe()?;

    let mut client = Mt5Client::new();
    client.initialize(Some(&pipe))?;

    println!(
        "connected={} build={}",
        client.is_connected(),
        client.build()
    );

    if let (Ok(login), Ok(password), Ok(server)) = (
        std::env::var("MT5_LOGIN"),
        std::env::var("MT5_PASSWORD"),
        std::env::var("MT5_SERVER"),
    ) {
        let login: i64 = login.parse()?;
        client.login(login, &password, &server)?;
        println!("login() completed for account {}", login);
    } else {
        println!("Set MT5_LOGIN, MT5_PASSWORD and MT5_SERVER to exercise login().");
    }

    let raw = client.send_raw_command(3, &[])?;
    println!("send_raw_command(cmd=3) returned {} bytes", raw.len());

    let (code, message) = client.last_error()?;
    println!("last_error code={} message={}", code, message);

    client.shutdown();
    println!("connected_after_shutdown={}", client.is_connected());

    Ok(())
}
