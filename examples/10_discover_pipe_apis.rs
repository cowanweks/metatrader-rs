use metatrader_rs::{discover_mt5_pipe, try_discover_mt5_pipe};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let discovered = try_discover_mt5_pipe()?;
    println!("try_discover_mt5_pipe => {}", discovered);

    if std::env::var("MT5_USE_PANIC_DISCOVER").ok().as_deref() == Some("1") {
        let panicking_variant = discover_mt5_pipe();
        println!("discover_mt5_pipe => {}", panicking_variant);
    } else {
        println!("Set MT5_USE_PANIC_DISCOVER=1 to call discover_mt5_pipe().");
    }

    Ok(())
}
