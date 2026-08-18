#[doc = "../README.md"]

pub mod client;
pub mod error;
pub mod protocol;
mod trade;
pub mod types;


pub use client::{Mt5Client, QuotePoll};
pub use error::{Mt5Error, Result};
pub use protocol::{discover_mt5_pipe, try_discover_mt5_pipe};
pub use types::*;