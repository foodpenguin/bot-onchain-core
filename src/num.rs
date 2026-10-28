//! Chain/protocol-neutral numeric helpers: full-precision mul-div and unit conversion. Protocol math
//! (e.g. `morpho::math`) builds on these; the generic planner uses them directly.

use alloy::primitives::{U256, U512};

fn wide(x: U256) -> U512 {
    U512::from(x)
}
fn narrow(x: U512) -> U256 {
    U256::from(x)
}

/// floor(x * y / d) without intermediate overflow.
pub fn mul_div_down(x: U256, y: U256, d: U256) -> U256 {
    narrow(wide(x) * wide(y) / wide(d))
}
/// ceil(x * y / d) without intermediate overflow.
pub fn mul_div_up(x: U256, y: U256, d: U256) -> U256 {
    narrow((wide(x) * wide(y) + wide(d) - U512::from(1u8)) / wide(d))
}

/// Lossy U256 -> f64 (logging, ranking and off-chain estimates only).
pub fn u256_f64(x: U256) -> f64 {
    x.to_string().parse::<f64>().unwrap_or(f64::MAX)
}

/// Token amount with `decimals` -> f64 units.
pub fn units(x: U256, decimals: u8) -> f64 {
    u256_f64(x) / 10f64.powi(decimals as i32)
}
