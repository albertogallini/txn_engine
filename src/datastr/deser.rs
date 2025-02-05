use rust_decimal::{Decimal, RoundingStrategy};
use serde::{de, Deserialize, Deserializer};
use std::{fmt, str::FromStr};

/// Deserialize an amount from a CSV string.
///
/// If the string is empty, the result is `None`. Otherwise, the amount is parsed
/// from the string and rounded to four decimal places using the midpoint away
/// from zero rounding strategy. If parsing fails, an error is returned.
pub fn deserialize_amount<'de, D>(deserializer: D) -> Result<Option<Decimal>, D::Error>
where
    D: Deserializer<'de>,
{
    let s: Option<String> = Option::deserialize(deserializer)?;
    match s {
        Some(ref v) if !v.trim().is_empty() => Decimal::from_str(v.trim())
            .map(|mut d| {
                d = d.round_dp_with_strategy(4, RoundingStrategy::MidpointAwayFromZero);
                Some(d)
            })
            .map_err(de::Error::custom),
        _ => Ok(None),
    }
}

/// Deserialize an amount from a CSV string and round it to four decimal places using the midpoint away
/// from zero rounding strategy. If the string is empty, return an error instead of Option::None.
///
/// The input string is trimmed before parsing. If parsing fails, an error is returned.
pub fn deserialize_account_amount<'de, D>(deserializer: D) -> Result<Decimal, D::Error>
where
    D: Deserializer<'de>,
{
    let s: String = String::deserialize(deserializer)?;
    if s.trim().is_empty() {
        // If the string is empty, return an error instead of Option::None
        return Err(de::Error::custom("Amount cannot be an empty string"));
    }

    Decimal::from_str(s.trim())
        .map(|mut d| {
            d = d.round_dp_with_strategy(4, RoundingStrategy::MidpointAwayFromZero);
            d
        })
        .map_err(de::Error::custom)
}

// Helper function to deserialize and trim strings for any type T that can be FromStr
pub fn deserialize_trimmed_string<'de, T, D>(deserializer: D) -> Result<T, D::Error>
where
    T: FromStr,
    T::Err: fmt::Display,
    D: Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?;
    T::from_str(s.trim()).map_err(de::Error::custom)
}
