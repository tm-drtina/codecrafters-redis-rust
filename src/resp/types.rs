use std::collections::VecDeque;

use anyhow::Context;

/// RESP2 types
#[derive(Debug)]
pub enum RespType {
    SimpleString(String),
    SimpleError(String),
    Integer(i64),
    BulkString(Box<[u8]>),
    NullBulkString,
    Array(VecDeque<Self>),
}

impl RespType {
    pub(crate) const fn first_byte(&self) -> u8 {
        match self {
            Self::SimpleString(_) => b'+',
            Self::SimpleError(_) => b'-',
            Self::Integer(_) => b':',
            Self::BulkString(_) | Self::NullBulkString => b'$',
            Self::Array(_) => b'*',
        }
    }

    #[allow(dead_code)]
    pub(crate) fn as_str_bytes(&self) -> anyhow::Result<&[u8]> {
        match self {
            Self::SimpleString(s) => Ok(s.as_bytes()),
            Self::BulkString(s) => Ok(s.as_ref()),
            Self::SimpleError(_) | Self::Integer(_) | Self::NullBulkString | Self::Array(_) => {
                anyhow::bail!("Value is not a string type")
            }
        }
    }

    pub(crate) fn as_int(&self) -> anyhow::Result<i64> {
        match self {
            Self::Integer(i) => Ok(*i),
            Self::SimpleString(s) => s.parse().context("Failed to parse str as int"),
            Self::BulkString(s) => std::str::from_utf8(s)?.parse().context("Failed to parse str as int"),
            Self::SimpleError(_)
            | Self::NullBulkString
            | Self::Array(_) => {
                anyhow::bail!("Value is not a string type")
            }
        }
    }

    pub(crate) fn make_str_bytes_lowercase(&mut self) -> anyhow::Result<&[u8]> {
        match self {
            Self::SimpleString(s) => {
                s.make_ascii_lowercase();
                Ok(s.as_bytes())
            }
            Self::BulkString(s) => {
                s.make_ascii_lowercase();
                Ok(s.as_mut())
            }
            Self::SimpleError(_) | Self::Integer(_) | Self::NullBulkString | Self::Array(_) => {
                anyhow::bail!("Value is not a string type")
            }
        }
    }

    pub(crate) fn bulk_string_from_bytes(bytes: &[u8]) -> Self {
        RespType::BulkString(bytes.to_vec().into_boxed_slice())
    }
    pub(crate) fn bulk_string_from_string(s: String) -> Self {
        RespType::BulkString(s.into_bytes().into_boxed_slice())
    }
}
