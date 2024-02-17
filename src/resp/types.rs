use std::collections::VecDeque;

/// RESP data type 	Minimal protocol version 	Category 	First byte
/// Simple strings 	RESP2 	Simple 	+
/// Simple Errors 	RESP2 	Simple 	-
/// Integers 	RESP2 	Simple 	:
/// Bulk strings 	RESP2 	Aggregate 	$
/// Arrays 	RESP2 	Aggregate 	*

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
    #[allow(dead_code)]
    pub(crate) const fn first_byte(&self) -> u8 {
        match self {
            Self::SimpleString(_) => b'+',
            Self::SimpleError(_) => b'-',
            Self::Integer(_) => b':',
            Self::BulkString(_) | Self::NullBulkString => b'$',
            Self::Array(_) => b'*',
        }
    }
}
