mod decoder;
mod encoder;
pub(crate) mod header;
pub(crate) mod huffman;
mod table;

#[cfg(test)]
mod test;

pub(crate) use self::decoder::{BorrowedSource, DecodeSource};
pub use self::decoder::{Decoder, DecoderError, NeedMore};
pub use self::encoder::Encoder;
pub(crate) use self::encoder::FixedEncodeBuffer;
pub use self::header::{BytesStr, Header};
