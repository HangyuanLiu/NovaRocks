use std::marker::PhantomData;

use prost::Message;
use tonic::Status;
use tonic::codec::{BufferSettings, Codec, DecodeBuf, Decoder, EncodeBuf, Encoder};

/// FE uses the same native Prost framing as BE.  BE additionally owns raw
/// StageFragments validation because it is the only native ingress.
#[derive(Debug, Clone)]
pub(crate) struct NativeProstCodec<T, U>(PhantomData<(T, U)>);

impl<T, U> Default for NativeProstCodec<T, U> {
    fn default() -> Self {
        Self(PhantomData)
    }
}

impl<T, U> Codec for NativeProstCodec<T, U>
where
    T: Message + Send + 'static,
    U: Message + Default + Send + 'static,
{
    type Encode = T;
    type Decode = U;
    type Encoder = NativeProstEncoder<T>;
    type Decoder = NativeProstDecoder<U>;
    fn encoder(&mut self) -> Self::Encoder {
        NativeProstEncoder::default()
    }
    fn decoder(&mut self) -> Self::Decoder {
        NativeProstDecoder::default()
    }
}

#[derive(Debug, Clone)]
pub(crate) struct NativeProstEncoder<T> {
    marker: PhantomData<T>,
    settings: BufferSettings,
}
impl<T> Default for NativeProstEncoder<T> {
    fn default() -> Self {
        Self {
            marker: PhantomData,
            settings: BufferSettings::default(),
        }
    }
}
impl<T: Message> Encoder for NativeProstEncoder<T> {
    type Item = T;
    type Error = Status;
    fn encode(&mut self, item: T, dst: &mut EncodeBuf<'_>) -> Result<(), Status> {
        item.encode(dst)
            .expect("message encode only fails when buffer allocation fails");
        Ok(())
    }
    fn buffer_settings(&self) -> BufferSettings {
        self.settings
    }
}

#[derive(Debug, Clone)]
pub(crate) struct NativeProstDecoder<U> {
    marker: PhantomData<U>,
    settings: BufferSettings,
}
impl<U> Default for NativeProstDecoder<U> {
    fn default() -> Self {
        Self {
            marker: PhantomData,
            settings: BufferSettings::default(),
        }
    }
}
impl<U> Decoder for NativeProstDecoder<U>
where
    U: Message + Default + Send + 'static,
{
    type Item = U;
    type Error = Status;
    fn decode(&mut self, src: &mut DecodeBuf<'_>) -> Result<Option<U>, Status> {
        U::decode(src)
            .map(Some)
            .map_err(|error| Status::internal(error.to_string()))
    }
    fn buffer_settings(&self) -> BufferSettings {
        self.settings
    }
}
