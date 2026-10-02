// Appended only to the actual pool module in the scratch normal dependency.
// These wrappers expose existing operations, not another allocator algorithm.
#[doc(hidden)]
#[allow(missing_docs)]
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub struct M07FieldError(pub(crate) crate::hpack::DecoderError);
#[allow(missing_docs)]
impl M07FieldError {
    pub fn is_need_more(self) -> bool {
        matches!(self.0, crate::hpack::DecoderError::NeedMore(_))
    }
    pub fn is_exhausted(self) -> bool {
        self.0 == crate::hpack::DecoderError::HeaderFieldPoolExhausted
    }
    pub fn is_too_large(self) -> bool {
        self.0 == crate::hpack::DecoderError::HeaderFieldTooLarge
    }
    pub fn fill_error() -> Self {
        Self(crate::hpack::DecoderError::InvalidUtf8)
    }
}
#[doc(hidden)]
#[allow(missing_docs)]
pub fn m07_field_fill(
    pool: &ReceiveHeaderFieldPool,
    len: usize,
    fill: impl FnOnce(&mut [u8]) -> Result<(), M07FieldError>,
) -> Result<Bytes, M07FieldError> {
    pool.try_fill(len, |dst| fill(dst).map_err(|error| error.0))
        .map_err(M07FieldError)
}
#[doc(hidden)]
#[allow(missing_docs)]
pub fn m07_field_copy(pool: &ReceiveHeaderFieldPool, input: &[u8]) -> Result<Bytes, M07FieldError> {
    m07_field_fill(pool, input.len(), |dst| {
        dst.copy_from_slice(input);
        Ok(())
    })
}
#[doc(hidden)]
#[allow(missing_docs)]
pub fn m07_field_bind(pool: &ReceiveHeaderFieldPool) -> io::Result<ReceiveHeaderFieldPool> {
    pool.bind()
}
#[doc(hidden)]
#[allow(missing_docs)]
pub fn m07_field_wrapper_size() -> usize {
    Bytes::owner_with_exit_guard_metadata_size::<PoolField, FieldExit>()
}
