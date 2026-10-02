// Appended only to the actual decoder module in a complete scratch h2 normal
// dependency. The decoder, sources and dynamic table are not replaced.
#[doc(hidden)]
#[allow(missing_docs)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct M07HpackField(Header);
#[allow(missing_docs)]
impl M07HpackField {
    pub fn shared(name: bytes::Bytes, value: bytes::Bytes) -> Result<Self, M07HpackError> {
        Header::new_shared(name, value)
            .map(Self)
            .map_err(M07HpackError)
    }
    pub fn legacy(name: bytes::Bytes, value: bytes::Bytes) -> Result<Self, M07HpackError> {
        Header::new(name, value).map(Self).map_err(M07HpackError)
    }
    pub fn name_pointer(&self) -> *const u8 {
        self.0.name().as_slice().as_ptr()
    }
    pub fn value_pointer(&self) -> *const u8 {
        self.0.value_slice().as_ptr()
    }
    pub fn replace_shared(&self, value: bytes::Bytes) -> Result<Self, M07HpackError> {
        self.0
            .name()
            .into_entry_shared(value)
            .map(Self)
            .map_err(M07HpackError)
    }
    pub fn value_slice(&self) -> &[u8] {
        self.0.value_slice()
    }
}
#[doc(hidden)]
#[allow(missing_docs)]
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub struct M07HpackError(DecoderError);
#[allow(missing_docs)]
impl M07HpackError {
    #[allow(non_upper_case_globals)]
    pub const InvalidMaxDynamicSize: Self = Self(DecoderError::InvalidMaxDynamicSize);
    pub fn is_need_more(self) -> bool {
        matches!(self.0, DecoderError::NeedMore(_))
    }
}
#[doc(hidden)]
#[allow(missing_docs)]
#[derive(Debug)]
pub struct M07HpackDecoder {
    inner: Decoder,
}
#[allow(missing_docs)]
impl M07HpackDecoder {
    pub fn new() -> Self {
        let mut inner = Decoder::new(4096);
        inner.set_max_field_size(16384, 16384);
        Self { inner }
    }
    pub fn borrowed(
        &mut self,
        input: &[u8],
        committed: &mut usize,
    ) -> (Vec<M07HpackField>, Result<(), M07HpackError>) {
        let mut output = Vec::new();
        let mut source = BorrowedSource::new(input, *committed);
        let result = self
            .inner
            .decode_source(&mut source, |value| output.push(M07HpackField(value)));
        *committed = source.committed();
        (output, result.map_err(M07HpackError))
    }
    pub fn owned(
        &mut self,
        input: &mut BytesMut,
    ) -> (Vec<M07HpackField>, Result<(), M07HpackError>) {
        let mut output = Vec::new();
        let result = self.inner.decode_source(&mut Cursor::new(input), |value| {
            output.push(M07HpackField(value))
        });
        (output, result.map_err(M07HpackError))
    }
    // Direct actual Table insertion is a physical retention probe, not a claim
    // that production decoder outputs have acquired an original funding grant.
    pub fn retain_funded(&mut self, value: M07HpackField) {
        self.inner.table.insert(value.0);
    }
    pub fn table_state(&self) -> (usize, usize, usize) {
        (
            self.inner.table.size,
            self.inner.table.entries.len(),
            self.inner.table.max_size,
        )
    }
    pub fn discard_borrowed(
        &mut self,
        input: &[u8],
        begin: impl FnOnce(),
        end: impl FnOnce(),
    ) -> Result<(), M07HpackError> {
        let mut source = BorrowedSource::new(input, 0);
        begin();
        let result = self.inner.decode_source(&mut source, drop);
        end();
        result.map_err(M07HpackError)
    }
    pub fn discard_owned(
        &mut self,
        input: &[u8],
        begin: impl FnOnce(),
        end: impl FnOnce(),
    ) -> Result<(), M07HpackError> {
        begin();
        let mut owned = BytesMut::from(input);
        let result = self.inner.decode_source(&mut Cursor::new(&mut owned), drop);
        end();
        result.map_err(M07HpackError)
    }
    pub fn huffman(input: &[u8]) -> Vec<u8> {
        let mut output = BytesMut::new();
        huffman::encode(input, &mut output);
        output.to_vec()
    }
}
