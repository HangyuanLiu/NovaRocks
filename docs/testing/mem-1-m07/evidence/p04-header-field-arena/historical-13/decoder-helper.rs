// Thin wrappers over actual new_bounded/BorrowedSource/decode_source. Returned
// fields retain the actual Header, never a DTO copy. Table and Huffman are real.
#[doc(hidden)]
#[allow(missing_docs)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct M07ArenaField(Header);
#[allow(missing_docs)]
impl M07ArenaField {
    pub fn value_slice(&self) -> &[u8] {
        self.0.value_slice()
    }
    pub fn name_pointer(&self) -> *const u8 {
        self.0.name().as_slice().as_ptr()
    }
    pub fn value_pointer(&self) -> *const u8 {
        self.0.value_slice().as_ptr()
    }
}
#[doc(hidden)]
#[allow(missing_docs)]
#[derive(Debug)]
pub struct M07ArenaDecoder {
    inner: Decoder,
}
#[allow(missing_docs)]
impl M07ArenaDecoder {
    pub fn new(pool: Option<crate::ReceiveHeaderFieldPool>, max: usize, encoded: usize) -> Self {
        Self {
            inner: Decoder::new_bounded(4096, max, encoded, pool),
        }
    }
    pub fn borrowed(
        &mut self,
        input: &[u8],
        committed: &mut usize,
    ) -> (Vec<M07ArenaField>, Result<(), crate::M07FieldError>) {
        let mut fields = Vec::new();
        let mut source = BorrowedSource::new(input, *committed);
        let result = self
            .inner
            .decode_source(&mut source, |field| fields.push(M07ArenaField(field)));
        *committed = source.committed();
        (fields, result.map_err(crate::M07FieldError))
    }
    pub fn discard(
        &mut self,
        input: &[u8],
        begin: impl FnOnce(),
        end: impl FnOnce(),
    ) -> (usize, Result<(), crate::M07FieldError>) {
        let mut source = BorrowedSource::new(input, 0);
        begin();
        let result = self.inner.decode_source(&mut source, drop);
        end();
        (source.committed(), result.map_err(crate::M07FieldError))
    }
    pub fn table_state(&self) -> (usize, usize, usize) {
        (
            self.inner.table.size,
            self.inner.table.entries.len(),
            self.inner.table.max_size,
        )
    }
    pub fn huffman(input: &[u8]) -> Vec<u8> {
        let mut output = BytesMut::new();
        huffman::encode(input, &mut output);
        output.to_vec()
    }
}
