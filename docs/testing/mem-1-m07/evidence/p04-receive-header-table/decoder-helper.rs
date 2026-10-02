// Scratch-only access to the actual decoder/table protocol state. No HPACK model.
#[doc(hidden)]
#[allow(missing_docs)]
#[derive(Debug)]
pub struct M07TableDecoder {
    inner: Decoder,
}
#[allow(missing_docs)]
impl M07TableDecoder {
    pub fn new(
        pool: Option<crate::ReceiveHeaderFieldPool>,
        table: Option<crate::M07BoundTable>,
    ) -> Self {
        Self {
            inner: Decoder::new_bounded_with_table(4096, 16384, 16384, pool, table.map(|t| t.0)),
        }
    }
    pub fn ack(&mut self, size: usize) {
        self.inner.queue_size_update(size);
    }
    pub fn begin(&mut self) {
        self.inner.begin_block();
    }
    pub fn finish(&self) -> Result<(), crate::M07TableError> {
        self.inner.finish_block().map_err(crate::M07TableError)
    }
    pub fn state(&self) -> (usize, usize, usize, usize, Option<usize>, bool) {
        let len = match &self.inner.table.entries {
            TableEntries::Default(entries) => entries.len(),
            TableEntries::Fixed(entries) => {
                crate::receive_header_table::m07_fixed_table_len(entries)
            }
        };
        (
            self.inner.table.size,
            len,
            self.inner.table.max_size,
            self.inner.last_max_update,
            self.inner.required_min,
            self.inner.can_resize,
        )
    }
    pub fn borrowed(
        &mut self,
        input: &[u8],
        committed: &mut usize,
    ) -> (Vec<crate::M07TableHeader>, Result<(), crate::M07TableError>) {
        let mut output = Vec::new();
        let mut source = BorrowedSource::new(input, *committed);
        let result = self.inner.decode_source(&mut source, |header| {
            output.push(crate::M07TableHeader(header))
        });
        *committed = source.committed();
        (output, result.map_err(crate::M07TableError))
    }
    pub fn discard(
        &mut self,
        input: &[u8],
        committed: &mut usize,
    ) -> Result<(), crate::M07TableError> {
        let mut source = BorrowedSource::new(input, *committed);
        let result = self.inner.decode_source(&mut source, drop);
        *committed = source.committed();
        result.map_err(crate::M07TableError)
    }
}
