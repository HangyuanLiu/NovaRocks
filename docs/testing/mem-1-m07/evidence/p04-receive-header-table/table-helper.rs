// Scratch-only opaque access to the actual typed ring, Header and Layout.
#[doc(hidden)]
#[allow(missing_docs)]
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct M07TableError(pub(crate) DecoderError);
#[allow(missing_docs)]
impl M07TableError {
    pub fn is_need_more(self) -> bool {
        matches!(self.0, DecoderError::NeedMore(_))
    }
    pub fn is_invalid_max(self) -> bool {
        self.0 == DecoderError::InvalidMaxDynamicSize
    }
    pub fn is_table_full(self) -> bool {
        self.0 == DecoderError::HeaderTableBufferExhausted
    }
}
#[doc(hidden)]
#[allow(missing_docs)]
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct M07TableHeader(pub(crate) Header);
#[allow(missing_docs)]
impl M07TableHeader {
    pub fn new(name: Bytes, value: Bytes) -> Result<Self, M07TableError> {
        Header::new_shared(name, value)
            .map(Self)
            .map_err(M07TableError)
    }
    pub fn from_pool(
        pool: &crate::ReceiveHeaderFieldPool,
        name: &[u8],
        value: &[u8],
    ) -> Result<Self, M07TableError> {
        let name = pool
            .try_fill(name.len(), |out| {
                out.copy_from_slice(name);
                Ok(())
            })
            .map_err(M07TableError)?;
        let value = pool
            .try_fill(value.len(), |out| {
                out.copy_from_slice(value);
                Ok(())
            })
            .map_err(M07TableError)?;
        Self::new(name, value)
    }
    pub fn value(&self) -> &[u8] {
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
pub struct M07BoundTable(pub(crate) BoundHeaderTableBuffer);
#[allow(missing_docs)]
impl M07BoundTable {
    pub fn bind(pool: &ReceiveHeaderTableBuffer) -> io::Result<Self> {
        pool.bind().map(Self)
    }
    pub fn len(&self) -> usize {
        self.0.len
    }
    pub fn capacity(&self) -> usize {
        self.0.slots.capacity()
    }
    pub fn backing_pointer(&self) -> *const () {
        self.0.slots.as_ptr().cast()
    }
    pub fn get(&self, index: usize) -> Option<M07TableHeader> {
        self.0.get(index).cloned().map(M07TableHeader)
    }
    pub fn back(&self) -> Option<M07TableHeader> {
        self.0.back().cloned().map(M07TableHeader)
    }
    pub fn pop_back(&mut self) -> Option<M07TableHeader> {
        self.0.pop_back().map(M07TableHeader)
    }
    pub fn push_front(&mut self, header: M07TableHeader) -> Result<(), M07TableError> {
        self.0.push_front(header.0).map_err(M07TableError)
    }
}
pub(crate) fn m07_fixed_table_len(table: &BoundHeaderTableBuffer) -> usize {
    table.len
}
#[doc(hidden)]
#[allow(missing_docs)]
pub fn m07_table_slot_layout(max: usize) -> (usize, usize) {
    let layout = std::alloc::Layout::array::<Option<Header>>(max / 32).unwrap();
    (layout.size(), layout.align())
}
