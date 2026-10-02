// Appended only to scratch receive_pool; exact private types feed the existing
// bytes metadata Layout getter, so the driver never guesses descriptor bytes.
#[doc(hidden)]
pub fn m07_data_wrapper_metadata_size() -> usize {
    Bytes::owner_with_exit_guard_metadata_size::<PoolBuffer, BufferExit>()
}
