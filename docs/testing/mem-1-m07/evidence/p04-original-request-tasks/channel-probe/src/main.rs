fn main() {
    assert!(tonic::transport::OriginalConnectionDriver::task_allocation_capacity_bound().unwrap() > 0);
    assert!(tonic::transport::OriginalConnectionDriver::metadata_allocation_capacity_bound().unwrap() > 0);
    assert!(tonic::transport::OriginalHttp2ProtocolTask::task_allocation_capacity_bound().unwrap() > 0);
    assert!(tonic::transport::OriginalHttp2ProtocolTask::metadata_allocation_capacity_bound().unwrap() > 0);
    let bounds = tonic::transport::http2_split_client_task_allocation_capacity_bounds().unwrap();
    let pool = tonic::transport::OriginalHttp2RequestTaskPool::allocation_capacity_bound(
        128, bounds.pipe, bounds.send,
    ).unwrap();
    assert!(pool <= 128 * 4096);
    println!("closed_split_connection={} pipe={} send={} pool={}", bounds.connection, bounds.pipe, bounds.send, pool);
}
