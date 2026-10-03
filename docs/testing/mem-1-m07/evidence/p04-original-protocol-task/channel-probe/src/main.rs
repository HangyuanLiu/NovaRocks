fn main() {
    assert!(tonic::transport::OriginalConnectionDriver::task_allocation_capacity_bound().unwrap() > 0);
    assert!(tonic::transport::OriginalConnectionDriver::metadata_allocation_capacity_bound().unwrap() > 0);
    assert!(tonic::transport::OriginalHttp2ProtocolTask::task_allocation_capacity_bound().unwrap() > 0);
    assert!(tonic::transport::OriginalHttp2ProtocolTask::metadata_allocation_capacity_bound().unwrap() > 0);
}
