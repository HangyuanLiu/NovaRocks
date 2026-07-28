use crate::common::types::UniqueId;
use crate::runtime::endpoint::RuntimeEndpoint;

use super::FragmentIoError;

#[derive(Clone, Debug)]
pub struct ExchangeFrame {
    pub destination: RuntimeEndpoint,
    pub destination_fragment_instance_id: UniqueId,
    pub sender_fragment_instance_id: UniqueId,
    pub destination_node_id: i32,
    pub sender_id: i32,
    pub backend_number: i32,
    pub sequence: i64,
    pub eos: bool,
    pub payload: Vec<u8>,
}

pub trait ExchangeFrameTransmitter: Send + Sync + 'static {
    fn transmit(&self, frame: ExchangeFrame) -> Result<(), FragmentIoError>;
}
