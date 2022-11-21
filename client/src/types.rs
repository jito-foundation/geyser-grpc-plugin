//! Contains application specific representations of proto definitions.

use solana_sdk::{pubkey::Pubkey, slot_hashes::Slot};

use crate::geyser_proto::{
    AccountUpdate as PbAccountUpdate, PartialAccountUpdate as PbPartialAccountUpdate,
    SlotUpdate as PbSlotUpdate, SlotUpdateStatus as PbSlotUpdateStatus,
};

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum SlotStatus {
    Rooted,
    Confirmed,
    Processed,
}

pub(crate) trait AccountUpdateNotification {
    fn pubkey(&self) -> Pubkey;
    fn set_seq(&mut self, seq: u64);
    fn seq(&self) -> u64;
    fn slot(&self) -> Slot;
}

pub struct AccountUpdate {
    pub pubkey: Pubkey,
    pub owner: Pubkey,
    pub data: Vec<u8>,
    pub tx_signature: Option<String>,
    pub slot: Slot,
    pub lamports: u64,
    pub rent_epoch: u64,
    pub seq: u64,
    pub replica_version: u32,
    pub is_executable: bool,
    pub is_startup: bool,
}

impl AccountUpdateNotification for AccountUpdate {
    fn pubkey(&self) -> Pubkey {
        self.pubkey
    }
    fn set_seq(&mut self, seq: u64) {
        self.seq = seq;
    }
    fn seq(&self) -> u64 {
        self.seq
    }
    fn slot(&self) -> Slot {
        self.slot
    }
}

impl From<PbAccountUpdate> for AccountUpdate {
    fn from(proto: PbAccountUpdate) -> Self {
        let pubkey = Pubkey::new(&proto.pubkey[..]);
        let owner = Pubkey::new(&proto.owner[..]);

        Self {
            pubkey,
            owner,
            data: proto.data,
            tx_signature: proto.tx_signature,
            slot: proto.slot,
            lamports: proto.lamports,
            rent_epoch: proto.rent_epoch,
            seq: proto.seq,
            is_executable: proto.is_executable,
            is_startup: proto.is_startup,
            replica_version: proto.replica_version,
        }
    }
}

pub struct PartialAccountUpdate {
    pub pubkey: Pubkey,
    pub tx_signature: Option<String>,
    pub slot: Slot,
    pub seq: u64,
    pub is_startup: bool,
}

impl AccountUpdateNotification for PartialAccountUpdate {
    fn pubkey(&self) -> Pubkey {
        self.pubkey
    }
    fn set_seq(&mut self, seq: u64) {
        self.seq = seq;
    }
    fn seq(&self) -> u64 {
        self.seq
    }
    fn slot(&self) -> Slot {
        self.slot
    }
}

impl From<PbPartialAccountUpdate> for PartialAccountUpdate {
    fn from(proto: PbPartialAccountUpdate) -> Self {
        let pubkey = Pubkey::new(&proto.pubkey[..]);

        Self {
            pubkey,
            tx_signature: proto.tx_signature,
            slot: proto.slot,
            seq: proto.seq,
            is_startup: proto.is_startup,
        }
    }
}

pub enum SlotUpdateStatus {
    Confirmed,
    Processed,
    Rooted,
}

impl From<PbSlotUpdateStatus> for SlotUpdateStatus {
    fn from(proto: PbSlotUpdateStatus) -> Self {
        match proto {
            PbSlotUpdateStatus::Confirmed => Self::Confirmed,
            PbSlotUpdateStatus::Processed => Self::Processed,
            PbSlotUpdateStatus::Rooted => Self::Rooted,
        }
    }
}

pub struct SlotUpdate {
    pub parent_slot: Option<Slot>,
    pub slot: Slot,
    pub status: SlotUpdateStatus,
}

impl From<PbSlotUpdate> for SlotUpdate {
    fn from(proto: PbSlotUpdate) -> Self {
        let pb_status = PbSlotUpdateStatus::from_i32(proto.status).unwrap();
        Self {
            parent_slot: proto.parent_slot,
            slot: proto.slot,
            status: SlotUpdateStatus::from(pb_status),
        }
    }
}
