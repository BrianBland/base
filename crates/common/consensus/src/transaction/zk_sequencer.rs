//! Canonical zk-backed sequencer transaction type.

use alloc::vec::Vec;
use core::mem;

use alloy_consensus::{Sealable, Transaction, Typed2718};
use alloy_eips::{
    eip2718::{Decodable2718, Eip2718Error, Eip2718Result, Encodable2718, IsTyped2718},
    eip2930::AccessList,
    eip7702::SignedAuthorization,
};
use alloy_primitives::{Address, B256, Bytes, ChainId, TxHash, TxKind, U256, keccak256};
use alloy_rlp::{BufMut, Decodable, Encodable, Header};

use crate::{OpTxType, ZK_SEQUENCER_TX_TYPE_ID, ZkSequencerTxBody};

/// Sequencer transaction whose sender is explicit and whose signatures are proven in-batch.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Default)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(rename_all = "camelCase"))]
pub struct TxZkSequencer {
    /// Explicit sender address proven by the enclosing zk proof.
    pub sender: Address,
    /// Unsigned EVM transaction body.
    pub body: ZkSequencerTxBody,
}

impl TxZkSequencer {
    /// Creates a new zk-backed sequencer transaction.
    pub const fn new(sender: Address, body: ZkSequencerTxBody) -> Self {
        Self { sender, body }
    }

    /// Returns the encoded length of the transaction fields, excluding the RLP list header.
    pub fn rlp_encoded_fields_length(&self) -> usize {
        self.sender.length() + self.body.rlp_encoded_fields_length()
    }

    /// Encodes the transaction fields, excluding the RLP list header.
    pub fn rlp_encode_fields(&self, out: &mut dyn BufMut) {
        self.sender.encode(out);
        self.body.rlp_encode_fields(out);
    }

    /// Decodes the transaction fields, excluding the RLP list header.
    pub fn rlp_decode_fields(buf: &mut &[u8]) -> alloy_rlp::Result<Self> {
        Ok(Self {
            sender: Decodable::decode(buf)?,
            body: ZkSequencerTxBody::rlp_decode_fields(buf)?,
        })
    }

    /// Decodes the transaction from RLP bytes.
    pub fn rlp_decode(buf: &mut &[u8]) -> alloy_rlp::Result<Self> {
        let header = Header::decode(buf)?;
        if !header.list {
            return Err(alloy_rlp::Error::UnexpectedString);
        }
        let remaining = buf.len();

        if header.payload_length > remaining {
            return Err(alloy_rlp::Error::InputTooShort);
        }

        let this = Self::rlp_decode_fields(buf)?;

        if buf.len() + header.payload_length != remaining {
            return Err(alloy_rlp::Error::UnexpectedLength);
        }

        Ok(this)
    }

    /// Returns a heuristic in-memory size for the transaction.
    pub fn size(&self) -> usize {
        mem::size_of::<Address>() + self.body.size()
    }

    /// Returns the transaction type.
    pub const fn tx_type(&self) -> OpTxType {
        OpTxType::ZkSequencer
    }

    /// Returns the RLP header for the transaction.
    pub fn rlp_header(&self) -> Header {
        Header { list: true, payload_length: self.rlp_encoded_fields_length() }
    }

    /// RLP encodes the transaction.
    pub fn rlp_encode(&self, out: &mut dyn BufMut) {
        self.rlp_header().encode(out);
        self.rlp_encode_fields(out);
    }

    /// Returns the RLP encoded length of the transaction.
    pub fn rlp_encoded_length(&self) -> usize {
        self.rlp_header().length_with_payload()
    }

    /// Returns the EIP-2718 encoded length of the transaction.
    pub fn eip2718_encoded_length(&self) -> usize {
        self.rlp_encoded_length() + 1
    }

    /// Returns the outer network RLP header.
    pub fn network_header(&self) -> Header {
        Header { list: false, payload_length: self.eip2718_encoded_length() }
    }

    /// Returns the network-encoded length of the transaction.
    pub fn network_encoded_length(&self) -> usize {
        self.network_header().length_with_payload()
    }

    /// Network encodes the transaction.
    pub fn network_encode(&self, out: &mut dyn BufMut) {
        self.network_header().encode(out);
        self.encode_2718(out);
    }

    /// Calculates the transaction hash.
    pub fn tx_hash(&self) -> TxHash {
        let mut buf = Vec::with_capacity(self.eip2718_encoded_length());
        self.encode_2718(&mut buf);
        keccak256(&buf)
    }
}

impl Typed2718 for TxZkSequencer {
    fn ty(&self) -> u8 {
        ZK_SEQUENCER_TX_TYPE_ID
    }
}

impl IsTyped2718 for TxZkSequencer {
    fn is_type(ty: u8) -> bool {
        ty == ZK_SEQUENCER_TX_TYPE_ID
    }
}

impl Encodable2718 for TxZkSequencer {
    fn encode_2718_len(&self) -> usize {
        self.eip2718_encoded_length()
    }

    fn encode_2718(&self, out: &mut dyn BufMut) {
        out.put_u8(ZK_SEQUENCER_TX_TYPE_ID);
        self.rlp_encode(out);
    }
}

impl Transaction for TxZkSequencer {
    fn chain_id(&self) -> Option<ChainId> {
        self.body.chain_id()
    }

    fn nonce(&self) -> u64 {
        self.body.nonce()
    }

    fn gas_limit(&self) -> u64 {
        self.body.gas_limit()
    }

    fn gas_price(&self) -> Option<u128> {
        self.body.gas_price()
    }

    fn max_fee_per_gas(&self) -> u128 {
        self.body.max_fee_per_gas()
    }

    fn max_priority_fee_per_gas(&self) -> Option<u128> {
        self.body.max_priority_fee_per_gas()
    }

    fn max_fee_per_blob_gas(&self) -> Option<u128> {
        self.body.max_fee_per_blob_gas()
    }

    fn priority_fee_or_price(&self) -> u128 {
        self.body.priority_fee_or_price()
    }

    fn effective_gas_price(&self, base_fee: Option<u64>) -> u128 {
        self.body.effective_gas_price(base_fee)
    }

    fn is_dynamic_fee(&self) -> bool {
        self.body.is_dynamic_fee()
    }

    fn kind(&self) -> TxKind {
        self.body.kind()
    }

    fn is_create(&self) -> bool {
        self.body.is_create()
    }

    fn value(&self) -> U256 {
        self.body.value()
    }

    fn input(&self) -> &Bytes {
        self.body.input()
    }

    fn access_list(&self) -> Option<&AccessList> {
        self.body.access_list()
    }

    fn blob_versioned_hashes(&self) -> Option<&[B256]> {
        self.body.blob_versioned_hashes()
    }

    fn authorization_list(&self) -> Option<&[SignedAuthorization]> {
        self.body.authorization_list()
    }
}

impl Decodable2718 for TxZkSequencer {
    fn typed_decode(ty: u8, buf: &mut &[u8]) -> Eip2718Result<Self> {
        if !<Self as IsTyped2718>::is_type(ty) {
            return Err(Eip2718Error::UnexpectedType(ty));
        }
        let tx = Self::decode(buf)?;
        Ok(tx)
    }

    fn fallback_decode(data: &mut &[u8]) -> Eip2718Result<Self> {
        let tx = Self::decode(data)?;
        Ok(tx)
    }
}

impl Encodable for TxZkSequencer {
    fn encode(&self, out: &mut dyn BufMut) {
        self.rlp_encode(out);
    }

    fn length(&self) -> usize {
        self.rlp_encoded_length()
    }
}

impl Decodable for TxZkSequencer {
    fn decode(data: &mut &[u8]) -> alloy_rlp::Result<Self> {
        Self::rlp_decode(data)
    }
}

impl Sealable for TxZkSequencer {
    fn hash_slow(&self) -> B256 {
        self.tx_hash()
    }
}

#[cfg(feature = "alloy-compat")]
impl From<TxZkSequencer> for alloy_rpc_types_eth::TransactionRequest {
    fn from(tx: TxZkSequencer) -> Self {
        let mut request: alloy_rpc_types_eth::TransactionRequest = match tx.body {
            ZkSequencerTxBody::Legacy(inner) => inner.into(),
            ZkSequencerTxBody::Eip2930(inner) => inner.into(),
            ZkSequencerTxBody::Eip1559(inner) => inner.into(),
            ZkSequencerTxBody::Eip7702(inner) => inner.into(),
        };
        request.from = Some(tx.sender);
        request
    }
}

/// Bincode-compatible serde implementations for zk sequencer transactions.
#[cfg(all(feature = "serde", feature = "serde-bincode-compat"))]
pub(super) mod serde_bincode_compat {
    use alloy_primitives::Address;
    use serde::{Deserialize, Deserializer, Serialize, Serializer};
    use serde_with::{DeserializeAs, SerializeAs, serde_as};

    /// Bincode-compatible representation of a [`super::TxZkSequencer`].
    #[serde_as]
    #[derive(Debug, Serialize, Deserialize)]
    pub struct TxZkSequencer {
        sender: Address,
        #[serde_as(as = "super::super::zk_body::serde_bincode_compat::ZkSequencerTxBody<'_>")]
        body: super::super::ZkSequencerTxBody,
    }

    impl From<&super::TxZkSequencer> for TxZkSequencer {
        fn from(value: &super::TxZkSequencer) -> Self {
            Self { sender: value.sender, body: value.body.clone() }
        }
    }

    impl From<TxZkSequencer> for super::TxZkSequencer {
        fn from(value: TxZkSequencer) -> Self {
            Self { sender: value.sender, body: value.body }
        }
    }

    impl SerializeAs<super::TxZkSequencer> for TxZkSequencer {
        fn serialize_as<S>(source: &super::TxZkSequencer, serializer: S) -> Result<S::Ok, S::Error>
        where
            S: Serializer,
        {
            let borrowed = TxZkSequencer::from(source);
            borrowed.serialize(serializer)
        }
    }

    impl<'de> DeserializeAs<'de, super::TxZkSequencer> for TxZkSequencer {
        fn deserialize_as<D>(deserializer: D) -> Result<super::TxZkSequencer, D::Error>
        where
            D: Deserializer<'de>,
        {
            let borrowed = TxZkSequencer::deserialize(deserializer)?;
            Ok(borrowed.into())
        }
    }
}
