//! ZK-backed sequencer transaction bodies.

use alloy_consensus::{Transaction, TxEip1559, TxEip2930, TxEip7702, TxLegacy, TxType};
use alloy_eips::{
    Typed2718, eip2718::IsTyped2718, eip2930::AccessList, eip7702::SignedAuthorization,
};
use alloy_primitives::{B256, Bytes, ChainId, TxKind, U256};
use alloy_rlp::{BufMut, Decodable, Encodable, Header};

/// Unsigned transaction body carried by a [`crate::TxZkSequencer`].
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(rename_all = "camelCase"))]
pub enum ZkSequencerTxBody {
    /// Legacy transaction body.
    Legacy(TxLegacy),
    /// EIP-2930 transaction body.
    Eip2930(TxEip2930),
    /// EIP-1559 transaction body.
    Eip1559(TxEip1559),
    /// EIP-7702 transaction body.
    Eip7702(TxEip7702),
}

impl ZkSequencerTxBody {
    /// Returns the inner Ethereum transaction type.
    pub const fn inner_tx_type(&self) -> TxType {
        match self {
            Self::Legacy(_) => TxType::Legacy,
            Self::Eip2930(_) => TxType::Eip2930,
            Self::Eip1559(_) => TxType::Eip1559,
            Self::Eip7702(_) => TxType::Eip7702,
        }
    }

    /// Returns the encoded length of the body fields embedded inside a zk-backed sequencer tx.
    pub fn rlp_encoded_fields_length(&self) -> usize {
        (self.inner_tx_type() as u8).length()
            + match self {
                Self::Legacy(tx) => tx.length(),
                Self::Eip2930(tx) => tx.length(),
                Self::Eip1559(tx) => tx.length(),
                Self::Eip7702(tx) => tx.length(),
            }
    }

    /// Encodes the body fields embedded inside a zk-backed sequencer tx.
    pub fn rlp_encode_fields(&self, out: &mut dyn BufMut) {
        (self.inner_tx_type() as u8).encode(out);
        match self {
            Self::Legacy(tx) => tx.encode(out),
            Self::Eip2930(tx) => tx.encode(out),
            Self::Eip1559(tx) => tx.encode(out),
            Self::Eip7702(tx) => tx.encode(out),
        }
    }

    /// Decodes the body fields embedded inside a zk-backed sequencer tx.
    pub fn rlp_decode_fields(buf: &mut &[u8]) -> alloy_rlp::Result<Self> {
        let inner_type = TxType::try_from(u8::decode(buf)?)
            .map_err(|_| alloy_rlp::Error::Custom("invalid zk sequencer inner tx type"))?;
        match inner_type {
            TxType::Legacy => Ok(Self::Legacy(TxLegacy::decode(buf)?)),
            TxType::Eip2930 => Ok(Self::Eip2930(TxEip2930::decode(buf)?)),
            TxType::Eip1559 => Ok(Self::Eip1559(TxEip1559::decode(buf)?)),
            TxType::Eip7702 => Ok(Self::Eip7702(TxEip7702::decode(buf)?)),
            _ => Err(alloy_rlp::Error::Custom("unsupported zk sequencer inner tx type")),
        }
    }

    /// Returns mutable access to the input bytes.
    pub fn input_mut(&mut self) -> &mut Bytes {
        match self {
            Self::Legacy(tx) => &mut tx.input,
            Self::Eip2930(tx) => &mut tx.input,
            Self::Eip1559(tx) => &mut tx.input,
            Self::Eip7702(tx) => &mut tx.input,
        }
    }

    /// Returns a heuristic in-memory size for the body.
    pub fn size(&self) -> usize {
        match self {
            Self::Legacy(tx) => tx.size(),
            Self::Eip2930(tx) => tx.size(),
            Self::Eip1559(tx) => tx.size(),
            Self::Eip7702(tx) => tx.size(),
        }
    }
}

impl Default for ZkSequencerTxBody {
    fn default() -> Self {
        Self::Legacy(TxLegacy::default())
    }
}

impl From<TxLegacy> for ZkSequencerTxBody {
    fn from(value: TxLegacy) -> Self {
        Self::Legacy(value)
    }
}

impl From<TxEip2930> for ZkSequencerTxBody {
    fn from(value: TxEip2930) -> Self {
        Self::Eip2930(value)
    }
}

impl From<TxEip1559> for ZkSequencerTxBody {
    fn from(value: TxEip1559) -> Self {
        Self::Eip1559(value)
    }
}

impl From<TxEip7702> for ZkSequencerTxBody {
    fn from(value: TxEip7702) -> Self {
        Self::Eip7702(value)
    }
}

impl Transaction for ZkSequencerTxBody {
    fn chain_id(&self) -> Option<ChainId> {
        match self {
            Self::Legacy(tx) => tx.chain_id(),
            Self::Eip2930(tx) => tx.chain_id(),
            Self::Eip1559(tx) => tx.chain_id(),
            Self::Eip7702(tx) => tx.chain_id(),
        }
    }

    fn nonce(&self) -> u64 {
        match self {
            Self::Legacy(tx) => tx.nonce(),
            Self::Eip2930(tx) => tx.nonce(),
            Self::Eip1559(tx) => tx.nonce(),
            Self::Eip7702(tx) => tx.nonce(),
        }
    }

    fn gas_limit(&self) -> u64 {
        match self {
            Self::Legacy(tx) => tx.gas_limit(),
            Self::Eip2930(tx) => tx.gas_limit(),
            Self::Eip1559(tx) => tx.gas_limit(),
            Self::Eip7702(tx) => tx.gas_limit(),
        }
    }

    fn gas_price(&self) -> Option<u128> {
        match self {
            Self::Legacy(tx) => tx.gas_price(),
            Self::Eip2930(tx) => tx.gas_price(),
            Self::Eip1559(tx) => tx.gas_price(),
            Self::Eip7702(tx) => tx.gas_price(),
        }
    }

    fn max_fee_per_gas(&self) -> u128 {
        match self {
            Self::Legacy(tx) => tx.max_fee_per_gas(),
            Self::Eip2930(tx) => tx.max_fee_per_gas(),
            Self::Eip1559(tx) => tx.max_fee_per_gas(),
            Self::Eip7702(tx) => tx.max_fee_per_gas(),
        }
    }

    fn max_priority_fee_per_gas(&self) -> Option<u128> {
        match self {
            Self::Legacy(tx) => tx.max_priority_fee_per_gas(),
            Self::Eip2930(tx) => tx.max_priority_fee_per_gas(),
            Self::Eip1559(tx) => tx.max_priority_fee_per_gas(),
            Self::Eip7702(tx) => tx.max_priority_fee_per_gas(),
        }
    }

    fn max_fee_per_blob_gas(&self) -> Option<u128> {
        match self {
            Self::Legacy(tx) => tx.max_fee_per_blob_gas(),
            Self::Eip2930(tx) => tx.max_fee_per_blob_gas(),
            Self::Eip1559(tx) => tx.max_fee_per_blob_gas(),
            Self::Eip7702(tx) => tx.max_fee_per_blob_gas(),
        }
    }

    fn priority_fee_or_price(&self) -> u128 {
        match self {
            Self::Legacy(tx) => tx.priority_fee_or_price(),
            Self::Eip2930(tx) => tx.priority_fee_or_price(),
            Self::Eip1559(tx) => tx.priority_fee_or_price(),
            Self::Eip7702(tx) => tx.priority_fee_or_price(),
        }
    }

    fn effective_gas_price(&self, base_fee: Option<u64>) -> u128 {
        match self {
            Self::Legacy(tx) => tx.effective_gas_price(base_fee),
            Self::Eip2930(tx) => tx.effective_gas_price(base_fee),
            Self::Eip1559(tx) => tx.effective_gas_price(base_fee),
            Self::Eip7702(tx) => tx.effective_gas_price(base_fee),
        }
    }

    fn is_dynamic_fee(&self) -> bool {
        match self {
            Self::Legacy(tx) => tx.is_dynamic_fee(),
            Self::Eip2930(tx) => tx.is_dynamic_fee(),
            Self::Eip1559(tx) => tx.is_dynamic_fee(),
            Self::Eip7702(tx) => tx.is_dynamic_fee(),
        }
    }

    fn kind(&self) -> TxKind {
        match self {
            Self::Legacy(tx) => tx.kind(),
            Self::Eip2930(tx) => tx.kind(),
            Self::Eip1559(tx) => tx.kind(),
            Self::Eip7702(tx) => tx.kind(),
        }
    }

    fn is_create(&self) -> bool {
        match self {
            Self::Legacy(tx) => tx.is_create(),
            Self::Eip2930(tx) => tx.is_create(),
            Self::Eip1559(tx) => tx.is_create(),
            Self::Eip7702(tx) => tx.is_create(),
        }
    }

    fn value(&self) -> U256 {
        match self {
            Self::Legacy(tx) => tx.value(),
            Self::Eip2930(tx) => tx.value(),
            Self::Eip1559(tx) => tx.value(),
            Self::Eip7702(tx) => tx.value(),
        }
    }

    fn input(&self) -> &Bytes {
        match self {
            Self::Legacy(tx) => tx.input(),
            Self::Eip2930(tx) => tx.input(),
            Self::Eip1559(tx) => tx.input(),
            Self::Eip7702(tx) => tx.input(),
        }
    }

    fn access_list(&self) -> Option<&AccessList> {
        match self {
            Self::Legacy(tx) => tx.access_list(),
            Self::Eip2930(tx) => tx.access_list(),
            Self::Eip1559(tx) => tx.access_list(),
            Self::Eip7702(tx) => tx.access_list(),
        }
    }

    fn blob_versioned_hashes(&self) -> Option<&[B256]> {
        match self {
            Self::Legacy(tx) => tx.blob_versioned_hashes(),
            Self::Eip2930(tx) => tx.blob_versioned_hashes(),
            Self::Eip1559(tx) => tx.blob_versioned_hashes(),
            Self::Eip7702(tx) => tx.blob_versioned_hashes(),
        }
    }

    fn authorization_list(&self) -> Option<&[SignedAuthorization]> {
        match self {
            Self::Legacy(tx) => tx.authorization_list(),
            Self::Eip2930(tx) => tx.authorization_list(),
            Self::Eip1559(tx) => tx.authorization_list(),
            Self::Eip7702(tx) => tx.authorization_list(),
        }
    }
}

impl Typed2718 for ZkSequencerTxBody {
    fn ty(&self) -> u8 {
        self.inner_tx_type() as u8
    }
}

impl IsTyped2718 for ZkSequencerTxBody {
    fn is_type(type_id: u8) -> bool {
        matches!(
            TxType::try_from(type_id),
            Ok(TxType::Legacy | TxType::Eip2930 | TxType::Eip1559 | TxType::Eip7702)
        )
    }
}

impl Encodable for ZkSequencerTxBody {
    fn encode(&self, out: &mut dyn BufMut) {
        Header { list: true, payload_length: self.rlp_encoded_fields_length() }.encode(out);
        self.rlp_encode_fields(out);
    }

    fn length(&self) -> usize {
        let payload_length = self.rlp_encoded_fields_length();
        Header { list: true, payload_length }.length_with_payload()
    }
}

impl Decodable for ZkSequencerTxBody {
    fn decode(buf: &mut &[u8]) -> alloy_rlp::Result<Self> {
        let header = Header::decode(buf)?;
        if !header.list {
            return Err(alloy_rlp::Error::UnexpectedString);
        }

        let remaining = buf.len();
        if header.payload_length > remaining {
            return Err(alloy_rlp::Error::InputTooShort);
        }

        let body = Self::rlp_decode_fields(buf)?;
        if buf.len() + header.payload_length != remaining {
            return Err(alloy_rlp::Error::UnexpectedLength);
        }

        Ok(body)
    }
}

/// Bincode-compatible serde implementations for zk sequencer transaction bodies.
#[cfg(all(feature = "serde", feature = "serde-bincode-compat"))]
pub(super) mod serde_bincode_compat {
    use alloy_consensus::transaction::serde_bincode_compat::{
        TxEip1559, TxEip2930, TxEip7702, TxLegacy,
    };
    use serde::{Deserialize, Deserializer, Serialize, Serializer};
    use serde_with::{DeserializeAs, SerializeAs};

    /// Bincode-compatible representation of a [`super::ZkSequencerTxBody`].
    #[derive(Debug, Serialize, Deserialize)]
    pub enum ZkSequencerTxBody<'a> {
        /// Legacy variant.
        Legacy(TxLegacy<'a>),
        /// EIP-2930 variant.
        Eip2930(TxEip2930<'a>),
        /// EIP-1559 variant.
        Eip1559(TxEip1559<'a>),
        /// EIP-7702 variant.
        Eip7702(TxEip7702<'a>),
    }

    impl<'a> From<&'a super::ZkSequencerTxBody> for ZkSequencerTxBody<'a> {
        fn from(value: &'a super::ZkSequencerTxBody) -> Self {
            match value {
                super::ZkSequencerTxBody::Legacy(tx) => Self::Legacy(tx.into()),
                super::ZkSequencerTxBody::Eip2930(tx) => Self::Eip2930(tx.into()),
                super::ZkSequencerTxBody::Eip1559(tx) => Self::Eip1559(tx.into()),
                super::ZkSequencerTxBody::Eip7702(tx) => Self::Eip7702(tx.into()),
            }
        }
    }

    impl<'a> From<ZkSequencerTxBody<'a>> for super::ZkSequencerTxBody {
        fn from(value: ZkSequencerTxBody<'a>) -> Self {
            match value {
                ZkSequencerTxBody::Legacy(tx) => Self::Legacy(tx.into()),
                ZkSequencerTxBody::Eip2930(tx) => Self::Eip2930(tx.into()),
                ZkSequencerTxBody::Eip1559(tx) => Self::Eip1559(tx.into()),
                ZkSequencerTxBody::Eip7702(tx) => Self::Eip7702(tx.into()),
            }
        }
    }

    impl SerializeAs<super::ZkSequencerTxBody> for ZkSequencerTxBody<'_> {
        fn serialize_as<S>(
            source: &super::ZkSequencerTxBody,
            serializer: S,
        ) -> Result<S::Ok, S::Error>
        where
            S: Serializer,
        {
            let borrowed = ZkSequencerTxBody::from(source);
            borrowed.serialize(serializer)
        }
    }

    impl<'de> DeserializeAs<'de, super::ZkSequencerTxBody> for ZkSequencerTxBody<'de> {
        fn deserialize_as<D>(deserializer: D) -> Result<super::ZkSequencerTxBody, D::Error>
        where
            D: Deserializer<'de>,
        {
            let borrowed = ZkSequencerTxBody::deserialize(deserializer)?;
            Ok(borrowed.into())
        }
    }
}
