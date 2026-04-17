use std::{
    collections::{HashMap, HashSet},
    fmt, fs,
    path::{Path, PathBuf},
    sync::Arc,
};

use alloy_primitives::{Address, B256, FixedBytes, U256, hex, keccak256};
use arc_swap::ArcSwap;
use revm::primitives::StorageKey;
use serde::{Deserialize, Serialize};

/// Custom serialization for selector-keyed maps, using `"*"` for wildcards.
mod selector_map_serde {
    use std::fmt;

    use serde::{
        de::{self, MapAccess, Visitor},
        ser::SerializeMap,
    };

    use super::{DowsePrefetchItem, DowseSelector, FixedBytes, HashMap, hex};

    pub(super) fn serialize<S>(
        map: &HashMap<DowseSelector, Vec<DowsePrefetchItem>>,
        serializer: S,
    ) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut keys = map.keys().collect::<Vec<_>>();
        keys.sort_by(|left, right| match (left, right) {
            (None, None) => std::cmp::Ordering::Equal,
            (None, Some(_)) => std::cmp::Ordering::Less,
            (Some(_), None) => std::cmp::Ordering::Greater,
            (Some(left), Some(right)) => left.as_slice().cmp(right.as_slice()),
        });

        let mut map_serializer = serializer.serialize_map(Some(map.len()))?;
        for key in keys {
            let key_string = key
                .as_ref()
                .map_or_else(|| "*".to_string(), |selector| format!("0x{}", hex::encode(selector)));
            map_serializer.serialize_entry(&key_string, &map[key])?;
        }
        map_serializer.end()
    }

    pub(super) fn deserialize<'de, D>(
        deserializer: D,
    ) -> Result<HashMap<DowseSelector, Vec<DowsePrefetchItem>>, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct DowseSelectorMapVisitor;

        impl<'de> Visitor<'de> for DowseSelectorMapVisitor {
            type Value = HashMap<DowseSelector, Vec<DowsePrefetchItem>>;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("a map with selector keys")
            }

            fn visit_map<M>(self, mut access: M) -> Result<Self::Value, M::Error>
            where
                M: MapAccess<'de>,
            {
                let mut map = HashMap::with_capacity(access.size_hint().unwrap_or(0));
                while let Some((key, value)) =
                    access.next_entry::<String, Vec<DowsePrefetchItem>>()?
                {
                    let selector = if key == "*" {
                        None
                    } else {
                        let hex_string = key.strip_prefix("0x").unwrap_or(&key);
                        let bytes = hex::decode(hex_string).map_err(de::Error::custom)?;
                        if bytes.len() != 4 {
                            return Err(de::Error::custom("selector must be 4 bytes"));
                        }
                        Some(FixedBytes::<4>::from_slice(&bytes))
                    };
                    map.insert(selector, value);
                }
                Ok(map)
            }
        }

        deserializer.deserialize_map(DowseSelectorMapVisitor)
    }
}

/// Custom serialization for the outer code-hash keyed entries map.
mod entries_serde {
    use std::fmt;

    use serde::{
        Deserialize, Serialize,
        de::{self, MapAccess, Visitor},
        ser::SerializeMap,
    };

    use super::{B256, DowseSelectorMap, HashMap, hex, selector_map_serde};

    pub(super) fn serialize<S>(
        entries: &HashMap<B256, DowseSelectorMap>,
        serializer: S,
    ) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        #[derive(Serialize)]
        struct DowseSelectorMapWrapper<'a>(
            #[serde(serialize_with = "selector_map_serde::serialize")] pub &'a DowseSelectorMap,
        );

        let mut hashes = entries.keys().collect::<Vec<_>>();
        hashes.sort();

        let mut map_serializer = serializer.serialize_map(Some(entries.len()))?;
        for hash in hashes {
            let key_string = format!("0x{}", hex::encode(hash));
            map_serializer
                .serialize_entry(&key_string, &DowseSelectorMapWrapper(&entries[hash]))?;
        }
        map_serializer.end()
    }

    pub(super) fn deserialize<'de, D>(
        deserializer: D,
    ) -> Result<HashMap<B256, DowseSelectorMap>, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct DowseEntriesVisitor;

        impl<'de> Visitor<'de> for DowseEntriesVisitor {
            type Value = HashMap<B256, DowseSelectorMap>;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("a map of code hash to selector map")
            }

            fn visit_map<M>(self, mut access: M) -> Result<Self::Value, M::Error>
            where
                M: MapAccess<'de>,
            {
                #[derive(Deserialize)]
                struct DowseSelectorMapWrapper(
                    #[serde(deserialize_with = "selector_map_serde::deserialize")]
                    pub  DowseSelectorMap,
                );

                let mut map = HashMap::with_capacity(access.size_hint().unwrap_or(0));
                while let Some((key, wrapper)) =
                    access.next_entry::<String, DowseSelectorMapWrapper>()?
                {
                    let hex_string = key.strip_prefix("0x").unwrap_or(&key);
                    let bytes = hex::decode(hex_string).map_err(de::Error::custom)?;
                    if bytes.len() != 32 {
                        return Err(de::Error::custom("code hash must be 32 bytes"));
                    }
                    map.insert(B256::from_slice(&bytes), wrapper.0);
                }
                Ok(map)
            }
        }

        deserializer.deserialize_map(DowseEntriesVisitor)
    }
}

/// Selector key used by dowse: specific selector or wildcard.
pub type DowseSelector = Option<FixedBytes<4>>;

/// Per-selector entry map in a dowse-compatible hint table.
pub type DowseSelectorMap = HashMap<DowseSelector, Vec<DowsePrefetchItem>>;

/// Metadata attached to a dowse-compatible hint table.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct DowseHintTableMetadata {
    /// Human-readable description of the hint source.
    #[serde(default)]
    pub description: String,
    /// Source label, such as `bytecode-analysis` or `trace-inference`.
    #[serde(default)]
    pub source: String,
    /// Optional contract name.
    #[serde(default)]
    pub contract_name: Option<String>,
}

/// Dowse-compatible slot expression.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum DowseSlotExpression {
    /// Fixed slot value.
    Concrete {
        /// Fixed 32-byte word value.
        value: B256,
    },
    /// 32-byte calldata word at the given byte offset.
    CalldataWord {
        /// Byte offset into the full calldata, including the 4-byte selector prefix.
        offset: usize,
    },
    /// Caller address, left-padded to 32 bytes.
    Caller,
    /// `keccak256(concat(inputs))`.
    Keccak256 {
        /// Ordered 32-byte inputs to hash together.
        inputs: Vec<Self>,
    },
    /// Arithmetic addition of two slot expressions.
    Add {
        /// Left operand.
        left: Box<Self>,
        /// Right operand.
        right: Box<Self>,
    },
    /// Dependent storage load, not resolvable in this runtime.
    SLoad {
        /// Storage key expression to read before continuing resolution.
        key: Box<Self>,
    },
}

impl DowseSlotExpression {
    /// Resolves the expression into a `U256` word using the provided call context.
    pub fn resolve_word(&self, context: &DowsePrefetchContext<'_>) -> Option<U256> {
        match self {
            Self::Concrete { value } => Some((*value).into()),
            Self::CalldataWord { offset } => {
                let start = *offset;
                let end = start.saturating_add(32);
                if context.calldata.len() < end {
                    return None;
                }
                Some(B256::from_slice(&context.calldata[start..end]).into())
            }
            Self::Caller => {
                let mut word = [0_u8; 32];
                word[12..32].copy_from_slice(context.caller.as_slice());
                Some(B256::from(word).into())
            }
            Self::Keccak256 { inputs } => {
                let mut preimage = Vec::with_capacity(inputs.len().saturating_mul(32));
                for input in inputs {
                    let value = input.resolve_word(context)?;
                    preimage.extend_from_slice(B256::from(value).as_slice());
                }
                Some(keccak256(preimage).into())
            }
            Self::Add { left, right } => {
                Some(left.resolve_word(context)?.wrapping_add(right.resolve_word(context)?))
            }
            Self::SLoad { .. } => None,
        }
    }

    /// Resolves the expression into a concrete storage key.
    pub fn resolve_storage_key(&self, context: &DowsePrefetchContext<'_>) -> Option<StorageKey> {
        let value = self.resolve_word(context)?;
        Some(StorageKey::from_be_bytes(B256::from(value).0))
    }

    /// Resolves the expression into an address by taking the low 20 bytes.
    pub fn resolve_address(&self, context: &DowsePrefetchContext<'_>) -> Option<Address> {
        let value = self.resolve_word(context)?;
        Some(Address::from_word(B256::from(value)))
    }
}

/// Dowse-compatible prefetch item.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind")]
pub enum DowsePrefetchItem {
    /// Prefetch account metadata for a fixed address, optionally chaining into a selector.
    Account {
        /// Account to prefetch.
        address: Address,
        /// Optional selector to use when resolving child storage hints for the prefetched account.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        selector: Option<FixedBytes<4>>,
    },
    /// Prefetch a storage slot on the current contract.
    Storage {
        /// Slot expression to resolve against the current call context.
        slot: DowseSlotExpression,
    },
    /// Prefetch account metadata for a computed address, optionally chaining into a selector.
    ComputedAccount {
        /// Address expression to resolve against the current call context.
        address: DowseSlotExpression,
        /// Optional selector to use when resolving child storage hints for the prefetched account.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        selector: Option<FixedBytes<4>>,
    },
}

/// Runtime call context used to resolve dowse-style slot expressions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DowsePrefetchContext<'a> {
    /// Address currently being called.
    pub target: Address,
    /// Full calldata including the 4-byte selector.
    pub calldata: &'a [u8],
    /// Effective caller.
    pub caller: Address,
    /// Parsed selector, if calldata length is at least 4 bytes.
    pub selector: DowseSelector,
}

impl<'a> DowsePrefetchContext<'a> {
    /// Constructs a new context and derives the selector from calldata.
    pub fn new(target: Address, calldata: &'a [u8], caller: Address) -> Self {
        let selector = if calldata.len() >= 4 {
            Some(FixedBytes::<4>::from_slice(&calldata[..4]))
        } else {
            None
        };
        Self { target, calldata, caller, selector }
    }
}

/// Concrete resolved prefetch target derived from a dowse hint table.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DowseResolvedPrefetchTarget {
    /// Prefetch account metadata.
    Account(Address),
    /// Prefetch a storage key.
    Storage {
        /// Contract address that owns the storage slot.
        address: Address,
        /// Concrete storage key.
        slot: StorageKey,
    },
}

/// Dowse-compatible hint table stored under code hashes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DowseHintTable {
    /// Schema version. v1 matches current dowse JSON.
    pub version: u32,
    /// Human-readable metadata.
    pub metadata: DowseHintTableMetadata,
    /// Code-hash keyed selector entries.
    #[serde(with = "entries_serde")]
    pub entries: HashMap<B256, DowseSelectorMap>,
    /// Address to code-hash registry.
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub code_hashes: HashMap<Address, B256>,
}

impl DowseHintTable {
    /// Supported dowse schema version in this implementation.
    pub const SUPPORTED_VERSION: u32 = 1;

    /// Creates an empty hint table.
    pub fn new() -> Self {
        Self {
            version: Self::SUPPORTED_VERSION,
            metadata: DowseHintTableMetadata::default(),
            entries: HashMap::new(),
            code_hashes: HashMap::new(),
        }
    }

    /// Loads a dowse-compatible hint table from a JSON string.
    pub fn from_json_str(json: &str) -> Result<Self, DowseHintTableLoadError> {
        let table =
            serde_json::from_str::<Self>(json).map_err(DowseHintTableLoadError::JsonDeserialize)?;
        table.validate()?;
        Ok(table)
    }

    /// Loads a dowse-compatible hint table from a JSON file.
    pub fn from_json_path(path: impl AsRef<Path>) -> Result<Self, DowseHintTableLoadError> {
        let path = path.as_ref();
        let json = fs::read_to_string(path).map_err(|error| DowseHintTableLoadError::ReadFile {
            path: path.to_path_buf(),
            error,
        })?;
        Self::from_json_str(&json)
    }

    /// Serializes the hint table to pretty JSON.
    pub fn to_json_string(&self) -> Result<String, DowseHintTableLoadError> {
        serde_json::to_string_pretty(self).map_err(DowseHintTableLoadError::JsonSerialize)
    }

    /// Validates that the loaded table matches the supported format.
    pub const fn validate(&self) -> Result<(), DowseHintTableLoadError> {
        if self.version != Self::SUPPORTED_VERSION {
            return Err(DowseHintTableLoadError::UnsupportedVersion {
                version: self.version,
                supported_version: Self::SUPPORTED_VERSION,
            });
        }
        Ok(())
    }

    /// Registers an address to code-hash mapping.
    pub fn register_code_hash(&mut self, address: Address, code_hash: B256) {
        self.code_hashes.insert(address, code_hash);
    }

    /// Inserts selector-specific items for the given address and code hash.
    pub fn insert(
        &mut self,
        address: Address,
        code_hash: B256,
        selector: DowseSelector,
        items: Vec<DowsePrefetchItem>,
    ) {
        self.code_hashes.insert(address, code_hash);
        self.entries.entry(code_hash).or_default().insert(selector, items);
    }

    /// Looks up items for an address and selector, falling back to wildcard entries.
    pub fn lookup(
        &self,
        address: Address,
        selector: DowseSelector,
    ) -> Option<&[DowsePrefetchItem]> {
        let code_hash = self.code_hashes.get(&address)?;
        let selector_map = self.entries.get(code_hash)?;
        if let Some(selector) = selector
            && let Some(items) = selector_map.get(&Some(selector))
        {
            return Some(items);
        }
        selector_map.get(&None).map(Vec::as_slice)
    }

    /// Resolves all prefetch targets for a call context.
    pub fn resolve_targets(
        &self,
        context: &DowsePrefetchContext<'_>,
    ) -> Vec<DowseResolvedPrefetchTarget> {
        let mut targets = Vec::new();
        let mut seen = HashSet::new();

        let Some(items) = self.lookup(context.target, context.selector) else {
            return targets;
        };

        for item in items {
            match item {
                DowsePrefetchItem::Account { address, selector } => {
                    Self::push_unique_target(
                        &mut targets,
                        &mut seen,
                        DowseResolvedPrefetchTarget::Account(*address),
                    );
                    self.append_child_storage_targets(
                        &mut targets,
                        &mut seen,
                        *address,
                        *selector,
                        context,
                    );
                }
                DowsePrefetchItem::Storage { slot } => {
                    if let Some(storage_key) = slot.resolve_storage_key(context) {
                        Self::push_unique_target(
                            &mut targets,
                            &mut seen,
                            DowseResolvedPrefetchTarget::Storage {
                                address: context.target,
                                slot: storage_key,
                            },
                        );
                    }
                }
                DowsePrefetchItem::ComputedAccount { address, selector } => {
                    if let Some(resolved_address) = address.resolve_address(context) {
                        Self::push_unique_target(
                            &mut targets,
                            &mut seen,
                            DowseResolvedPrefetchTarget::Account(resolved_address),
                        );
                        self.append_child_storage_targets(
                            &mut targets,
                            &mut seen,
                            resolved_address,
                            *selector,
                            context,
                        );
                    }
                }
            }
        }

        targets
    }

    /// Resolves only concrete storage targets for a call context.
    pub fn resolve_storage_targets(
        &self,
        context: &DowsePrefetchContext<'_>,
    ) -> Vec<(Address, StorageKey)> {
        self.resolve_targets(context)
            .into_iter()
            .filter_map(|target| match target {
                DowseResolvedPrefetchTarget::Account(_) => None,
                DowseResolvedPrefetchTarget::Storage { address, slot } => Some((address, slot)),
            })
            .collect()
    }

    /// Returns the total number of selector entries in the table.
    pub fn selector_count(&self) -> usize {
        self.entries.values().map(HashMap::len).sum()
    }

    /// Returns the total number of prefetch items in the table.
    pub fn item_count(&self) -> usize {
        self.entries.values().flat_map(HashMap::values).map(Vec::len).sum()
    }

    fn append_child_storage_targets(
        &self,
        targets: &mut Vec<DowseResolvedPrefetchTarget>,
        seen: &mut HashSet<DowseResolvedPrefetchTarget>,
        address: Address,
        selector: DowseSelector,
        context: &DowsePrefetchContext<'_>,
    ) {
        let Some(selector) = selector else {
            return;
        };
        let Some(items) = self.lookup(address, Some(selector)) else {
            return;
        };
        for item in items {
            if let DowsePrefetchItem::Storage { slot } = item
                && let Some(storage_key) = slot.resolve_storage_key(context)
            {
                Self::push_unique_target(
                    targets,
                    seen,
                    DowseResolvedPrefetchTarget::Storage { address, slot: storage_key },
                );
            }
        }
    }

    fn push_unique_target(
        targets: &mut Vec<DowseResolvedPrefetchTarget>,
        seen: &mut HashSet<DowseResolvedPrefetchTarget>,
        target: DowseResolvedPrefetchTarget,
    ) {
        if seen.insert(target) {
            targets.push(target);
        }
    }
}

impl Default for DowseHintTable {
    fn default() -> Self {
        Self::new()
    }
}

/// Error returned when loading or reloading a dowse-compatible hint table.
#[derive(Debug)]
pub enum DowseHintTableLoadError {
    /// Failed to read the requested file.
    ReadFile {
        /// Source path.
        path: PathBuf,
        /// Underlying I/O error.
        error: std::io::Error,
    },
    /// Failed to deserialize JSON.
    JsonDeserialize(serde_json::Error),
    /// Failed to serialize JSON.
    JsonSerialize(serde_json::Error),
    /// Loaded version is not supported.
    UnsupportedVersion {
        /// Version in the file.
        version: u32,
        /// Version supported by this implementation.
        supported_version: u32,
    },
}

impl fmt::Display for DowseHintTableLoadError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ReadFile { path, error } => {
                write!(formatter, "failed to read hint table {}: {error}", path.display())
            }
            Self::JsonDeserialize(error) => {
                write!(formatter, "failed to deserialize hint table: {error}")
            }
            Self::JsonSerialize(error) => {
                write!(formatter, "failed to serialize hint table: {error}")
            }
            Self::UnsupportedVersion { version, supported_version } => {
                write!(
                    formatter,
                    "unsupported hint table version {version}; expected {supported_version}"
                )
            }
        }
    }
}

impl std::error::Error for DowseHintTableLoadError {}

/// Atomically swappable store for the active dowse-compatible hint table.
#[derive(Debug, Default)]
pub struct DowseHintTableStore {
    active_table: ArcSwap<DowseHintTable>,
}

impl DowseHintTableStore {
    /// Creates a store from the provided initial table.
    pub fn new(table: DowseHintTable) -> Self {
        Self { active_table: ArcSwap::new(Arc::new(table)) }
    }

    /// Creates a store with an empty initial table.
    pub fn empty() -> Self {
        Self::new(DowseHintTable::new())
    }

    /// Creates a store by loading a JSON file.
    pub fn from_json_path(path: impl AsRef<Path>) -> Result<Self, DowseHintTableLoadError> {
        Ok(Self::new(DowseHintTable::from_json_path(path)?))
    }

    /// Returns an atomic snapshot of the currently active table.
    pub fn snapshot(&self) -> Arc<DowseHintTable> {
        self.active_table.load_full()
    }

    /// Replaces the active table with a validated in-memory table.
    pub fn replace(&self, table: DowseHintTable) -> Arc<DowseHintTable> {
        self.active_table.swap(Arc::new(table))
    }

    /// Loads a new table from disk and atomically swaps it in on success.
    ///
    /// If loading fails, the active table is left unchanged and the error is returned.
    pub fn reload_json_path(
        &self,
        path: impl AsRef<Path>,
    ) -> Result<Arc<DowseHintTable>, DowseHintTableLoadError> {
        let table = DowseHintTable::from_json_path(path)?;
        Ok(self.replace(table))
    }

    /// Resolves targets against the currently active table snapshot.
    pub fn resolve_targets(
        &self,
        context: &DowsePrefetchContext<'_>,
    ) -> Vec<DowseResolvedPrefetchTarget> {
        self.snapshot().resolve_targets(context)
    }

    /// Resolves storage targets against the currently active table snapshot.
    pub fn resolve_storage_targets(
        &self,
        context: &DowsePrefetchContext<'_>,
    ) -> Vec<(Address, StorageKey)> {
        self.snapshot().resolve_storage_targets(context)
    }
}

#[cfg(test)]
mod tests {
    use std::fs;

    use alloy_primitives::{B256, address};
    use revm::primitives::StorageKey;
    use tempfile::tempdir;

    use crate::{Erc20Context, Erc20StorageLayout, PrefetchHintBuilder, TxShape};

    use super::{
        DowseHintTable, DowseHintTableMetadata, DowseHintTableStore, DowsePrefetchContext,
        DowsePrefetchItem, DowseSlotExpression, FixedBytes,
    };

    #[test]
    fn json_roundtrip_preserves_wildcard_and_selector_entries() {
        let address = address!("4200000000000000000000000000000000000006");
        let token_code_hash = alloy_primitives::keccak256(b"token");
        let mut table = DowseHintTable::new();
        table.metadata = DowseHintTableMetadata {
            description: "test".to_string(),
            source: "manual".to_string(),
            contract_name: Some("Token".to_string()),
        };
        table.insert(
            address,
            token_code_hash,
            None,
            vec![DowsePrefetchItem::Storage {
                slot: DowseSlotExpression::Concrete { value: B256::with_last_byte(9) },
            }],
        );
        table.insert(
            address,
            token_code_hash,
            Some(FixedBytes::from([0xa9, 0x05, 0x9c, 0xbb])),
            vec![DowsePrefetchItem::Storage {
                slot: DowseSlotExpression::CalldataWord { offset: 4 },
            }],
        );

        let encoded = table.to_json_string().expect("serialize");
        let decoded = DowseHintTable::from_json_str(&encoded).expect("deserialize");

        assert_eq!(decoded.version, DowseHintTable::SUPPORTED_VERSION);
        assert_eq!(decoded.metadata.contract_name.as_deref(), Some("Token"));
        assert_eq!(decoded.selector_count(), 2);
        assert_eq!(decoded.item_count(), 2);
        assert!(decoded.lookup(address, None).is_some());
        assert!(
            decoded.lookup(address, Some(FixedBytes::from([0xa9, 0x05, 0x9c, 0xbb]))).is_some()
        );
    }

    #[test]
    fn resolves_transfer_from_storage_targets_compatibly_with_existing_builder() {
        let token = address!("4200000000000000000000000000000000000006");
        let from = address!("0000000000000000000000000000000000001337");
        let to = address!("0000000000000000000000000000000000001338");
        let spender = address!("0000000000000000000000000000000000001339");
        let selector = FixedBytes::from([0x23, 0xb8, 0x72, 0xdd]);
        let layout =
            Erc20StorageLayout { paused_slot: Some(StorageKey::from(9_u64)), ..Default::default() };
        let expected = PrefetchHintBuilder::erc20_standard(
            &Erc20Context { token, from, to, spender, tx_shape: TxShape::TransferFrom, layout },
            &[],
        );

        let mut table = DowseHintTable::new();
        table.insert(
            token,
            alloy_primitives::keccak256(b"erc20"),
            Some(selector),
            vec![
                DowsePrefetchItem::Storage {
                    slot: DowseSlotExpression::Concrete { value: B256::with_last_byte(9) },
                },
                DowsePrefetchItem::Storage {
                    slot: DowseSlotExpression::Keccak256 {
                        inputs: vec![
                            DowseSlotExpression::Caller,
                            DowseSlotExpression::Keccak256 {
                                inputs: vec![
                                    DowseSlotExpression::CalldataWord { offset: 4 },
                                    DowseSlotExpression::Concrete {
                                        value: B256::with_last_byte(1),
                                    },
                                ],
                            },
                        ],
                    },
                },
                DowsePrefetchItem::Storage {
                    slot: DowseSlotExpression::Keccak256 {
                        inputs: vec![
                            DowseSlotExpression::CalldataWord { offset: 4 },
                            DowseSlotExpression::Concrete { value: B256::ZERO },
                        ],
                    },
                },
                DowsePrefetchItem::Storage {
                    slot: DowseSlotExpression::Keccak256 {
                        inputs: vec![
                            DowseSlotExpression::CalldataWord { offset: 36 },
                            DowseSlotExpression::Concrete { value: B256::ZERO },
                        ],
                    },
                },
            ],
        );

        let mut calldata = Vec::with_capacity(100);
        calldata.extend_from_slice(selector.as_slice());
        let mut from_word = [0_u8; 32];
        from_word[12..32].copy_from_slice(from.as_slice());
        calldata.extend_from_slice(&from_word);
        let mut to_word = [0_u8; 32];
        to_word[12..32].copy_from_slice(to.as_slice());
        calldata.extend_from_slice(&to_word);
        calldata.extend_from_slice(&[0_u8; 32]);

        let resolved =
            table.resolve_storage_targets(&DowsePrefetchContext::new(token, &calldata, spender));

        assert_eq!(resolved, expected);
    }

    #[test]
    fn chained_account_selector_adds_child_storage_hints() {
        let router = address!("00000000000000000000000000000000000000a1");
        let token = address!("00000000000000000000000000000000000000b2");
        let trader = address!("00000000000000000000000000000000000000c3");
        let router_code_hash = alloy_primitives::keccak256(b"router");
        let token_code_hash = alloy_primitives::keccak256(b"token");
        let router_selector = FixedBytes::from([0x12, 0x34, 0x56, 0x78]);
        let child_selector = FixedBytes::from([0xa9, 0x05, 0x9c, 0xbb]);

        let mut table = DowseHintTable::new();
        table.insert(
            router,
            router_code_hash,
            Some(router_selector),
            vec![DowsePrefetchItem::Account { address: token, selector: Some(child_selector) }],
        );
        table.insert(
            token,
            token_code_hash,
            Some(child_selector),
            vec![DowsePrefetchItem::Storage {
                slot: DowseSlotExpression::Keccak256 {
                    inputs: vec![
                        DowseSlotExpression::Caller,
                        DowseSlotExpression::Concrete { value: B256::ZERO },
                    ],
                },
            }],
        );

        let calldata = router_selector.as_slice().to_vec();
        let targets = table.resolve_targets(&DowsePrefetchContext::new(router, &calldata, trader));

        assert_eq!(targets.len(), 2);
        assert_eq!(targets[0], super::DowseResolvedPrefetchTarget::Account(token));
        assert_eq!(
            targets[1],
            super::DowseResolvedPrefetchTarget::Storage {
                address: token,
                slot: PrefetchHintBuilder::erc20_balance_slot(trader, StorageKey::ZERO),
            }
        );
    }

    #[test]
    fn reload_keeps_previous_table_when_new_file_is_invalid() {
        let directory = tempdir().expect("tempdir");
        let path = directory.path().join("hints.json");

        let initial_json = r#"
        {
          "version": 1,
          "metadata": { "source": "manual", "description": "initial" },
          "entries": {},
          "code_hashes": {}
        }
        "#;
        fs::write(&path, initial_json).expect("write initial");

        let store = DowseHintTableStore::from_json_path(&path).expect("load store");
        let initial_snapshot = store.snapshot();
        assert_eq!(initial_snapshot.metadata.description, "initial");

        fs::write(&path, "{ not valid json").expect("write invalid");
        let error = store.reload_json_path(&path).expect_err("reload should fail");
        assert!(matches!(error, super::DowseHintTableLoadError::JsonDeserialize(_)));

        let reloaded_snapshot = store.snapshot();
        assert_eq!(reloaded_snapshot.metadata.description, "initial");
    }

    #[test]
    fn reload_swaps_active_table_after_successful_load() {
        let directory = tempdir().expect("tempdir");
        let path = directory.path().join("hints.json");

        fs::write(
            &path,
            r#"
            {
              "version": 1,
              "metadata": { "source": "manual", "description": "old" },
              "entries": {},
              "code_hashes": {}
            }
            "#,
        )
        .expect("write initial");

        let store = DowseHintTableStore::from_json_path(&path).expect("load store");
        fs::write(
            &path,
            r#"
            {
              "version": 1,
              "metadata": { "source": "manual", "description": "new" },
              "entries": {},
              "code_hashes": {}
            }
            "#,
        )
        .expect("write replacement");

        let previous = store.reload_json_path(&path).expect("reload");
        assert_eq!(previous.metadata.description, "old");
        assert_eq!(store.snapshot().metadata.description, "new");
    }
}
