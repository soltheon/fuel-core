use crate::storage;
use fuel_core_storage::{
    iter::{
        changes_iterator::ChangesIterator,
        IterDirection,
        IterableStore,
    },
    kv_store::KeyValueInspect,
    merkle::column::MerkleizedColumn,
    transactional::{
        Modifiable,
        StorageChanges,
        StorageTransaction,
    },
    StorageAsMut,
};

/// Compressed block type alias
pub type CompressedBlock = fuel_core_compression::VersionedCompressedBlock;

/// Trait for interacting with storage that supports compression
pub trait CompressionStorage:
    KeyValueInspect<Column = MerkleizedColumn<storage::column::CompressionColumn>>
    + Modifiable
{
}

impl<T> CompressionStorage for T where
    T: KeyValueInspect<Column = MerkleizedColumn<storage::column::CompressionColumn>>
        + Modifiable
{
}

pub(crate) trait WriteCompressedBlock {
    fn write_compressed_block(
        &mut self,
        height: &u32,
        compressed_block: &crate::ports::compression_storage::CompressedBlock,
    ) -> crate::Result<()>;
    fn latest_compressed_block_size(&self) -> crate::Result<Option<usize>>;
}

impl<Storage> WriteCompressedBlock for StorageTransaction<Storage>
where
    Storage:
        KeyValueInspect<Column = MerkleizedColumn<storage::column::CompressionColumn>>,
{
    fn write_compressed_block(
        &mut self,
        height: &u32,
        compressed_block: &crate::ports::compression_storage::CompressedBlock,
    ) -> crate::Result<()> {
        self.storage_as_mut::<storage::CompressedBlocks>()
            .insert(&(*height).into(), compressed_block)
            .map_err(crate::errors::CompressionError::FailedToWriteCompressedBlock)
    }

    fn latest_compressed_block_size(&self) -> crate::Result<Option<usize>> {
        let changes = StorageChanges::Changes(self.changes().clone());
        let view = ChangesIterator::new(&changes);
        let raw_kv = view
            .iter_store(
                MerkleizedColumn::<storage::column::CompressionColumn>::TableColumn(
                    storage::column::CompressionColumn::CompressedBlocks,
                ),
                None,
                None,
                IterDirection::Reverse,
            )
            .next()
            .transpose()
            .map_err(crate::errors::CompressionError::FailedToGetCompressedBlockSize)?;

        match raw_kv {
            Some((_, value)) => Ok(Some(value.len())),
            _ => Ok(None),
        }
    }
}
