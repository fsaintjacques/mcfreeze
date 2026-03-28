use arrow::{
    array::{Array, ArrayRef, BinaryArray, LargeBinaryArray, LargeStringArray, StringArray},
    datatypes::DataType,
    record_batch::RecordBatch,
};
use kv_loader::KvBatch;

// ---------------------------------------------------------------------------
// BinaryCol — type-erased byte-column accessor
// ---------------------------------------------------------------------------

/// Wraps an Arrow column that holds byte data (Binary, LargeBinary, Utf8,
/// LargeUtf8) and provides zero-copy access to individual values.
pub(crate) struct BinaryCol(ArrayRef);

impl BinaryCol {
    pub fn try_from(arr: ArrayRef, col: &str) -> Result<Self, crate::error::BqError> {
        match arr.data_type() {
            DataType::Binary
            | DataType::LargeBinary
            | DataType::Utf8
            | DataType::LargeUtf8 => Ok(Self(arr)),
            dt => Err(crate::error::BqError::Schema(format!(
                "column {col:?} has unsupported type {dt}; expected Binary, LargeBinary, Utf8, or LargeUtf8"
            ))),
        }
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Total byte size of all values in the column — O(1) via the Arrow values buffer.
    pub fn total_bytes(&self) -> usize {
        let arr = self.0.as_ref();
        match arr.data_type() {
            DataType::Binary     => arr.as_any().downcast_ref::<BinaryArray>()     .unwrap().values().len(),
            DataType::LargeBinary => arr.as_any().downcast_ref::<LargeBinaryArray>().unwrap().values().len(),
            DataType::Utf8       => arr.as_any().downcast_ref::<StringArray>()     .unwrap().values().len(),
            DataType::LargeUtf8  => arr.as_any().downcast_ref::<LargeStringArray>().unwrap().values().len(),
            _ => unreachable!("BinaryCol::try_from already validated the type"),
        }
    }

    /// Returns the raw bytes of row `i`, zero-copy.
    pub fn value(&self, i: usize) -> &[u8] {
        let arr = self.0.as_ref();
        match arr.data_type() {
            DataType::Binary => arr
                .as_any()
                .downcast_ref::<BinaryArray>()
                .unwrap()
                .value(i),
            DataType::LargeBinary => arr
                .as_any()
                .downcast_ref::<LargeBinaryArray>()
                .unwrap()
                .value(i),
            DataType::Utf8 => arr
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(i)
                .as_bytes(),
            DataType::LargeUtf8 => arr
                .as_any()
                .downcast_ref::<LargeStringArray>()
                .unwrap()
                .value(i)
                .as_bytes(),
            _ => unreachable!("BinaryCol::try_from already validated the type"),
        }
    }
}

// SAFETY: ArrayRef = Arc<dyn Array + Send + Sync>; the inner data is immutable
// once constructed so sharing across threads is safe.
unsafe impl Send for BinaryCol {}
unsafe impl Sync for BinaryCol {}

// ---------------------------------------------------------------------------
// ArrowBatch
// ---------------------------------------------------------------------------

/// A decoded Arrow RecordBatch exposed as a [`KvBatch`].
///
/// Column references are zero-copy: `iter` borrows directly from the Arrow
/// buffer memory without any per-row allocation.
pub struct ArrowBatch {
    pub(crate) keys:   BinaryCol,
    pub(crate) values: BinaryCol,
}

impl ArrowBatch {
    pub fn from_record_batch(
        batch:       &RecordBatch,
        key_col_idx: usize,
        val_col_idx: usize,
    ) -> Result<Self, crate::error::BqError> {
        let schema = batch.schema();
        let key_name = schema.field(key_col_idx).name().clone();
        let val_name = schema.field(val_col_idx).name().clone();
        Ok(Self {
            keys:   BinaryCol::try_from(batch.column(key_col_idx).clone(), &key_name)?,
            values: BinaryCol::try_from(batch.column(val_col_idx).clone(), &val_name)?,
        })
    }
}

impl KvBatch for ArrowBatch {
    fn len(&self) -> usize {
        self.keys.len()
    }

    fn iter(&self) -> impl Iterator<Item = (&[u8], &[u8])> {
        (0..self.keys.len()).map(|i| (self.keys.value(i), self.values.value(i)))
    }

    fn total_bytes(&self) -> u64 {
        (self.keys.total_bytes() + self.values.total_bytes()) as u64
    }
}
