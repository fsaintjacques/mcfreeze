// SPDX-License-Identifier: Apache-2.0

use std::future::Future;

use mcfreeze_loader::RecordBatchSource;

use crate::error::BqError;
use crate::source::BqRecordBatchSource;

impl RecordBatchSource for BqRecordBatchSource {
    type Error = BqError;

    fn next_batch(
        &mut self,
    ) -> impl Future<Output = Result<Option<arrow::record_batch::RecordBatch>, Self::Error>> + Send
    {
        self.next_batch()
    }
}
