# Operations Runbook

## Failed contract validation
1. Open the quarantine file.
2. Compare headers with the dataset contract.
3. Correct the producer or create a versioned contract change.
4. Replay the batch with a new batch ID.

## Failed referential validation
1. Identify missing customer or product keys.
2. Confirm source arrival ordering.
3. Reprocess the missing dimension before the fact load.

## Rollback
Gold outputs are batch-derived. Restore the previous Gold snapshot and rerun from immutable Bronze.
