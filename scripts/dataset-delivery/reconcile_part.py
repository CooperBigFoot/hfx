#!/usr/bin/env python3
"""reconcile : InterruptedCopyJournal × OwnedUploadObservation → UnverifiedCopyJournal | Refusal.

This explicit entrypoint preserves interrupted journal bytes on refusal.
"""

from dataset_delivery import reconcile_part_main


if __name__ == "__main__":
    raise SystemExit(reconcile_part_main())
