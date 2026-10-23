"""
Audit stamping helpers for writes to the main market list collections.

The change-stream audit system reads lastChangedBy / lastChangedAt / lastChangeSource
from every write. Deletes are performed as a transactional "stamp then delete" so the
pre-image captured by the change stream carries the audit fields of the deletion.

Only the main collections are audited; adhoc collections must not use these helpers.
"""
from datetime import datetime, timezone

LAST_CHANGED_BY = 'lastChangedBy'
LAST_CHANGED_AT = 'lastChangedAt'
LAST_CHANGE_SOURCE = 'lastChangeSource'
DELETED_FLAG = '_deleted'

SYSTEM_USER = 'SYS'

SOURCE_ROLLING = 'rolling'
SOURCE_CLEANUP = 'cleanup'
SOURCE_DEFAULT = 'default'

AUDITED_COLLECTIONS = frozenset({'market_list', 'market_list_international', 'market_list_connections'})

# Documents per stamp+delete transaction (keeps each transaction well within time/size limits)
TRANSACTION_BATCH_SIZE = 1000


def utc_now():
    """Timestamp for lastChangedAt (stored as BSON Date)."""
    return datetime.now(timezone.utc)


def audit_fields(changed_by, change_source, changed_at):
    return {
        LAST_CHANGED_BY: changed_by,
        LAST_CHANGED_AT: changed_at,
        LAST_CHANGE_SOURCE: change_source,
    }


def stamp_and_delete(collection, ids, changed_by, change_source, changed_at,
                     batch_size=TRANSACTION_BATCH_SIZE):
    """Delete documents by _id; per batch, in one transaction, first $set _deleted + audit fields, then delete.

    Returns the total number of deleted documents.
    """
    if collection.name not in AUDITED_COLLECTIONS:
        raise ValueError(f"stamp_and_delete is only for audited collections, got '{collection.name}'")

    stamp = {'$set': {DELETED_FLAG: True, **audit_fields(changed_by, change_source, changed_at)}}
    client = collection.database.client
    total_deleted = 0

    for i in range(0, len(ids), batch_size):
        batch_filter = {'_id': {'$in': ids[i:i + batch_size]}}

        def _txn(session, batch_filter=batch_filter):
            collection.update_many(batch_filter, stamp, session=session)
            return collection.delete_many(batch_filter, session=session).deleted_count

        with client.start_session() as session:
            total_deleted += session.with_transaction(_txn)

    return total_deleted
