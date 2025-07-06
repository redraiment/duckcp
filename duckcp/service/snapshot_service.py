import logging
from typing import Optional

from duckcp.configuration import meta_configuration as metadata
from duckcp.entity.snapshot import Snapshot

logger = logging.getLogger(__name__)


def snapshot_find(storage_id: int) -> Optional[Snapshot]:
    """
    找到存储单元的快照记录。
    """
    with metadata.connect() as meta:
        return meta.record('select * from snapshots where storage_id = ?', storage_id, constructor=Snapshot._make)


def take_snapshot(storage_id: int, checksum: str, records: list[str]):
    """
    保存快照。
    """
    with metadata.connect() as meta:
        snapshot = meta.record('''
          insert or replace into snapshots
            (storage_id, checksum, records)
          values
            (?, ?, ?)
          returning *
        ''', storage_id, checksum, records, constructor=Snapshot._make)
        logger.info('创建快照(%s)', storage_id)
        logger.debug('snapshot=%s', snapshot)
