"""
数据迁移至多维表格，原理如下：
1. 在来源仓库上执行SQL。
2. 对比查询结果与快照是否一致。
3. 若快照已失效，则清空多维表格的记录。
4. 清空多维表格后，再添加新记录到多维表格。
5. 记录保存完成后，讲记录编码保存至本地缓存。
"""
import logging
from typing import Any

from duckcp.entity.statement import Statement
from duckcp.entity.storage import Storage
from duckcp.feishu.bitable import batch_delete, batch_create, Record
from duckcp.helper.digest import sha256
from duckcp.repository.bitable_repository import BiTableRepository
from duckcp.service import snapshot_service

logger = logging.getLogger(__name__)


def digest(records: list[dict[str, Any]]) -> str:
    """
    计算记录的摘要。
    """
    content = '\n'.join([
        f'{name}:{value}'
        for record in records
        for name, value in sorted(record.items())
    ])
    return sha256(content)


def bitable_transform(statement: Statement, repository: BiTableRepository, storage: Storage):
    """
    将数据源迁移到多维表格中。
    """
    authenticator = repository.authenticator
    logger.debug('storage=%s', storage)
    document = storage.properties['document']
    table = storage.properties['table']
    logger.debug('document=%s, table=%s', document, table)

    # 1. 获取数据并计算摘要
    records = statement.all()
    checksum = digest(records)
    logger.debug('records=%s, checksum=%s', records, checksum)

    # 2. 对比快照
    snapshot = snapshot_service.snapshot_find(storage.id)
    if snapshot is not None and snapshot.checksum != checksum and bool(snapshot.records):
        # 快照失效，先清空历史数据
        batch_delete(authenticator(), document, table, snapshot.records)
        logger.info('清空飞书文档(%s)多维表格(%s)', document, table)

    # 3. 保存数据
    if snapshot is None or snapshot.checksum != checksum and bool(records):
        # 无快照或快照失效，则添加数据
        records = batch_create(authenticator(), document, table, [
            Record(fields=record)
            for record in records
        ])
        snapshot_service.take_snapshot(storage.id, checksum, [record['record_id'] for record in records])
        logger.info('保存飞书文档(%s)多维表格(%s)记录%s条', document, table, len(records))
    else:
        logger.info('飞书文档(%s)多维表格(%s)数据未变化', document, table)
