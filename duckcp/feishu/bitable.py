"""
多维表格接口
"""
import logging
from typing import TypedDict, NotRequired, Any

from duckcp.feishu import OPEN_API, FeiShuError
from duckcp.helper import http
from duckcp.helper.collection import chunk

LIST_FIELDS_API = f'{OPEN_API}/bitable/v1/apps/{{document}}/tables/{{table}}/fields'
LIST_RECORDS_API = f'{OPEN_API}/bitable/v1/apps/{{document}}/tables/{{table}}/records/search'
BATCH_CREATE_API = f'{OPEN_API}/bitable/v1/apps/{{document}}/tables/{{table}}/records/batch_create'
BATCH_DELETE_API = f'{OPEN_API}/bitable/v1/apps/{{document}}/tables/{{table}}/records/batch_delete'
logger = logging.getLogger(__name__)


class Record(TypedDict):
    """
    多维表格单行记录。
    """
    record_id: NotRequired[str]  # 记录编号：创建时可忽略。
    fields: dict[str, Any]  # 字段数据。


class Field(TypedDict):
    """
    多维表格字段。
    """
    field_id: str
    field_name: str
    type: int
    ui_type: str
    is_primary: bool
    is_hidden: bool
    description: str


class Page[T](TypedDict):
    """
    分页查询结果
    """
    has_more: bool
    page_token: str
    total: int
    items: list[T]


class BatchDeleteRecord(TypedDict):
    """
    批量删除结果响应。
    """
    deleted: bool  # 是否删除成功。
    record_id: str  # 记录编号。


class Batch[T](TypedDict):
    """
    批量操作返回的结果。
    """
    records: list[T]  # 记录集。


class Response[T](TypedDict):
    """
    操作结果响应。
    """
    code: int  # 错误编码。
    msg: str  # 错误消息。
    data: T  # 结果。


def list_fields(access_token: str, document: str, table: str) -> list[Field]:
    url = LIST_FIELDS_API.format(document=document, table=table)
    response: Response[Page[Field]] = http.get(url, headers={'Authorization': f'Bearer {access_token}'}, query={'page_size': 100})
    if response['code'] == 0:
        return response['data']['items']
    else:
        raise FeiShuError('获取字段', response.get('msg', ''))


def list_records(access_token: str, document: str, table: str) -> list[Record]:
    """
    分页查询所有记录。
    """
    url = LIST_RECORDS_API.format(document=document, table=table)
    headers = {'Authorization': f'Bearer {access_token}'}
    query: dict[str, Any] = {'page_size': 500}
    records: list[Record] = []
    while True:
        response: Response[Page[Record]] = http.post(url, headers=headers, query=query, params={})
        if response['code'] == 0:
            records.extend(response['data']['items'])
            if response['data']['has_more']:
                query['page_token'] = response['data']['page_token']
            else:
                break
        else:
            raise FeiShuError('获取记录', response.get('msg', ''))
    return records


def batch_create(access_token: str, document: str, table: str, records: list[Record]) -> list[Record]:
    """
    批量创建记录。
    @param access_token: 访问凭证。
    @param document: 多维文档编号。
    @param table: 多维表格编号。
    @param records: 记录编号集合。
    @return: 成功创建的记录。
    """
    url = BATCH_CREATE_API.format(document=document, table=table)
    result: list[Record] = []
    for bucket in chunk(records, 1000):
        response: Response[Batch[Record]] = http.post(url, headers={'Authorization': f'Bearer {access_token}'}, params={'records': bucket})
        if response['code'] == 0:
            result.extend(response['data']['records'])
        else:
            raise FeiShuError('批量创建', response.get('msg', ''))
    logger.debug('rows=%s', len(result))
    return result


def batch_delete(access_token: str, document: str, table: str, records: list[str]) -> list[str]:
    """
    批量删除记录。
    @param access_token: 访问凭证。
    @param document: 多维文档编号。
    @param table: 多维表格编号。
    @param records: 记录编号集合。
    @return: 删除成功的记录编号集合。
    """
    url = BATCH_DELETE_API.format(document=document, table=table)
    result = []
    for bucket in chunk(records, 500):
        response: Response[Batch[BatchDeleteRecord]] = http.post(url, headers={'Authorization': f'Bearer {access_token}'}, params={'records': bucket})
        if response['code'] == 0:
            result.extend([record['record_id'] for record in response['data']['records'] if record['deleted']])
        else:
            raise FeiShuError('批量删除', response.get('msg', ''))
    logger.debug('rows=%s', len(result))
    return result
