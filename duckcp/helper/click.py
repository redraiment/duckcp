from json import JSONDecodeError

from click import ParamType

from duckcp.helper.serialization import json_decode


class JSONParamType(ParamType):
    """
    JSON类型。
    """
    name = 'json'

    def convert(self, value, parameter, context):
        try:
            return json_decode(value)
        except JSONDecodeError:
            self.fail(f"'{value}'不是有效的JSON格式", parameter, context)


JSON = JSONParamType()
