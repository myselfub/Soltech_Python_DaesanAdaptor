import asyncio
import ctypes
import json
import os
import re
import socket
import sys
import threading
import traceback
from copy import deepcopy
from datetime import datetime, timedelta
import time
from io import StringIO

import clr
import dbutils.pooled_db
import pyodbc
from dbutils.pooled_db import PooledDB

import app_config


class Database:
    """ 데이터베이스 클래스 """
    _instance = None
    __inited: bool = False
    __pool: dbutils.pooled_db.PooledDB = None
    __max_insert_row_count: int = 49

    def __new__(cls, *args, **kwargs) -> _instance:
        if cls._instance is None:
            cls._instance = super(Database, cls).__new__(cls)
        return cls._instance

    def __init__(self) -> None:
        try:
            if not self.__inited:
                self.__inited = True
                _creator: pyodbc = pyodbc
                _config_database: dict = app_config.ConfigUtil().get_configs().get('DATABASE')
                _max_connection: int = int(f'{_config_database.get('MAX_CONNECTION')}')
                _min_cache: int = int(f'{_config_database.get('MIN_CACHE')}')
                _max_cache: int = int(f'{_config_database.get('MAX_CACHE')}')
                _max_share: int = int(f'{_config_database.get('MAX_SHARE')}')
                _blocking: bool = True
                _user: str = _config_database.get('USER')
                _password: str = _config_database.get('PASSWORD')

                if _config_database.get('DSN') is None:
                    self.__pool = PooledDB(
                        creator=_creator,
                        maxconnections=_max_connection,
                        mincached=_min_cache,
                        maxcached=_max_cache,
                        maxshared=_max_share,
                        blocking=_blocking,
                        host=f'{_config_database.get('IP')},{_config_database.get('PORT')}',
                        database=f'{_config_database.get('DATABASE')}',
                        user=_user,
                        password=_password
                    )
                else:
                    self.__pool = PooledDB(
                        creator=_creator,
                        maxconnections=_max_connection,
                        mincached=_min_cache,
                        maxcached=_max_cache,
                        maxshared=_max_share,
                        blocking=_blocking,
                        dsn=f'{_config_database.get('DSN')}',
                        user=_user,
                        password=_password
                    )
        except Exception as e:
            app_config.LogUtil().error(
                message=f'{self.__module__}.{self.__class__.__name__}.init Error: {e}'
            )
            sys.exit(1)

    def get_connection(self) \
            -> dbutils.pooled_db.PooledDedicatedDBConnection or dbutils.pooled_db.PooledSharedDBConnection:
        return self.__pool.connection()

    def select(self, query: str, params: tuple = None) -> list:
        _rows: list = []
        _conn: dbutils.pooled_db.PooledDedicatedDBConnection = None
        try:
            if self.__pool is not None:
                _conn = self.__pool.connection()
                with _conn.cursor() as _cursor:
                    if params is not None:
                        _cursor.execute(query, params)
                    else:
                        _cursor.execute(query)
                    _fetch_rows: list[tuple[str or int or float]] = _cursor.fetchall()
                    _columns: list[str] = [_column[0] for _column in _cursor.description]

                for _fetch_row in _fetch_rows:
                    _rows.append(dict(zip(_columns, _fetch_row)))
                app_config.LogUtil().info(message=f'Execute Query: {query}')
        except Exception as e:
            app_config.LogUtil().error(
                message=f'{self.__module__}.{self.__class__.__name__}.select Error: {e}'
            )
        finally:
            if _conn is not None:
                _conn.close()

        return _rows

    def select_tags(self) -> list:
        _config_database: dict = app_config.ConfigUtil().get_configs().get('DATABASE')
        _select_tag_query: str = _config_database.get('SELECT_TAG_QUERY')
        if _select_tag_query in app_config.ConfigUtil().get_none_values():
            _select_tag_query: str = """SELECT
                   TAGSN AS TAG_SN,
                   TAG_NM
               FROM
                   [DBSCHEMA].RDITAG_TB
               WHERE
                   TAG_USE_YN = 'Y'
                ORDER BY
                    TAGSN AS TAG_SN"""
        _result: list = self.select(
            query=_select_tag_query,
            params=None
        )
        for _dict in _result:
            _dict['TAG_SN'] = str(int(_dict.get('TAG_SN', '')))
        return _result

    def insert(self, query: str, params: tuple = None) -> None:
        _conn: dbutils.pooled_db.PooledDedicatedDBConnection = None
        try:
            if self.__pool is not None:
                _conn = self.__pool.connection()
                with _conn.cursor() as _cursor:
                    if _conn is not None:
                        _conn.close()
                    if params is not None:
                        _cursor.execute(query, params)
                    else:
                        _cursor.execute(query)
                    _cursor.commit()
                app_config.LogUtil().info(message=f'Execute Query: {query}, Param: {params}')
        except Exception as e:
            app_config.LogUtil().error(
                message=f'{self.__module__}.{self.__class__.__name__}.insert Error: {e}'
            )
        finally:
            if _conn is not None:
                _conn.close()

    def insert_revisn_log(self, put_list: list or str, dest: str) -> list:
        _result = []
        if put_list in app_config.ConfigUtil().get_none_values():
            return _result
        elif type(put_list) == str:
            put_list = json.loads(put_list)

        now: str = datetime.now().strftime('%Y-%m-%d')

        """_str_insert_subfix: str = '(?, ?, ?, ?, ?, ?, ?), '"""
        _list_insert_param: list = []
        _str_insert_subfix: str = 'INTO [DBSCHEMA].RDIREVISNLOG_TB (REVISN_DT, DATA_SAVE_DT, TAG_SN, LAST_VALUE, OLD_VALUE, REVISN_RESN, REVISN_REMARK) VALUES (?, ?, ?, ?, ?, ?, ?) '
        for _dict_data in put_list:
            _tag_nm: str = _dict_data.get('name')
            _result.append(_tag_nm)
            _save_dt: str = _dict_data.get('time').replace('-', '').replace(' ', '').replace(':', '').replace(
                '.000', '')
            _tag_sn: str = _dict_data.get('sn')
            _last_value: str = _dict_data.get('val') if _dict_data.get('val') is not None else ''
            _old_value: str = _dict_data.get('old_val') if _dict_data.get('old_val') is not None else ''
            _revisn_resn = '일괄보정'

            _param: dict = {
                'REVISN_DT': now,
                'DATA_SAVE_DT': _save_dt,
                'TAG_SN': _tag_sn,
                'LAST_VALUE': _last_value,
                'OLD_VALUE': _old_value,
                'REVISN_RESN': _revisn_resn,
                'REVISN_REMARK': dest
            }
            _params: tuple = (
                _param.get('REVISN_DT', ''),
                _param.get('DATA_SAVE_DT', ''),
                _param.get('TAG_SN', ''),
                _param.get('LAST_VALUE', ''),
                _param.get('OLD_VALUE', ''),
                _param.get('REVISN_RESN', ''),
                _param.get('REVISN_REMARK', '')
            )
            _list_insert_param.append(_params)
        _query_len = len(_list_insert_param)
        _split_len = 0
        _string_io = StringIO()
        _string_io.write('INSERT ALL ')
        _tuple_param: tuple = ()
        for _idx in range(_query_len):
            _tuple_param = _tuple_param + _list_insert_param[_idx]
            _string_io.write(_str_insert_subfix)
            _split_len += 1
            if _split_len > self.__max_insert_row_count or (_idx == _query_len - 1):
                _split_len = 0
                """_str_insert = _string_io.getvalue()[:-2]"""
                _string_io.write('SELECT * FROM dual')
                _str_insert = _string_io.getvalue()
                self.insert(
                    query=_str_insert,
                    params=_tuple_param
                )
                _string_io.seek(0)
                _string_io.truncate(0)
                _string_io.write('INSERT ALL ')
                _tuple_param: tuple = ()
        _string_io.close()
        _result_set = set(_result)
        _result = list(_result_set)
        return _result


class Historian:
    """ 히스토리안 클래스 """
    _instance = None
    __inited: bool = False
    __disposed_value: bool = False
    __list_none_value: list = None
    __local_time: bool or int = False
    __date_time_format: str = ''
    __digits: int = 0
    __server = None
    __ihuApi = None
    __ihuErrEnum = None
    __ihuDataEnum = None
    __ihuQualityEnum = None
    __ihuTagPropertiesEnum = None
    __ihuDataSample = None
    __ihuRawDataSample = None
    __c_convert = None
    __c_datetime = None

    """
    def __new__(cls, *args, **kwargs) -> _instance:
        if cls._instance is None:
            cls._instance = super(Historian, cls).__new__(cls)
        return cls._instance
    """

    def __init__(self) -> None:
        """if not self.__inited:"""
        self.__inited = True
        self.__list_none_value = app_config.ConfigUtil().get_none_values()
        if getattr(sys, 'frozen', False):
            _this_path: str = os.path.dirname(sys.executable)
        else:
            _this_path: str = os.path.dirname(os.path.abspath(__file__))
        _config: dict = app_config.ConfigUtil().get_configs().get('CONFIG')
        _num: str = ''.join(re.findall(r'\d+', _config.get('BIT')))
        if _num == '32':
            _dll_ihuapi: str = 'IHUAPI_32.dll'
            _dll_utilities: str = 'Utilities_32.dll'
        else:
            _dll_ihuapi: str = 'IHUAPI.dll'
            _dll_utilities: str = 'Utilities.dll'
        self.__date_time_format = _config.get('DATEFORMAT')
        self.__digits = int(_config.get('DIGITS'))
        ctypes.CDLL(os.path.join(_this_path, 'dll', _dll_ihuapi))
        clr.AddReference(os.path.join(_this_path, 'dll', _dll_utilities))
        from Proficy.Historian.UserAPI import (
            IHUAPI,
            ihuErrorCode,
            ihuDataType,
            ihuQualityStatus,
            ihuTagProperties,
            IHU_DATA_SAMPLE,
            IHU_RETRIEVED_RAW_VALUES,
            IHU_RAW_QUALITY
        )
        from System import DateTime as CDateTime, Convert as CConvert
        self.__ihuApi: Proficy.Historian.UserAPI.IHUAPI = IHUAPI
        self.__ihuErrEnum: Proficy.Historian.UserAPI.ihuErrorCode = ihuErrorCode
        self.__ihuDataEnum: Proficy.Historian.UserAPI.ihuDataType = ihuDataType
        self.__ihuQualityEnum: Proficy.Historian.UserAPI.ihuQualityStatus = ihuQualityStatus
        self.__ihuTagPropertiesEnum: Proficy.Historian.UserAPI.ihuTagProperties = ihuTagProperties
        self.__ihuDataSample: Proficy.Historian.UserAPI.IHU_DATA_SAMPLE = IHU_DATA_SAMPLE
        self.__ihuRawDataSample: Proficy.Historian.UserAPI.IHU_RETRIEVED_RAW_VALUES = IHU_RETRIEVED_RAW_VALUES
        self.__ihuRawQuality: Proficy.Historian.UserAPI.IHU_RAW_QUALITY = IHU_RAW_QUALITY
        self.__c_datetime: System.DateTime = CDateTime
        self.__c_convert: System.Convert = CConvert

    def connect(self) -> bool:
        return self.__connect()

    def __connect(self) -> bool:
        _config_historian: dict = app_config.ConfigUtil().get_configs().get('HISTORIAN')
        _ip: str = _config_historian.get('IP')
        _user: str = _config_historian.get('USER')
        _password: str = _config_historian.get('PASSWORD')
        _local_time: str = _config_historian.get('LOCAL_TIME')
        if _local_time.lower() == 'true':
            self.__local_time: bool = True
        elif _local_time.lower() == 'false':
            self.__local_time: bool = False
        elif re.fullmatch(r'^-?[0-9]$', _local_time):
            self.__local_time: int = int(_local_time)
        _connect_result: tuple = self.__ihuApi.ihuConnect(_ip, _user, _password)
        _err_code: Proficy.Historian.UserAPI.ihuErrorCode = _connect_result[0]
        self.__server: int = _connect_result[1]
        return _err_code == self.__ihuErrEnum.OK

    def __disposed(self, disposing: bool) -> None:
        if not self.__disposed_value:
            if disposing:
                self.__disconnect()
            self.__disposed_value = True

    def __disconnect(self) -> None:
        self.__ihuApi.ihuDisconnect(self.__server)

    def req_values(self, param: dict) -> str:
        _list_tag: str or list[str] = param.get('tagList')
        if _list_tag in self.__list_none_value:
            return ''
        elif type(_list_tag) == str:
            _list_tag = json.loads(_list_tag)

        _result: str = ''
        if self.__connect():
            _result = self.__req_values(list_tag=_list_tag)
            self.__disconnect()
        self.__disposed(disposing=True)

        return _result

    def __req_values(self, list_tag: list[str]) -> str:
        _read_value_result: tuple = self.__ihuApi.ihuReadCurrentValue(self.__server, list_tag)
        _err_code_read_value_result: Proficy.Historian.UserAPI.ihuErrorCode = _read_value_result[0]
        _p_data: (
                Proficy.Historian.UserAPI.IHU_DATA_SAMPLE or
                list[Proficy.Historian.UserAPI.IHU_DATA_SAMPLE]
        ) = _read_value_result[1]
        _p_err: (
                Proficy.Historian.UserAPI.ihuErrorCode or
                list[Proficy.Historian.UserAPI.ihuErrorCode]
        ) = _read_value_result[2]

        _list_result: list[dict] = []
        if _p_data is not None:
            for _idx, _value in enumerate(_p_data):
                _dict_result: dict = {}
                if _p_err[_idx] != self.__ihuErrEnum.INVALID_TAGNAME:
                    _switch: dict = {
                        self.__ihuDataEnum.Undefined: str(_value.Value.Integer),
                        self.__ihuDataEnum.Short: str(_value.Value.Integer),
                        self.__ihuDataEnum.Integer: str(_value.Value.Integer),
                        self.__ihuDataEnum.Float: str(_value.Value.Float),
                        self.__ihuDataEnum.DoubleFloat: str(_value.Value.Float)
                    }
                    _str_value: str = _switch.get(_value.ValueDataType, str(_value.Value.Integer))
                    _timestamp_result: tuple = self.__ihuApi.IHU_TIMESTAMP_ToParts(_value.TimeStamp)
                    _err_code_timestamp: Proficy.Historian.UserAPI.ihuErrorCode = _timestamp_result[0]
                    _dt_time_before: str = _timestamp_result[1].ToString()
                    _dt_time_before: str = _dt_time_before.replace('오전', 'AM').replace('오후', 'PM')
                    _dt: datetime = datetime.strptime(_dt_time_before, self.__date_time_format)
                    _dict_result['name']: str = _value.Tagname
                    _dict_result['time']: str = _dt.strftime('%Y-%m-%d %H:%M:%S')
                    _dict_result['val']: str = str(round(float(_str_value), self.__digits))
                    _dict_result['conf']: int = \
                        100 if _value.Quality.ToString() == self.__ihuQualityEnum.OPCGood.ToString() else 0
                    _list_result.append(_dict_result)

        _str_result: str = '{"cmd": "reqValues", "param": []}'
        if len(_list_result) > 0:
            _dict_result_: dict = {
                'cmd': 'reqValues',
                'param': _list_result
            }
            _str_result = str(_dict_result_)

        return _str_result

    def fetch_values(self, param: dict) -> str:
        _list_tag: str or list[str] = param.get('tagList')
        if _list_tag in self.__list_none_value:
            return ''
        elif type(_list_tag) == str:
            _list_tag = json.loads(_list_tag)

        _str_start: str = param.get('start')
        _str_dt_format = '%Y-%m-%d %H:%M:%S'
        _str_start = (datetime.strptime(_str_start, _str_dt_format) - timedelta(seconds=1)).strftime(_str_dt_format)
        _str_end: str = param.get('end')

        _result: str = ''
        if self.__connect():
            _result = self.__fetch_values(list_tag=_list_tag, str_start=_str_start, str_end=_str_end)
            self.__disconnect()
        self.__disposed(disposing=True)

        return _result

    def __fetch_values(self, list_tag: list[str], str_start: str, str_end: str) -> str:
        try:
            if type(self.__local_time) == bool:
                if self.__local_time:
                    _dt_start: System.DateTime = self.__c_convert.ToDateTime(str_start).ToLocalTime()
                    _dt_end: System.DateTime = self.__c_convert.ToDateTime(str_end).ToLocalTime()
                else:
                    _dt_start: System.DateTime = self.__c_convert.ToDateTime(str_start)
                    _dt_end: System.DateTime = self.__c_convert.ToDateTime(str_end)
            else:
                _dt_start: System.DateTime = self.__c_convert.ToDateTime(str_start).AddHours(self.__local_time)
                _dt_end: System.DateTime = self.__c_convert.ToDateTime(str_end).AddHours(self.__local_time)
        except Exception as e:
            app_config.LogUtil().error(
                message=f'{self.__module__}.{self.__class__.__name__}.fetch_value Error: {e}'
            )
            return ''
        _start_time_result: tuple = self.__ihuApi.IHU_TIMESTAMP_FromParts(_dt_start)
        _end_time_result: tuple = self.__ihuApi.IHU_TIMESTAMP_FromParts(_dt_end)
        _err_code_start: Proficy.Historian.UserAPI.ihuErrorCode = _start_time_result[0]
        _timestamp_start: Proficy.Historian.UserAPI.IHU_TIMESTAMP = _start_time_result[1]
        _err_code_end: Proficy.Historian.UserAPI.ihuErrorCode = _end_time_result[0]
        _timestamp_end: Proficy.Historian.UserAPI.IHU_TIMESTAMP = _end_time_result[1]

        _list_result: list[dict] = []
        for _tag in list_tag:
            _read_raw_result: tuple = self.__ihuApi.ihuReadRawDataByTime(
                self.__server, _tag, _timestamp_start, _timestamp_end
            )
            _p_err: Proficy.Historian.UserAPI.ihuErrorCode = _read_raw_result[0]
            _p_data: (
                    Proficy.Historian.UserAPI.IHU_DATA_SAMPLE or
                    list[Proficy.Historian.UserAPI.IHU_DATA_SAMPLE]
            ) = _read_raw_result[1]
            if _p_data is not None:
                _list_result_: list[dict] = []
                for _value in _p_data:
                    _dict_result_: dict = {}
                    if _p_err != self.__ihuErrEnum.INVALID_TAGNAME:
                        _switch: dict = {
                            self.__ihuDataEnum.Short: str(_value.Value.Integer),
                            self.__ihuDataEnum.Integer: str(_value.Value.Integer),
                            self.__ihuDataEnum.Float: str(_value.Value.Float),
                            self.__ihuDataEnum.DoubleFloat: str(_value.Value.Float)
                        }
                        _str_value: str = _switch.get(_value.ValueDataType, str(_value.Value.Integer))
                        _timestamp_result: tuple = self.__ihuApi.IHU_TIMESTAMP_ToParts(_value.TimeStamp)
                        _timestamp_err_code: Proficy.Historian.UserAPI.ihuErrorCode = _timestamp_result[0]
                        _dt_time_before: str = _timestamp_result[1].ToString()
                        _dt_time_before: str = _dt_time_before.replace('오전', 'AM').replace('오후', 'PM')
                        _dt: datetime = datetime.strptime(_dt_time_before, self.__date_time_format)
                        _dict_result_['time']: str = _dt.strftime('%Y-%m-%d %H:%M:%S')
                        _dict_result_['val']: str = str(round(float(_str_value), self.__digits))
                    _list_result_.append(_dict_result_)

                _dict_result: dict = {
                    'name': _tag,
                    'values': _list_result_
                }
                _list_result.append(_dict_result)

        _str_result: str = '{"cmd": "fetchValues", "param": []}'
        if len(_list_result) > 0:
            _dict_result_: dict = {
                'cmd': 'fetchValues',
                'param': _list_result
            }
            _str_result = str(_dict_result_)
        return _str_result

    def multi_fetch_values(self, param: dict) -> str:
        _list_tag: str or list[str] = param.get('tagList')
        if _list_tag in self.__list_none_value:
            return ''
        elif type(_list_tag) == str:
            _list_tag = json.loads(_list_tag)

        _str_start: str = param.get('start')
        _str_dt_format = '%Y-%m-%d %H:%M:%S'
        _str_start = (datetime.strptime(_str_start, _str_dt_format) - timedelta(seconds=1)).strftime(_str_dt_format)
        _str_end: str = param.get('end')

        _result: str = ''
        if self.__connect():
            _result = self.__multi_fetch_values(list_tag=_list_tag, str_start=_str_start, str_end=_str_end)
            self.__disconnect()
        self.__disposed(disposing=True)

        return _result

    def __multi_fetch_values(self, list_tag: list[str], str_start: str, str_end: str) -> str:
        try:
            if type(self.__local_time) == bool:
                if self.__local_time:
                    _dt_start: System.DateTime = self.__c_convert.ToDateTime(str_start).ToLocalTime()
                    _dt_end: System.DateTime = self.__c_convert.ToDateTime(str_end).ToLocalTime()
                else:
                    _dt_start: System.DateTime = self.__c_convert.ToDateTime(str_start)
                    _dt_end: System.DateTime = self.__c_convert.ToDateTime(str_end)
            else:
                _dt_start: System.DateTime = self.__c_convert.ToDateTime(str_start).AddHours(self.__local_time)
                _dt_end: System.DateTime = self.__c_convert.ToDateTime(str_end).AddHours(self.__local_time)
        except Exception as e:
            app_config.LogUtil().error(
                message=f'{self.__module__}.{self.__class__.__name__}.fetch_value Error: {e}'
            )
            return ''
        _start_time_result: tuple = self.__ihuApi.IHU_TIMESTAMP_FromParts(_dt_start)
        _end_time_result: tuple = self.__ihuApi.IHU_TIMESTAMP_FromParts(_dt_end)
        _err_code_start: Proficy.Historian.UserAPI.ihuErrorCode = _start_time_result[0]
        _timestamp_start: Proficy.Historian.UserAPI.IHU_TIMESTAMP = _start_time_result[1]
        _err_code_end: Proficy.Historian.UserAPI.ihuErrorCode = _end_time_result[0]
        _timestamp_end: Proficy.Historian.UserAPI.IHU_TIMESTAMP = _end_time_result[1]

        _string_array = ctypes.c_wchar_p * len(list_tag)
        _ctypes_array = _string_array(*list_tag)

        _read_raw_result: tuple = self.__ihuApi.ihuReadMultiTagRawDataByTime(
            self.__server, _ctypes_array, _timestamp_start, _timestamp_end
        )
        _p_err: Proficy.Historian.UserAPI.ihuErrorCode = _read_raw_result[0]
        _p_data: (
                Proficy.Historian.UserAPI.IHU_RETRIEVED_RAW_VALUES or
                list[Proficy.Historian.UserAPI.IHU_RETRIEVED_RAW_VALUES]
        ) = _read_raw_result[1]
        _list_result: list[dict] = []
        if _p_data is not None:
            for _value in _p_data:
                if _p_err != self.__ihuErrEnum.INVALID_TAGNAME:
                    _p_data_: (
                            Proficy.Historian.UserAPI.IHU_DATA_SAMPLE or
                            list[Proficy.Historian.UserAPI.IHU_DATA_SAMPLE]
                    ) = _value.Values
                    _tag: str = _value.Tagname
                    _list_result_: list[dict] = []
                    for _value_ in _p_data_:
                        _dict_result_: dict = {}
                        _switch: dict = {
                            self.__ihuDataEnum.Short: str(_value_.Value.Integer),
                            self.__ihuDataEnum.Integer: str(_value_.Value.Integer),
                            self.__ihuDataEnum.Float: str(_value_.Value.Float),
                            self.__ihuDataEnum.DoubleFloat: str(_value_.Value.Float)
                        }
                        _str_value: str = _switch.get(_value_.ValueDataType, str(_value_.Value.Integer))
                        _timestamp_result: tuple = self.__ihuApi.IHU_TIMESTAMP_ToParts(_value_.TimeStamp)
                        _timestamp_err_code: Proficy.Historian.UserAPI.ihuErrorCode = _timestamp_result[0]
                        _dt_time_before: str = _timestamp_result[1].ToString()
                        _dt_time_before: str = _dt_time_before.replace('오전', 'AM').replace('오후', 'PM')
                        _dt: datetime = datetime.strptime(_dt_time_before, self.__date_time_format)
                        _dict_result_['time']: str = _dt.strftime('%Y-%m-%d %H:%M:%S')
                        _dict_result_['val']: str = str(round(float(_str_value), self.__digits))
                        _list_result_.append(_dict_result_)
                    _dict_result: dict = {
                        'name': _tag,
                        'values': _list_result_
                    }
                    _list_result.append(_dict_result)

        _str_result: str = '{"cmd": "fetchValues", "param": []}'
        if len(_list_result) > 0:
            _dict_result_: dict = {
                'cmd': 'fetchValues',
                'param': _list_result
            }
            _str_result = str(_dict_result_)
        return _str_result

    def put_values(self, param: dict or list) -> str:
        _list_tag: str or list = ''
        is_biz_format = False
        if type(param) == list:
            _list_tag: str or list = param
            is_biz_format = True
        elif type(param) == dict:
            _list_tag: str or list[str] or list[dict] = param.get('tagList')
        if _list_tag in self.__list_none_value:
            return ''
        elif type(_list_tag) == str:
            _list_tag = json.loads(_list_tag)

        _result: str = ''
        if self.__connect():
            if is_biz_format:
                _result = asyncio.run(self.__put_values_biz_format(list_data=_list_tag))
            else:
                _result = asyncio.run(self.__put_values(list_data=_list_tag))
            self.__disconnect()
        self.__disposed(disposing=True)

        return _result

    async def __put_values_biz_format(self, list_data: list[dict]) -> str:
        _list_result_err_code: list = []
        _list_tag_name: list = []
        for _data in list_data:
            _list_data_sample: Proficy.Historian.UserAPI.IHU_DATA_SAMPLE = []
            _list_err_code: Proficy.Historian.UserAPI.ihuErrorCode = []
            _tag_name: str = _data.get('name')
            if _tag_name not in _list_tag_name:
                _list_tag_name.append(_tag_name)
            if _tag_name in self.__list_none_value:
                continue
            _time: str = _data.get('time')
            _val: str = _data.get('val')
            if _time in self.__list_none_value or _val in self.__list_none_value:
                continue
            _type: str = _data.get('type', 'float')
            _switch_type: dict = {
                'short': self.__ihuDataEnum.Short,
                'int': self.__ihuDataEnum.Integer,
                'float': self.__ihuDataEnum.Float,
                'doubleFloat': self.__ihuDataEnum.DoubleFloat
            }
            _switch_object: dict = {
                'short': self.__c_convert.ToSingle,
                'int': self.__c_convert.ToSingle,
                'float': self.__c_convert.ToSingle,
                'doubleFloat': self.__c_convert.ToDouble
            }

            try:
                if type(self.__local_time) == bool:
                    if self.__local_time:
                        _dt_time: System.DateTime = self.__c_convert.ToDateTime(_time).ToLocalTime()
                    else:
                        _dt_time: System.DateTime = self.__c_convert.ToDateTime(_time)
                else:
                    _dt_time: System.DateTime = self.__c_convert.ToDateTime(_time).AddHours(self.__local_time)
            except Exception as e:
                app_config.LogUtil().error(
                    message=f'{self.__module__}.{self.__class__.__name__}.fetch_value Error: {e}'
                )
                continue
            data_sample: Proficy.Historian.UserAPI.IHU_DATA_SAMPLE = self.__ihuDataSample()
            _time_result: tuple = self.__ihuApi.IHU_TIMESTAMP_FromParts(_dt_time)
            _time_err_code: Proficy.Historian.UserAPI.ihuErrorCode = _time_result[0]
            _timestamp: Proficy.Historian.UserAPI.IHU_TIMESTAMP = _time_result[1]
            data_sample.Tagname = _tag_name
            data_sample.TimeStamp = _timestamp
            data_sample.ValueDataType = _switch_type.get(_type, self.__ihuDataEnum.Integer)
            if re.fullmatch(r'^-?\d+(\.\d+)?$', str(_val)):
                _float_val = float(_val)
            else:
                _float_val = _val
            data_sample.ValueObject = _switch_object.get(_type, self.__c_convert.ToSingle)(_float_val)
            _raw_quality: Proficy.Historian.UserAPI.IHU_RAW_QUALITY = self.__ihuRawQuality()
            _raw_quality.QualityStatus = self.__ihuQualityEnum.OPCGood
            data_sample.Quality = _raw_quality
            _list_data_sample.append(data_sample)

            _result_err_code = self.__ihuApi.ihuWriteData(
                self.__server, _list_data_sample, _list_err_code, False, False
            )
            _dict_result: dict = {
                'name': _tag_name,
                'value': _result_err_code.ToString()
            }
            _list_result_err_code.append(_dict_result)

        app_config.LogUtil().info(message=f'PutValues: {_list_tag_name}')
        _result_str: str = f'{{"cmd": "putValues", "param": {_list_result_err_code}}}'

        return _result_str

    async def __put_values(self, list_data: list[dict]) -> str:
        _list_result_err_code: list = []
        _list_tag_name: list = []
        for _data in list_data:
            _tag_name: str = _data.get('name')
            _list_tag_name.append(_tag_name)
            _tag_values: list[dict] = _data.get('values')
            if _tag_name in self.__list_none_value:
                continue

            _list_data_sample: Proficy.Historian.UserAPI.IHU_DATA_SAMPLE = []
            _list_err_code: Proficy.Historian.UserAPI.ihuErrorCode = []
            for _tag_value in _tag_values:
                _time: str = _tag_value.get('time')
                _val: str = _tag_value.get('val')
                _type: str = _tag_value.get('type', 'float')
                _switch_type: dict = {
                    'short': self.__ihuDataEnum.Short,
                    'int': self.__ihuDataEnum.Integer,
                    'float': self.__ihuDataEnum.Float,
                    'doubleFloat': self.__ihuDataEnum.DoubleFloat
                }
                _switch_object: dict = {
                    'short': self.__c_convert.ToSingle,
                    'int': self.__c_convert.ToSingle,
                    'float': self.__c_convert.ToSingle,
                    'doubleFloat': self.__c_convert.ToDouble
                }
                if _time in self.__list_none_value or _val in self.__list_none_value:
                    continue
                try:
                    if type(self.__local_time) == bool:
                        if self.__local_time:
                            _dt_time: System.DateTime = self.__c_convert.ToDateTime(_time).ToLocalTime()
                        else:
                            _dt_time: System.DateTime = self.__c_convert.ToDateTime(_time)
                    else:
                        _dt_time: System.DateTime = self.__c_convert.ToDateTime(_time).AddHours(self.__local_time)
                except Exception as e:
                    app_config.LogUtil().error(
                        message=f'{self.__module__}.{self.__class__.__name__}.fetch_value Error: {e}'
                    )
                    return ''
                data_sample: Proficy.Historian.UserAPI.IHU_DATA_SAMPLE = self.__ihuDataSample()
                _time_result: tuple = self.__ihuApi.IHU_TIMESTAMP_FromParts(_dt_time)
                _time_err_code: Proficy.Historian.UserAPI.ihuErrorCode = _time_result[0]
                _timestamp: Proficy.Historian.UserAPI.IHU_TIMESTAMP = _time_result[1]
                data_sample.Tagname = _tag_name
                data_sample.TimeStamp = _timestamp
                data_sample.ValueDataType = _switch_type.get(_type, self.__ihuDataEnum.Integer)
                if re.fullmatch(r'^-?\d+(\.\d+)?$', str(_val)):
                    _float_val = float(_val)
                else:
                    _float_val = _val
                data_sample.ValueObject = _switch_object.get(_type, self.__c_convert.ToSingle)(_float_val)
                _raw_quality: Proficy.Historian.UserAPI.IHU_RAW_QUALITY = self.__ihuRawQuality()
                _raw_quality.QualityStatus = self.__ihuQualityEnum.OPCGood
                data_sample.Quality = _raw_quality
                _list_data_sample.append(data_sample)

            _result_err_code = self.__ihuApi.ihuWriteData(
                self.__server, _list_data_sample, _list_err_code, False, False
            )
            _dict_result: dict = {
                'name': _tag_name,
                'value': _result_err_code.ToString()
            }
            _list_result_err_code.append(_dict_result)

        app_config.LogUtil().info(message=f'PutValues: {_list_tag_name}')
        _result_str: str = f'{{"cmd": "putValues", "param": {_list_result_err_code}}}'

        return _result_str

    def data_length(self, param: dict) -> dict:
        _str_values = self.multi_fetch_values(param=param).replace("'", '"')
        _values: dict = json.loads(_str_values)
        _data_length: int = 0
        _result_param: list = _values.get('param')
        if _result_param not in self.__list_none_value:
            for _tag in _result_param:
                _data_length = _data_length + (len(_tag.get('values', [])))
        return {
            'data_length': _data_length,
            'result': _values
        }


class BizNexus:
    """ 비즈넥서스 통신 클래스 """
    _instance = None
    __inited: bool = False
    __list_none_value: list = None
    __period: str = ''
    __ip: str = None
    __port: int = None
    __tagapi: ctypes.WinDLL = None
    __is_connected: int = None
    __tag_step: int = 10
    __buffer_size: int = 100 * 1024 * 1024

    """
    def __new__(cls, *args, **kwargs) -> _instance:
        if cls._instance is None:
            cls._instance = super(BizNexus, cls).__new__(cls)
        return cls._instance
    """

    def __init__(self) -> None:
        """if not self.__inited:"""
        self.__inited = True
        self.__list_none_value = app_config.ConfigUtil().get_none_values()
        self.__period = app_config.ConfigUtil().get_configs().get('CONFIG').get('PERIOD')
        _config_biznexus: dict = app_config.ConfigUtil().get_configs().get('BIZNEXUS')
        self.__ip = str(f'{_config_biznexus.get('IP')}')
        self.__port = int(f'{_config_biznexus.get('PORT')}')
        if getattr(sys, 'frozen', False):
            _this_path: str = os.path.dirname(sys.executable)
        else:
            _this_path: str = os.path.dirname(os.path.abspath(__file__))
        _dll_tagapi: str = 'TagAPI64.dll'
        # self.__tagapi = ctypes.WinDLL(os.path.join(_this_path, 'dll', _dll_tagapi))
        self.__tagapi = ctypes.CDLL(os.path.join(_this_path, 'dll', _dll_tagapi))
        self.__tagapi.TagConnect._argtypes_ = [ctypes.c_char_p, ctypes.c_char_p, ctypes.c_uint16]
        self.__tagapi.TagConnect.restype = ctypes.c_ulong

        self.__tagapi.TagDisconnect.restype = ctypes.c_ulong

        self.__tagapi.TagExec._argtypes_ = [ctypes.c_char_p]
        self.__tagapi.TagExec.restype = ctypes.c_char_p
        self.__connect()

    def __connect(self) -> int:
        """ 비즈넥서스 연결 """
        self.__is_connected = self.__tagapi.TagConnect(self.__ip.encode('utf-8'), ''.encode('utf-8'), self.__port)
        if not self.__is_connected:
            self.__is_connected = self.__tagapi.TagConnect(self.__ip.encode('utf-8'), ''.encode('utf-8'), self.__port)
        return self.__is_connected

    def close(self) -> int:
        """ 비즈넥서스 종료 """
        _disconnected: int = 1
        if self.__is_connected == 0:
            _disconnected = self.__tagapi.TagDisconnect()
        return _disconnected

    def exec(self, str_cmd: str, is_once: bool = False) -> str:
        """ 비즈넥서스 명령 실행 """
        _result: bytes = self.__tagapi.TagExec(str_cmd.encode('utf-8'))
        _str_result: str = ''
        try:
            _str_result = ctypes.string_at(_result).decode('utf-8')
            """
            _buffer = ctypes.create_string_buffer(self.__buffer_size)
            ctypes.memmove(_buffer, _result, self.__buffer_size)
            _str_result = _buffer.value.decode('utf-8')
            """
        except Exception as e1:
            app_config.LogUtil().error(
                message=f'{self.__module__}.{self.__class__.__name__}.exec Error1: {e1}'
            )
            try:
                _str_result = ctypes.string_at(_result).decode('cp949')
                """
                _buffer = ctypes.create_string_buffer(self.__buffer_size)
                ctypes.memmove(_buffer, _result, self.__buffer_size)
                _str_result = _buffer.value.decode('cp949')
                """
            except Exception as e2:
                app_config.LogUtil().error(
                    message=f'{self.__module__}.{self.__class__.__name__}.exec Error2: {e2}'
                )
                _str_result = ctypes.string_at(_result).decode('utf-8', errors='replace')
                """
                _buffer = ctypes.create_string_buffer(self.__buffer_size)
                ctypes.memmove(_buffer, _result, self.__buffer_size)
                _str_result = _buffer.value.decode('utf-8', errors='replace')
                """
        if 'error' in _str_result and is_once == False:
            self.close()
            time.sleep(1.5)
            self.__connect()
            time.sleep(1.5)
            _str_result = self.exec(str_cmd, True)
        return _str_result

    def data_length(self, dict_receive: dict) -> dict:
        """
        if dict_receive.get('cmd', '') != 'fetchValues':
            dict_receive['cmd'] = 'fetchValues'
        """
        # TODO: SPAN 여부에 따라 변경이 있어서 필요할지 고민중
        if (dict_receive.get('cmd', '') in self.__list_none_value or
                (dict_receive.get('cmd') != 'fetchValues'
                    and dict_receive.get('cmd') != 'fetchSnapshots'
                    and dict_receive.get('cmd') != 'putValues'
                )
        ):
            dict_receive['cmd'] = 'fetchValues'

        _str_start: str = dict_receive.get('param').get('start', '')
        _str_end: str = dict_receive.get('param').get('end', '')
        _span: str = str(dict_receive.get('param').get('span', ''))
        _tag_list: list = deepcopy(dict_receive.get('param').get('tagList'))

        if self.__period.lower().startswith('m'):
            _add_time: timedelta = timedelta(minutes=1)
        elif self.__period.lower().startswith('h'):
            _add_time: timedelta = timedelta(hours=1)
        else:
            _add_time: timedelta = timedelta(seconds=1)

        dict_receive['param']['start'] = datetime.strftime(
            datetime.strptime(_str_start, '%Y-%m-%d %H:%M:%S') - _add_time, '%Y-%m-%d %H:%M:%S')
        dict_receive['param']['end'] = datetime.strftime(datetime.strptime(_str_end, '%Y-%m-%d %H:%M:%S') + _add_time,
                                                         '%Y-%m-%d %H:%M:%S')
        if _span not in self.__list_none_value:
            dict_receive['param']['span'] = int(_span)
            dict_receive['cmd'] = 'fetchSnapshots'

        _temp_dict_receive: dict = deepcopy(dict_receive)
        _values: dict = {}
        for _idx in range(0, len(_tag_list), self.__tag_step):
            _temp_dict_receive['param']['tagList'] = _tag_list[_idx:_idx + self.__tag_step]

            _str_dict_receive: str = json.dumps(_temp_dict_receive, ensure_ascii=False).replace("'", '"')

            app_config.LogUtil().info(message=f'BizNexus exec: {_str_dict_receive}')
            _exec_data: str = self.exec(_str_dict_receive)
            _temp_values: dict = json.loads(_exec_data.replace("'", '"'))
            if len(_values) > 0:
                _values_param: list = _temp_values.get('param')
                _values['param'].extend(_values_param)
            else:
                _values: dict = json.loads(_exec_data.replace("'", '"'))

        _data_length: int = 0
        _result_param: list = _values.get('param')
        if _result_param not in self.__list_none_value:
            for _tags in _result_param:
                _delete_idx: list = []
                for _idx, _dict_tag_value in enumerate(_tags.get('values', [])):
                    _time: str = _dict_tag_value.get('time', '')
                    if _time not in self.__list_none_value and (
                            _time.replace('.000', '') == dict_receive.get('param').get('start') or
                            _time.replace('.000', '') == dict_receive.get(
                        'param').get('end')
                    ):
                        _delete_idx.append(_idx)
                    else:
                        _val: str or int or float = _dict_tag_value.get('val', '')
                        if _val not in self.__list_none_value and _val != 'ERR':
                            _data_length: int = _data_length + 1
                if len(_delete_idx) > 0:
                    for _idx in reversed(_delete_idx):
                        del _tags.get('values')[_idx]
        return {
            'data_length': _data_length,
            'result': _values
        }


class Socket:
    """ 소켓통신 클래스 """
    _instance = None
    __inited: bool = False
    __list_none_value: list = None
    __ip: str = None
    __port: int = None
    __buffer_size: int = None
    __timeout: int = None
    __socket_thread: threading.Thread = None
    __period: str = ''
    __tag_step: int = 10
    __import_success_count: int = -1
    __import_tag_count: int = -1

    def __new__(cls, *args, **kwargs) -> _instance:
        if cls._instance is None:
            cls._instance = super(Socket, cls).__new__(cls)
        return cls._instance

    def __init__(self) -> None:
        if not self.__inited:
            self.__inited = True
            try:
                self.__list_none_value = app_config.ConfigUtil().get_none_values()
                _config_socket: dict = app_config.ConfigUtil().get_configs().get('SOCKET')
                self.__ip = str(f'{_config_socket.get("IP")}')
                self.__port = int(f'{_config_socket.get("PORT")}')
                _buffer_size: str = str(f'{_config_socket.get("BUFFER_SIZE")}')
                self.__timeout: float = float(f'{_config_socket.get("TIMEOUT")}')
                _var_pattern: str = r'^[0-9 *]*[0-9 ]$'
                if _buffer_size is None or not re.fullmatch(_var_pattern, _buffer_size):
                    self.__buffer_size = 10 * 1024 * 1024
                else:
                    self.__buffer_size = eval(_buffer_size)
                    if self.__buffer_size <= 1024:
                        self.__buffer_size = 10 * 1024 * 1024
                self.__period = app_config.ConfigUtil().get_configs().get('CONFIG').get('PERIOD')
            except Exception as e:
                app_config.LogUtil().error(
                    message=f'{self.__module__}.{self.__class__.__name__}.init Error: {e}'
                )
                sys.exit(1)

    def start_server(self) -> None:
        """ 소켓 서버 쓰레드 실행 """
        self.__socket_thread = threading.Thread(target=self.__start_server)
        self.__socket_thread.start()

    def __start_server_async(self) -> None:
        """ 소켓 서버 실행(비동기식) """
        asyncio.run(self.__async_start_server())

    async def __async_start_server(self) -> None:
        """ 소켓 서버 실제 실행(비동기식) """

        async def __handle_client(reader, writer) -> None:
            _addr_: tuple = writer.get_extra_info('peername')
            app_config.LogUtil().info(message=f'Server Connect to {_addr_}')

            while True:
                _receive_data: bytes = await reader.read(self.__buffer_size)
                if not _receive_data:
                    break
                _str_receive: str = _receive_data.decode(encoding='utf-8')
                app_config.LogUtil().info(message=f'Receive Data: {_str_receive}')
                _send_data: bytes = self.__data_process_logic(receive_data=_str_receive)
                writer.write(_send_data)
                await writer.drain()

            writer.close()

        _server = await asyncio.start_server(__handle_client, self.__ip, self.__port)
        _addr = _server.sockets[0].getsockname()
        app_config.LogUtil().info(message=f'Server Start: {_addr}')

        async with _server:
            await _server.serve_forever()

    def __start_server(self) -> None:
        """ 소켓 서버 실제 실행 """
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as _server_socket:
            _server_socket.bind((self.__ip, self.__port))
            _server_socket.listen()
            app_config.LogUtil().info(message=f'Server Start: {self.__ip}:{self.__port}')

            """
            while True:
                _client_socket, _addr = _server_socket.accept()
                app_config.LogUtil().info(message=f'Server Connect to {_addr}')
                _receive_data: bytes = _client_socket.recv(self.__buffer_size)
                _str_receive: str = _receive_data.decode(encoding='utf-8')
                app_config.LogUtil().info(message=f'Receive Data: {_str_receive}')
                _process_thread = threading.Thread(target=self.__data_process, args=(_str_receive, _client_socket))
                _process_thread.start()
            """

            while True:
                _client_socket, _addr = _server_socket.accept()
                _client_socket.settimeout(self.__timeout)
                try:
                    app_config.LogUtil().info(message=f'Server Connect to {_addr}')
                    _receive_data: bytes = b''
                    """_receive_start_time: float = time.time()"""
                    while True:
                        _receive_chunk: bytes = _client_socket.recv(self.__buffer_size)
                        if not _receive_chunk:
                            break
                        _receive_data += _receive_chunk
                        if b'\n' in _receive_data:
                            break
                        """
                        _receive_end_time: float = time.time()
                        if _receive_end_time - _receive_start_time > (self.__timeout - 1.0):
                            app_config.LogUtil().info(message=f'Timeout')
                            break
                        """

                    _str_receive: str = _receive_data.decode(encoding='utf-8').rstrip('\n')
                    app_config.LogUtil().info(message=f'Receive Data: {_str_receive}')
                    _process_thread = threading.Thread(target=self.__data_process, args=(_str_receive, _client_socket))
                    _process_thread.start()
                except TimeoutError as e:
                    _client_socket.sendall(b'{"cmd":"error","param":"timeout"}')
                    _client_socket.close()
                    app_config.LogUtil().error(
                        message=f'{self.__module__}.{self.__class__.__name__}.start_server Error: {e}'
                    )
                    return

    def __data_process(self, receive_data: str, client_socket: socket.socket) -> None:
        """ 받은 데이터 처리 및 리턴 데이터 """
        if type(receive_data) == bytes:
            receive_data = receive_data.encode('utf-8')
        try:
            _result = self.__data_process_logic(receive_data=receive_data)
            client_socket.sendall(_result)
        except Exception as e:
            traceback.print_exc()
            app_config.LogUtil().error(
                message=f'{self.__module__}.{self.__class__.__name__}.data_process Error: {e}'
            )
        finally:
            client_socket.close()

    def __data_process_logic(self, receive_data: str) -> bytes:
        """ 받은 데이터 처리 및 리턴 데이터 로직 """
        # TODO: STATS 속도 개선, import의 시간도 쪼개서 반복문?
        _result: str = '{"cmd": "Failed"}'
        _byte_result = b''
        try:
            _dict_receive: dict = json.loads(receive_data)
            _cmd: str = _dict_receive.get('cmd')
            _dest: str = _dict_receive.get('dest')
            _param: str or dict = _dict_receive.get('param')
            if _dest is not None and _dest.lower() == 'biznexus':
                _biz_nexus: BizNexus = BizNexus()
                if _cmd == 'stats':
                    _total_length: int = self.__create_stats_init_data(_param)
                    _biznexus_data_length: int = _biz_nexus.data_length(_dict_receive).get('data_length', 0)
                    _result = json.dumps({
                        'cmd': _cmd,
                        'param': [{
                            'name': 'historian',
                            'totalLength': _total_length,
                            'dataLength': _biznexus_data_length,
                            'missLength': (_total_length - _biznexus_data_length)
                        }]
                    }, ensure_ascii=False).replace("'", '"')
                elif _cmd == 'fetchValues':
                    _result = json.dumps(_biz_nexus.data_length(_dict_receive).get('result', {}), ensure_ascii=False).replace("'", '"')
                elif _cmd == 'putValues':
                    # TODO: putValues 문제
                    _result = _biz_nexus.exec(receive_data).replace("'", '"')
                elif _cmd == 'importData':
                    if self.__import_tag_count < 0:
                        _biz_nexus.close()
                        _import_thread = threading.Thread(
                            target=self.__historian_to_biznexus_import,
                            args=(_dict_receive,)
                        )
                        _import_thread.start()
                    _result = json.dumps({
                        'cmd': _cmd,
                        'param': [{
                            'status': 'Processing'
                        }]
                    }, ensure_ascii=False).replace("'", '"')
                else:
                    _result = _biz_nexus.exec(receive_data).replace("'", '"')
                _biz_nexus.close()
            elif _dest is not None and _dest == 'importStatus':
                _result = json.dumps({
                    'cmd': _cmd,
                    'param': [{
                        'total': self.__import_tag_count,
                        'success': self.__import_success_count
                    }]
                }, ensure_ascii=False).replace("'", '"')
            elif _dest is not None and _dest.lower() == 'stats':
                _total_length: int = self.__create_stats_init_data(_param)
                _historian_data: dict = Historian().data_length(_param)
                _historian_data_length: int = _historian_data.get('data_length', 0)
                _historian_data_param: list = _historian_data.get('result', {}).get('param', [])
                _biz_nexus: BizNexus = BizNexus()
                _biznexus_data: dict = _biz_nexus.data_length(_dict_receive)
                _biznexus_data_length: int = _biznexus_data.get('data_length', 0)
                _biznexus_data_param: list = _biznexus_data.get('result', {}).get('param', [])
                _biz_nexus.close()

                _same_data_count: int = 0
                _dict_tags: dict = {}
                for _his_dict_tag in _historian_data_param:
                    _tag_name: str = _his_dict_tag.get('name', '')
                    if _tag_name not in self.__list_none_value:
                        if _dict_tags.get(_tag_name) in self.__list_none_value:
                            _dict_tags[_tag_name] = {}
                        _list_time_val_values = _his_dict_tag.get('values')
                        if _list_time_val_values not in self.__list_none_value:
                            for _time_val_value in _list_time_val_values:
                                _time_value = _time_val_value.get('time', '')
                                if _time_value not in self.__list_none_value:
                                    if _dict_tags.get(_tag_name) not in self.__list_none_value and \
                                            _dict_tags.get(_tag_name).get(_time_value) in self.__list_none_value:
                                        _dict_tags[_tag_name][_time_value] = _time_val_value.get('val', '')
                for _biz_dict_tag in _biznexus_data_param:
                    _tag_name: str = _biz_dict_tag.get('name', '')
                    if _tag_name not in self.__list_none_value:
                        _dict_time_val = _dict_tags.get(_tag_name)
                        if _dict_time_val not in self.__list_none_value:
                            _list_time_val_values = _biz_dict_tag.get('values')
                            if _list_time_val_values not in self.__list_none_value:
                                for _time_val_value in _list_time_val_values:
                                    _time_value = _time_val_value.get('time', '')
                                    if _time_value not in self.__list_none_value:
                                        _time_value = _time_value.replace('.000', '')
                                        _val_value = _time_val_value.get('val', '')
                                        if _val_value not in self.__list_none_value and _dict_time_val.get(
                                                _time_value) not in self.__list_none_value:
                                            _same_data_count = _same_data_count + 1 if float(_val_value) == float(
                                                _dict_time_val.get(_time_value)) else _same_data_count
                _result = json.dumps({
                    'cmd': _cmd,
                    'param': [{
                        'name': 'historian',
                        'totalLength': _total_length,
                        'dataLength': _historian_data_length,
                        'missLength': (_total_length - _historian_data_length),
                        'sameDataLength': _same_data_count,
                        'notSameDataLength': (_total_length - _same_data_count)
                    }, {
                        'name': 'biznexus',
                        'totalLength': _total_length,
                        'dataLength': _biznexus_data_length,
                        'missLength': (_total_length - _biznexus_data_length),
                        'sameDataLength': _same_data_count,
                        'notSameDataLength': (_total_length - _same_data_count)
                    }]
                }, ensure_ascii=False).replace("'", '"')
            else:
                if _cmd == 'reqValues':
                    _result = Historian().req_values(param=_param).replace("'", '"')
                elif _cmd == 'fetchValues':
                    _result = Historian().fetch_values(param=_param).replace("'", '"')
                elif _cmd == 'putValues':
                    _result = Historian().put_values(param=_param).replace("'", '"')
                elif _cmd == 'importData':
                    if self.__import_tag_count < 0:
                        _import_thread = threading.Thread(
                            target=self.__biznexus_to_historian_import,
                            args=(_dict_receive,)
                        )
                        _import_thread.start()
                    _result = json.dumps({
                        'cmd': _cmd,
                        'param': [{
                            'status': 'Processing'
                        }]
                    }, ensure_ascii=False).replace("'", '"')
                elif _cmd == 'stats':
                    _total_length: int = self.__create_stats_init_data(_param)
                    _historian_data_length: int = Historian().data_length(_param).get('data_length', 0)
                    _result = json.dumps({
                        'cmd': _cmd,
                        'param': [{
                            'name': 'historian',
                            'totalLength': _total_length,
                            'dataLength': _historian_data_length,
                            'missLength': (_total_length - _historian_data_length)
                        }]
                    }, ensure_ascii=False).replace("'", '"')
            _byte_result = _result.encode('utf-8')
        except Exception as e:
            traceback.print_exc()
            app_config.LogUtil().error(
                message=f'{self.__module__}.{self.__class__.__name__}.data_process_logic Error: {e}'
            )
        return _byte_result

    def __create_stats_init_data(self, param: dict) -> int:
        _database: Database = Database()
        _str_time_format = '%Y-%m-%d %H:%M:%S'
        _now: str = datetime.now().strftime(_str_time_format)
        _start_date: datetime = datetime.strptime(param.get('start', _now), _str_time_format)
        _end_date: datetime = datetime.strptime(param.get('end', _now), _str_time_format)
        _float_total_length: float = (_end_date - _start_date).total_seconds()
        _total_length: int = 0
        if self.__period.lower().startswith('m'):
            _total_length = int(_float_total_length / 60)
        elif self.__period.lower().startswith('h'):
            _total_length = int(_float_total_length / 60 / 60)
        else:
            _total_length = int(_float_total_length)
        _total_length = _total_length + 1
        _str_tag_list: str or list[str] = param.get('tagList')
        if _str_tag_list in self.__list_none_value:
            _replace_tag_list: list = []
            _tag_list: list = _database.select_tags()
            for _dict_tag in _tag_list:
                _replace_tag_list.append(_dict_tag.get('TAG_NM', ''))
            param['tagList'] = _replace_tag_list
        return _total_length * len(param['tagList'])

    def __historian_to_biznexus_import(self, dict_receive: dict) -> str:
        _database: Database = Database()
        _biz_nexus: BizNexus = BizNexus()
        _dest: str = dict_receive.get('dest', '')
        _param: str or dict = dict_receive.get('param', {})
        _start: str = _param.get('start', '')
        _end: str = _param.get('end', '')
        _str_tag_list: str or list[str] = _param.get('tagList', '')
        _time_format: str = '%Y-%m-%d %H:%M:%S'
        _dict_tag_sn = {}
        if _str_tag_list in self.__list_none_value:
            _replace_tag_list: list = []
            _tag_list: list = _database.select_tags()
            for _dict_tag in _tag_list:
                _replace_tag_list.append(_dict_tag.get('TAG_NM', ''))
                _dict_tag_sn[_dict_tag.get('TAG_NM', '')] = _dict_tag.get('TAG_SN', '')
            _param['tagList'] = _replace_tag_list
        self.__import_tag_count = len(_param.get('tagList'))
        self.__import_success_count = 0

        try:
            _temp_dict_receive: dict = deepcopy(dict_receive)
            for _idx in range(0, len(_param.get('tagList')), self.__tag_step):
                _temp_dict_receive['param']['tagList'] = _param.get('tagList')[_idx:_idx + self.__tag_step]
                _temp_param: str or dict = _temp_dict_receive.get('param', {})
                _temp_start: str = _temp_param.get('start', '')
                _temp_end: str = _temp_param.get('end', '')

                _his_import_data: dict = json.loads(Historian().multi_fetch_values(param=_temp_param).replace("'", '"'))
                _his_tag_list: list = _his_import_data.get('param', [])

                _biz_import_data: dict = _biz_nexus.data_length(_temp_dict_receive).get('result', {})
                _biz_tag_list: list = _biz_import_data.get('param', [])

                _put_param: list = []
                _dict_value: dict = {}
                for _tag_values in _biz_tag_list:
                    _tag_name: str = _tag_values.get('name', '')
                    if _tag_name not in self.__list_none_value:
                        if _dict_value.get(_tag_name, '') in self.__list_none_value:
                            _dict_value[_tag_name] = {}
                        _value_list: list = _tag_values.get('values', [])
                        for _value in _value_list:
                            _val: str or int or float = _value.get('val', '')
                            if _val not in self.__list_none_value:
                                _time: str = _value.get('time', '').replace('.000', '') + '.000'
                                if _dict_value.get(_tag_name, {}).get(_time) in self.__list_none_value:
                                    _dict_value[_tag_name][_time] = {
                                        'name': _tag_name,
                                        'sn': _dict_tag_sn.get(_tag_name, ''),
                                        'quality': 192,
                                        'time': _time
                                    }
                                    _put_param.append(_dict_value[_tag_name][_time])
                                _dict_value[_tag_name][_time]['old_val'] = _val

                for _tag_values in _his_tag_list:
                    _tag_name: str = _tag_values.get('name', '')
                    if _tag_name not in self.__list_none_value:
                        if _dict_value.get(_tag_name, '') in self.__list_none_value:
                            _dict_value[_tag_name] = {}
                        _value_list: list = _tag_values.get('values', [])
                        for _value in _value_list:
                            _val: str or int or float = _value.get('val', '')
                            if _val not in self.__list_none_value:
                                _time: str = _value.get('time', '').replace('.000', '') + '.000'
                                if _dict_value.get(_tag_name, {}).get(_time) in self.__list_none_value:
                                    _dict_value[_tag_name][_time] = {
                                        'name': _tag_name,
                                        'sn': _dict_tag_sn.get(_tag_name, ''),
                                        'quality': 192,
                                        'time': _time
                                    }
                                    _put_param.append(_dict_value[_tag_name][_time])
                                _dict_value[_tag_name][_time]['val'] = _val

                _put_data: dict = {
                    'cmd': 'putValues',
                    'dest': _dest,
                    'param': _put_param
                }
                _result = _biz_nexus.exec(json.dumps(_put_data, ensure_ascii=False).replace("'", '"')).replace("'", '"')
                _database.insert_revisn_log(_put_param, _dest)
                self.__import_success_count += self.__tag_step
        except Exception as e:
            app_config.LogUtil().error(
                message=f'{self.__module__}.{self.__class__.__name__}.__historian_to_biznexus_import Error: {e}'
            )
        finally:
            self.__import_tag_count = -1
            self.__import_success_count = -1
            _biz_nexus.close()
        return ''

    def __biznexus_to_historian_import(self, dict_receive: dict) -> str:
        _database: Database = Database()
        _biz_nexus: BizNexus = BizNexus()
        _dest: str = dict_receive.get('dest', '')
        _param: str or dict = dict_receive.get('param', {})
        _start: str = _param.get('start', '')
        _end: str = _param.get('end', '')
        _str_tag_list: str or list[str] = _param.get('tagList', '')
        _time_format: str = '%Y-%m-%d %H:%M:%S'
        _dict_tag_sn = {}
        if _str_tag_list in self.__list_none_value:
            _replace_tag_list: list = []
            _tag_list: list = _database.select_tags()
            for _dict_tag in _tag_list:
                _replace_tag_list.append(_dict_tag.get('TAG_NM', ''))
                _dict_tag_sn[_dict_tag.get('TAG_NM', '')] = _dict_tag.get('TAG_SN', '')
            _param['tagList'] = _replace_tag_list
        self.__import_tag_count = len(_param.get('tagList'))
        self.__import_success_count = 0

        try:
            _temp_dict_receive: dict = deepcopy(dict_receive)
            for _idx in range(0, len(_param.get('tagList')), self.__tag_step):
                _temp_dict_receive['param']['tagList'] = _param.get('tagList')[_idx:_idx + self.__tag_step]
                _temp_param: str or dict = _temp_dict_receive.get('param', {})
                _temp_start: str = _temp_param.get('start', '')
                _temp_end: str = _temp_param.get('end', '')

                _his_import_data: dict = json.loads(Historian().multi_fetch_values(param=_temp_param).replace("'", '"'))
                _his_tag_list: list = _his_import_data.get('param', [])

                _biz_import_data: dict = _biz_nexus.data_length(_temp_dict_receive).get('result', {})
                _biz_tag_list: list = _biz_import_data.get('param', [])

                _put_param: list = []
                _dict_value: dict = {}
                for _tag_values in _biz_tag_list:
                    _tag_name: str = _tag_values.get('name', '')
                    if _tag_name not in self.__list_none_value:
                        if _dict_value.get(_tag_name, '') in self.__list_none_value:
                            _dict_value[_tag_name] = {}
                        _value_list: list = _tag_values.get('values', [])
                        for _value in _value_list:
                            _val: str or int or float = _value.get('val', '')
                            if _val not in self.__list_none_value:
                                _time: str = _value.get('time', '').replace('.000', '')
                                if _dict_value.get(_tag_name, {}).get(_time) in self.__list_none_value:
                                    _dict_value[_tag_name][_time] = {
                                        'name': _tag_name,
                                        'sn': _dict_tag_sn.get(_tag_name),
                                        'quality': 192,
                                        'time': _time
                                    }
                                    _put_param.append(_dict_value[_tag_name][_time])
                                _dict_value[_tag_name][_time]['val'] = _val

                for _tag_values in _his_tag_list:
                    _tag_name: str = _tag_values.get('name', '')
                    if _tag_name not in self.__list_none_value:
                        if _dict_value.get(_tag_name, '') in self.__list_none_value:
                            _dict_value[_tag_name] = {}
                        _value_list: list = _tag_values.get('values', [])
                        for _value in _value_list:
                            _val: str or int or float = _value.get('val', '')
                            if _val not in self.__list_none_value:
                                _time: str = _value.get('time', '').replace('.000', '')
                                if _dict_value.get(_tag_name, {}).get(_time) in self.__list_none_value:
                                    _dict_value[_tag_name][_time] = {
                                        'name': _tag_name,
                                        'sn': _dict_tag_sn.get(_tag_name),
                                        'quality': 192,
                                        'time': _time
                                    }
                                    _put_param.append(_dict_value[_tag_name][_time])
                                _dict_value[_tag_name][_time]['old_val'] = _val

                _put_data: dict = {
                    'cmd': 'putValues',
                    'dest': _dest,
                    'param': _put_param
                }
                Historian().put_values(param=_put_param)
                _database.insert_revisn_log(_put_param, _dest)
                self.__import_success_count += self.__tag_step
        except Exception as e:
            app_config.LogUtil().error(
                message=f'{self.__module__}.{self.__class__.__name__}.__biznexus_to_historian_import Error: {e}'
            )
        finally:
            self.__import_tag_count = -1
            self.__import_success_count = -1
            _biz_nexus.close()
        return ''
