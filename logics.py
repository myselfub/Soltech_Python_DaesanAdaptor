import asyncio
import json
import re
import traceback
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from copy import deepcopy

import app_config_
import communication_


class Logics:
    """ 로직관련 클래스 """
    __scheduler_thread: threading.Thread = None
    __dict_keyword: dict = {}
    __dict_data: dict = {}
    __list_none_value: list = None
    __list_forbid_word: list = ['sys.exit', 'import ']
    __thread_executor: ThreadPoolExecutor = None
    __max_hour: int = 24
    __period: str = ''

    def __init__(self, dict_data: dict = None) -> None:
        if dict_data is None:
            dict_data = {}
        self.__dict_data = dict_data
        self.__list_none_value = app_config_.ConfigUtil().get_none_values()
        self.__dict_keyword = {
            'result': None,
            '__dict_data': self.__dict_data,
            '${DATA_LIST}': '__dict_data',
            '${FRONT_FILL_VALUE}': 'result = _fill_empty(is_reverse=False)',
            '${END_FILL_VALUE}': 'result = _fill_empty(is_reverse=True)',
            '${FRONT_FILL_ALL_VALUE}': 'result = _fill_all_empty(_find_empty_pre_value(result_count=1, is_reverse=False)[0])',
            '${END_FILL_ALL_VALUE}': 'result = _fill_all_empty(_find_empty_pre_value(result_count=1, is_reverse=True)[0])',
            '${LINEAR_INTERPOLATION}': 'result = _linear_interpolation()',
            '_find_empty_pre_value': self.__find_empty_pre_value,
            '_fill_all_empty': self.__fill_all_empty,
            '_fill_empty': self._fill_empty,
            '_linear_interpolation': self._linear_interpolation
        }
        self.__period = app_config_.ConfigUtil().get_configs().get('CONFIG').get('PERIOD')
        self.__thread_executor = ThreadPoolExecutor(max_workers=5)

    def revisn_data(self, dict_cmd: dict, list_revisn_tag: list = None) -> dict:
        _database: communication_.Database = communication_.Database()
        _result_dict = {
            'cmd': 'autoLogic',
            'tagList': []
        }
        if list_revisn_tag is None:
            list_revisn_tag: list = _database.select_revisn_data()
        if dict_cmd.get('param') is None or dict_cmd.get('param') == '':
            return _result_dict
        _start = dict_cmd.get('param').get('start')
        _end = dict_cmd.get('param').get('end')
        _auto = dict_cmd.get('param').get('auto')
        _str_fetch_result: str = communication_.Historian().fetch_values(param=dict_cmd.get('param'))
        if _str_fetch_result in self.__list_none_value:
            app_config_.LogUtil().error(
                message=f'{self.__module__}.{self.__class__.__name__}.revisn_data Error: FetchValue Is None.'
            )
            return _result_dict
        _str_fetch_result = _str_fetch_result.replace("'", '"')
        _dict_fetch_result: dict = json.loads(_str_fetch_result)
        if _dict_fetch_result.get('param') in self.__list_none_value or len(_dict_fetch_result.get('param')) == 0:
            app_config_.LogUtil().error(
                message=f'{self.__module__}.{self.__class__.__name__}.revisn_data Error: FetchValue Is None.'
            )
            return _result_dict
        _dict_datas: dict = self.search_empty_data(
            list_tag=_dict_fetch_result.get('param'),
            str_start=_start,
            str_end=_end,
            period=self.__period
        )
        self.__dict_data = _dict_datas
        _list_tag:list = []
        _list_param: list = []
        for _dict_revisn_tag in list_revisn_tag:
            _tag_nm: str = _dict_revisn_tag.get('TAG_NM')
            _tag_sn: str = str(int(_dict_revisn_tag.get('TAG_SN')))
            _rule_nm: str = _dict_revisn_tag.get('RULE_NM')
            _org_datas: dict = _dict_datas.get(_tag_nm)
            if _org_datas is not None:
                _revisn_result: dict = asyncio.run(self.execute_script(_dict_revisn_tag))
                if _revisn_result is None or len(_revisn_result) == 0:
                    continue
                _list_values: list = []
                for _key in _org_datas.keys():
                    if _org_datas.get(_key) != _revisn_result.get(_tag_nm, {}).get(_key):
                        _dict_values = {
                            'name': _tag_nm,
                            'quality': 192,
                            'time': _key + '.000',
                            'val': _revisn_result.get(_tag_nm, {}).get(_key),
                            'old': _org_datas.get(_key)
                        }
                        _list_param.append(_dict_values)
                        _list_values.append(_dict_values)
                _dict_tag_list = {
                    'name': _tag_nm,
                    'sn': _tag_sn,
                    'rule': _rule_nm,
                    'values': _list_values,
                    'auto': _auto
                }
                _list_tag.append(_dict_tag_list)
        _dict_tag: dict = {
            'cmd': 'autoLogic',
            'tagList': _list_tag
        }
        _result_dict['tagList'] = _list_param
        if _auto.lower() == 'false':
            return _result_dict
        if _list_param is not None and len(_list_param) > 0:
            _biznexus: communication_.BizNexus = communication_.BizNexus()
            _biznexus_cmd = {
                'cmd': 'putValues',
                'param': _list_param
            }
            _str_biznexus_cmd = json.dumps(_biznexus_cmd, ensure_ascii=False)
            _str_biznexus_cmd = _str_biznexus_cmd.replace("'", '"')
            _str_result = _biznexus.exec(_str_biznexus_cmd)
            _result = json.loads(_str_result)
            if len(_result) > 0:
                _result_insert = _database.insert_revisn_log(_dict_tag)
                #_result_dict['tagList'] = _result_insert
        return _result_dict

    def revisn_data_(self, dict_cmd: dict, list_revisn_tag: list = None) -> dict:
        _database: communication_.Database = communication_.Database()
        _result_dict = {
            'cmd': 'autoLogic',
            'tagList': []
        }
        if list_revisn_tag is None:
            list_revisn_tag: list = _database.select_revisn_data()
        if dict_cmd.get('param') is None or dict_cmd.get('param') == '':
            return _result_dict
        _start = dict_cmd.get('param').get('start')
        _end = dict_cmd.get('param').get('end')
        _auto = dict_cmd.get('param').get('auto')
        _str_fetch_result: str = communication_.Historian().fetch_values(param=dict_cmd.get('param'))
        if _str_fetch_result in self.__list_none_value:
            app_config_.LogUtil().error(
                message=f'{self.__module__}.{self.__class__.__name__}.revisn_data Error: FetchValue Is None.'
            )
            return _result_dict
        _str_fetch_result = _str_fetch_result.replace("'", '"')
        _dict_fetch_result: dict = json.loads(_str_fetch_result)
        if _dict_fetch_result.get('param') in self.__list_none_value or len(_dict_fetch_result.get('param')) == 0:
            app_config_.LogUtil().error(
                message=f'{self.__module__}.{self.__class__.__name__}.revisn_data Error: FetchValue Is None.'
            )
            return _result_dict
        _dict_datas: dict = self.search_empty_data(
            list_tag=_dict_fetch_result.get('param'),
            str_start=_start,
            str_end=_end,
            period=self.__period
        )
        self.__dict_data = _dict_datas
        _list_tag: list = []
        for _dict_revisn_tag in list_revisn_tag:
            _tag_nm: str = _dict_revisn_tag.get('TAG_NM')
            _tag_sn: str = str(int(_dict_revisn_tag.get('TAG_SN')))
            _rule_nm: str = _dict_revisn_tag.get('RULE_NM')
            _org_datas: dict = _dict_datas.get(_tag_nm)
            if _org_datas is not None:
                _revisn_result: dict = asyncio.run(self.execute_script(_dict_revisn_tag))
                if _revisn_result is None or len(_revisn_result) == 0:
                    continue
                _list_values: list = []
                for _key in _org_datas.keys():
                    if _org_datas.get(_key) != _revisn_result.get(_tag_nm, {}).get(_key):
                        _dict_values = {
                            'time': _key,
                            'val': _revisn_result.get(_tag_nm, {}).get(_key),
                            'old': _org_datas.get(_key),
                            'type': 'float'
                        }
                        _list_values.append(_dict_values)
                _dict_tag_list: dict = {
                    'name': _tag_nm,
                    'sn': _tag_sn,
                    'rule': _rule_nm,
                    'values': _list_values,
                    'auto': _auto
                }
                _list_tag.append(_dict_tag_list)
        _dict_tag: dict = {
            'cmd': 'autoLogic',
            'tagList': _list_tag
        }
        if _auto.lower() == 'false':
            return _dict_tag
        if _list_tag is not None and len(_list_tag) > 0:
            _str_write_result: str = communication_.Historian().put_values(param=_dict_tag)
            _str_write_result = _str_write_result.replace("'", '"')
            _dict_write_result: dict = json.loads(_str_write_result)
            if len(_dict_write_result.get('param')) > 0:
                _result_insert = _database.insert_revisn_log(_dict_tag)
                _result_dict['tagList'] = _result_insert
        return _result_dict

    def set_var(self, str_var_dict: str = None) -> None:
        """ keyword_dict에 파라미터 추가
        :param str_var_dict: (str) 추가할 dict형태의 str
        :return: None
        """
        if str_var_dict in self.__list_none_value:
            return
        _dict_var: dict = json.loads(str_var_dict)
        for _key in _dict_var.keys():
            if _key not in self.__dict_keyword.keys():
                self.__dict_keyword[_key] = _dict_var.get(_key)

    def search_empty_data(self, list_tag: list[dict], str_start: str, str_end: str, period: str = 's') -> dict:
        """ 시작과 종료 시간 사이의 빈값을 None으로 채움
        :param list_tag:
        :param str_start:
        :param str_end:
        :param period:
        :return:
        """
        _dict_tags_result: dict = {}
        _dict_dates: dict = {}
        _str_time_format = '%Y-%m-%d %H:%M:%S'
        # 데이터 interval 설정
        if period.lower().startswith('m'):
            _add_time: timedelta = timedelta(minutes=1)
            if not str_start.endswith(':00'):
                str_start = str_start[:-2] + '00'
            if not str_end.endswith(':00'):
                str_end = str_end[:-2] + '00'
        elif period.lower().startswith('h'):
            _add_time: timedelta = timedelta(hours=1)
            if not str_start.endswith(':00:00'):
                str_start = str_start[:-5] + '00:00'
            if not str_end.endswith(':00:00'):
                str_end = str_end[:-5] + '00:00'
        else:
            _add_time: timedelta = timedelta(seconds=1)

        _dt_start: datetime = datetime.strptime(str_start, _str_time_format)
        _dt_end: datetime = datetime.strptime(str_end, _str_time_format)
        _dt_dif_hour: int = (_dt_end - _dt_start).seconds // 3600
        if _dt_dif_hour > self.__max_hour:
            app_config_.LogUtil().info(
                message=f'{self.__module__}.{self.__class__.__name__}.search_empty_data Info: Stopped because trying {_dt_dif_hour}hours difference.'
            )
            return _dict_tags_result
        _dict_dates[_dt_start.strftime(_str_time_format)] = None
        while _dt_start != _dt_end:
            _dt_start += _add_time
            _dict_dates[_dt_start.strftime(_str_time_format)] = None
        for _dict_tag in list_tag:
            _tag_name: str = _dict_tag.get('name')
            _dict_tags_result[_tag_name] = deepcopy(_dict_dates)
            _list_tag_values: list = _dict_tag.get('values')
            # _tag_values_sorted: list = sorted(_tag_values, key=lambda item: datetime.strptime(item['time'], _str_time_format))
            for _tag_value in _list_tag_values:
                if _dict_tags_result.get(_tag_name).get(_tag_value.get('time'), 'NULL') != 'NULL':
                    _dict_tags_result[_tag_name][_tag_value.get('time')] = _tag_value.get('val')

        return _dict_tags_result

    def execute_script_old(self, dict_revisn: dict) -> __dict_keyword:
        """ Script 호출
        :param dict_revisn:
        :return:
        """
        _results: list or dict = []
        _futures: list = [self.__thread_executor.submit(self.__execute_script, dict_revisn)]
        try:
            for _future in as_completed(_futures):
                _results.append(_future.result())
        except Exception as e:
            app_config_.LogUtil().error(
                message=f'{self.__module__}.{self.__class__.__name__}.execute_script Error: {e}'
            )
        finally:
            self.__thread_executor.shutdown(wait=True)
        return _results

    async def execute_script(self, dict_revisn: dict) -> __dict_keyword:
        """ Script 호출
        :param dict_revisn:
        :return:
        """
        _results: list or dict = await self.__execute_script(dict_revisn=dict_revisn)
        return _results

    async def __execute_script(self, dict_revisn: dict) -> __dict_keyword:
        """ Script 호출
        :param dict_revisn : (dict) 호출할 스크립트 메소드(keyword_dict에 키워드가 존재하면 해당 로직을 불러옴)
        :return: self.__keyword_dict (dict): 키워드 dict
        """
        _tag_nm: str = dict_revisn.get('TAG_NM')
        _rule_nm: str = dict_revisn.get('RULE_NM')
        _rule_cn: str = dict_revisn.get('RULE_CN')

        self.__dict_keyword['result'] = None
        _list_var_data: str = '${DATA_LIST}'
        if _rule_cn.find(_list_var_data):
            _rule_cn = _rule_cn.replace(_list_var_data, self.__dict_keyword.get(_list_var_data))
        for _word in self.__list_forbid_word:
            if _rule_cn.find(_word):
                _rule_cn = _rule_cn.replace(_word, '')

        _var_pattern: str = r'^\$\{\w+\}$'
        _exec: str = ''
        if re.match(_var_pattern, _rule_cn):
            _exec = self.__dict_keyword.get(_rule_cn)
        else:
            _exec = _rule_cn

        _var_pattern: str = r'\$\{\w+\}'
        for _key in self.__dict_keyword.keys():
            if type(self.__dict_keyword.get(_key)) == str:
                _match: re.Match = re.search(_var_pattern, self.__dict_keyword.get(_key))
                if _match and type(self.__dict_keyword.get(_match.group())) == str:
                    _str_var: str = self.__dict_keyword.get(_key).replace(
                        _match.group(), self.__dict_keyword.get(_match.group())
                    )
                    self.__dict_keyword[_key] = _str_var

        try:
            exec(_exec, self.__dict_keyword, self.__dict_keyword)
        except Exception as e:
            app_config_.LogUtil().error(
                message=f'{self.__module__}.{self.__class__.__name__}.execute_script Error: {e}'
            )
            return
        _rule_cn = _rule_cn.replace('\r\n', '\n')
        app_config_.LogUtil().info(message=f'TAG: {_tag_nm}, Rule: {_rule_nm}, Execute Script: "{_rule_cn}"')

        return self.__dict_keyword.get('result')

    def __find_empty_pre_value(self, result_count: int = 1, is_reverse: bool = False) -> dict:
        """ 처음으로 나오는 빈값의 전/후 값을 찾음
        :param result_count: (int) 가져올 개수
        :param is_reverse: (bool) 빈값 전/후 여부
        :return: (list) 빈값 전/후의 count만큼의 키값
        """
        _dict_data: dict = deepcopy(self.__dict_data)
        _dict_key_result: dict = {}
        for _key_data in _dict_data.keys():
            _list_key_result: list = []
            _dict_data_dt = _dict_data.get(_key_data)
            _idx: int = 0
            if is_reverse:
                # _rang: range = range(len(_dict_data_dt) - 1, -1, -1)
                for _idx, _key in reversed(list(enumerate(_dict_data_dt.keys()))):
                    if _dict_data_dt.get(_key) in self.__list_none_value:
                        break
            else:
                # _rang: range = range(0, len(_dict_data_dt))
                for _idx, _key in enumerate(_dict_data_dt.keys()):
                    if _dict_data_dt.get(_key) in self.__list_none_value:
                        break
            if is_reverse:
                _idx = _idx + 1
                if _idx + result_count > len(_dict_data_dt):
                    result_count: int = len(_dict_data_dt) - _idx
                for _i, _key in enumerate(_dict_data_dt.keys()):
                    if _i < _idx:
                        continue
                    elif _i >= (_idx + result_count):
                        break
                    elif _i >= _idx and _idx < (_idx + result_count):
                        _list_key_result.append(_key)
            else:
                _idx = _idx - 1
                if _idx - result_count < 0:
                    result_count: int = _idx
                for _i, _key in enumerate(_dict_data_dt.keys()):
                    if _i <= (_idx - result_count):
                        continue
                    elif _i > _idx:
                        break
                    elif _i > (_idx - result_count) and _idx <= _idx:
                        _list_key_result.append(_key)
            _dict_key_result[_key_data] = _list_key_result

        return _dict_key_result

    def __fill_all_empty(self, fill_value: int or str) -> dict:
        """ dict_data의 빈 값들 전체를 특정 값으로 채움
        :param fill_value: (int or str) 채울 값
        :return: (list) 빈값을 채운 dict_data
        """
        _dict_result: dict = deepcopy(self.__dict_data)
        for _key in _dict_result.keys():
            _dict_dt = _dict_result.get(_key)
            for _key_dt in _dict_dt.keys():
                if _dict_dt.get(_dict_dt) in self.__list_none_value:
                    _dict_dt[_dict_dt] = fill_value
        return _dict_result

    def _fill_empty(self, is_reverse: bool = False) -> dict:
        """ dict_data의 빈 값들을 전/후 값으로 채움
        :param is_reverse: (bool) 전/후 여부
        :return: (list) 빈값을 채운 dict_data
        """
        _dict_result: dict = deepcopy(self.__dict_data)
        _list_dict_idx = []
        for _str_dict_key in _dict_result.keys():
            _dict_dt = _dict_result.get(_str_dict_key)
            _list_dict_idx = list(_dict_dt.keys())

            """
            if is_reverse and _dict_dt.get(_list_dict_idx[-1]) in self.__list_none_value:
                app_config_.LogUtil().error(
                    message=f'{self.__module__}.{self.__class__.__name__}.fill_empty Error: Last value is None.'
                )

                return {}
            elif not is_reverse and _dict_dt.get(_list_dict_idx[0]) in self.__list_none_value:
                app_config_.LogUtil().error(
                    message=f'{self.__module__}.{self.__class__.__name__}.fill_empty Error: First value is None.'
                )
                return {}
            """
            if is_reverse:
                for _idx, _key in reversed(list(enumerate(_dict_dt.keys()))):
                    if _idx == len(_list_dict_idx) - 1:
                        continue
                    if _dict_dt.get(_key) in self.__list_none_value:
                        _dict_dt[_key] = _dict_dt.get(_list_dict_idx[_idx + 1])
            else:
                for _idx, _key in enumerate(_dict_dt.keys()):
                    if _idx == 0:
                        continue
                    if _dict_dt.get(_key) in self.__list_none_value:
                        _dict_dt[_key] = _dict_dt.get(_list_dict_idx[_idx - 1])
        return _dict_result

    def _linear_interpolation(self) -> dict:
        """ dict_data의 빈 값들을 선형보간으로 보정
            Returns:
                list: 보정값을 채운 dict_data
        """
        _dict_result: dict = deepcopy(self.__dict_data)
        for _str_dict_key in _dict_result:
            _dict_dt = _dict_result.get(_str_dict_key)
            _list_dict_idx = list(_dict_dt.keys())
            _data_length: int = len(_dict_dt)

            _list_none_data: list = [
                _idx for _idx, _key in enumerate(_dict_dt.keys()) if
                _dict_dt.get(_list_dict_idx[_idx]) in self.__list_none_value
            ]

            for _idx in _list_none_data:
                _left_idx: int = _idx - 1
                _right_idx: int = _idx + 1

                while _left_idx >= 0 and _dict_dt.get(_list_dict_idx[_left_idx]) in self.__list_none_value:
                    _left_idx -= 1
                while _right_idx < _data_length and _dict_dt.get(_list_dict_idx[_right_idx]) in self.__list_none_value:
                    _right_idx += 1

                if _left_idx >= 0 and _right_idx < _data_length:
                    _left_value: int or float or str = _dict_dt.get(_list_dict_idx[_left_idx])
                    _right_value: int or float or str = _dict_dt.get(_list_dict_idx[_right_idx])
                    if type(_left_value) == str:
                        if re.fullmatch(r'^-?\d+(\.\d+)?$', _left_value):
                            _left_value = float(_left_value)
                        else:
                            _left_value = int(_left_value)
                    if type(_right_value) == str:
                        if re.fullmatch(r'^-?\d+(\.\d+)?$', _right_value):
                            _right_value = float(_right_value)
                        else:
                            _right_value = int(_right_value)
                    _dict_dt[_list_dict_idx[_idx]] = (
                            _left_value + (_right_value - _left_value) * (_idx - _left_idx) / (_right_idx - _left_idx)
                    )
                elif _left_idx >= 0:
                    _dict_dt[_list_dict_idx[_idx]] = _dict_dt[_list_dict_idx[_left_idx]]
                elif _right_idx < _data_length:
                    _dict_dt[_list_dict_idx[_idx]] = _dict_dt[_list_dict_idx[_right_idx]]

        return _dict_result

    def test(self) -> dict:
        _database: communication_.Database = communication_.Database()
        _start = '2024-10-25 13:00:00'
        _end = '2024-10-25 13:05:00'
        _auto = 'true'
        list_revisn_tag: list = [
            {'TAG_NM': 'SWP-RUN-STS', 'TAG_SN': '1', 'RULE_NM': '앞선채우기', 'RULE_CN': '${FRONT_FILL_VALUE}'}]
        _dict_fetch_result: dict = {
            'param': [{
                'name': 'SWP-RUN-STS',
                'values': [{
                    'time': '2024-10-25 13:01:00',
                    'val': '1.0'
                }, {
                    'time': '2024-10-25 13:02:00',
                    'val': '2.0'
                }]
            }, {
                'name': 'INTAKE-RUN-STS',
                'values': [{
                    'time': '2024-10-25 13:03:00',
                    'val': '1.0'
                }, {
                    'time': '2024-10-25 13:04:00',
                    'val': '2.0'
                }]
            }]
        }
        _dict_datas: dict = self.search_empty_data(
            list_tag=_dict_fetch_result.get('param'),
            str_start=_start,
            str_end=_end,
            period=self.__period
        )
        self.__dict_data = _dict_datas
        _list_tag: list = []
        _list_param: list = []
        for _dict_revisn_tag in list_revisn_tag:
            _tag_nm: str = _dict_revisn_tag.get('TAG_NM')
            _tag_sn: str = str(int(_dict_revisn_tag.get('TAG_SN')))
            _rule_nm: str = _dict_revisn_tag.get('RULE_NM')
            _org_datas: dict = _dict_datas.get(_tag_nm)
            if _org_datas is not None:
                _revisn_result: dict = asyncio.run(self.execute_script(_dict_revisn_tag))
                if _revisn_result is None or len(_revisn_result) == 0:
                    continue
                _list_values: list = []
                for _key in _org_datas.keys():
                    if _org_datas.get(_key) != _revisn_result.get(_tag_nm, {}).get(_key):
                        _dict_values = {
                            'name': _tag_nm,
                            'quality': 192,
                            'time': _key + '.000',
                            'val': _revisn_result.get(_tag_nm, {}).get(_key),
                            'old': _org_datas.get(_key)
                        }
                        _list_param.append(_dict_values)
                        _list_values.append(_dict_values)
                _dict_tag_list = {
                    'name': _tag_nm,
                    'sn': _tag_sn,
                    'rule': _rule_nm,
                    'values': _list_values,
                    'auto': _auto
                }
                _list_tag.append(_dict_tag_list)
        _dict_tag: dict = {
            'cmd': 'autoLogic',
            'tagList': _list_tag
        }
        if _list_param is not None and len(_list_param) > 0:
            _biznexus: communication_.BizNexus = communication_.BizNexus()
            _biznexus_cmd = {
                'cmd': 'putValues',
                'param': _list_param
            }
            _str_biznexus_cmd = json.dumps(_biznexus_cmd, ensure_ascii=False)
            _str_biznexus_cmd = _str_biznexus_cmd.replace("'", '"')
            _str_result = _biznexus.exec(_str_biznexus_cmd)
            _result = json.loads(_str_result)
            if len(_result) > 0:
                _result_insert = _database.insert_revisn_log(_dict_tag)
        return _dict_fetch_result


"""
무조건 마지막에 result = 함수호출 해줘야됌
예시)
test = '''def add(a, b):
    return a + b
fucResult = add(x, y)'''

global_vars = {}
local_vars = {'x': 5, 'y': 10}
exec(test, global_vars, local_vars)
print(local_vars['fucResult'])
"""
