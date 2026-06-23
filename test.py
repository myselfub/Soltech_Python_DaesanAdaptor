import json
import random
import socket
import threading
import time
import typing
from datetime import datetime, timedelta

import schedule

import communication


class Test:
    """ 테스트 클래스 """
    _instance = None
    __inited: bool = False
    __scheduler_thread: threading.Thread = None
    __str_time: str = ':00'
    __scheduler_func: typing.Callable = None
    __list_tag_name: list = ['INTAKE-RUN-STS', 'Cl482', 'TDS482']

    def __new__(cls, *args, **kwargs) -> _instance:
        if cls._instance is None:
            cls._instance = super(Test, cls).__new__(cls)
        return cls._instance

    def __init__(self, str_time: str = ':00') -> None:
        if not self.__inited:
            self.__inited = True
            self.__str_time = str_time
            self.__scheduler_func = self.__default_scheduler_func_test

    def run_scheduler_test(self) -> None:
        """ 스케쥴러 쓰레드 실행 """
        self.__scheduler_thread = threading.Thread(target=self.__run_scheduler_test)
        self.__scheduler_thread.start()

    def __run_scheduler_test(self) -> None:
        """ 스케쥴러 함수 실행 """
        if self.__scheduler_func is not None:
            schedule.every(1).hours.at(self.__str_time).do(self.__scheduler_func)
            while True:
                schedule.run_pending()
                time.sleep(10)

    def __default_scheduler_func_test(self) -> None:
        for _tag_name in self.__list_tag_name:
            #Socket().test(cmd='write', req_data=self._create_test_data(_tag_name))
            _dict_receive: dict = json.loads(self._create_test_data(_tag_name))
            _result = communication.Historian().put_values(param=_dict_receive.get('param')).replace("'", '"')

    def _default_scheduler_func_test(self) -> None:
        self.__default_scheduler_func_test()

    def _create_test_data(self, tag_name: str) -> str:
        _list_random_data: list = [None, '0.1', None, '0.5', None, '0.9', None]
        _str_time_format: str = '%Y-%m-%d %H:%M:%S'
        _str_time: str = (datetime.now() - timedelta(hours=1)).strftime('%Y-%m-%d %H')
        _end_time: str = datetime.now().strftime('%Y-%m-%d %H')
        _str_start_time: str = _str_time + ':00:00'
        _str_end_time: str = _end_time + ':00:00'

        _list_tag_list: list = []
        _dt_start: datetime = datetime.strptime(_str_start_time, _str_time_format)
        _dt_end: datetime = datetime.strptime(_str_end_time, _str_time_format)
        _add_second: timedelta = timedelta(seconds=1)
        _list_values: list = []
        while _dt_start != _dt_end:
            _value = random.choice(_list_random_data)
            if _value is None:
                _dt_start += _add_second
                continue
            _dict_values: dict = {
                'time': _dt_start.strftime(_str_time_format),
                'val': _value,
                'type': 'float'
            }
            _list_values.append(_dict_values)
            _dt_start += _add_second
        _dict_tag_list: dict = {
            'values': _list_values,
            'name': tag_name
        }
        _list_tag_list.append(_dict_tag_list)
        _dict_req_data = {
            'param': {
                'tagList': _list_tag_list,
            },
            'cmd': 'putValues'
        }
        _str_req_data = json.dumps(_dict_req_data, ensure_ascii=False)
        _str_req_data = _str_req_data.replace("'", '"')
        return _str_req_data

    def js_test(self):
        from py_mini_racer import py_mini_racer

        js_cd = """
        function test(a, b) {
            return a + b;
        };
        test(1, 2)
        """
        ctx = py_mini_racer.MiniRacer()
        result = ctx.eval(js_cd)
        print(result)

    def socket_test(self, cmd: str = 'req', req_data: str = None, ip: str = '', port: int = 0) -> None:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as _client_socket:
            _client_socket.connect((ip, port))
            _switch_ex = {
                'biz_fetch': b"""{
                    "cmd":"fetchValues",
                    "dest": "biznexus",
                    "param": {
                        "start": "2024-10-28 12:00:00",
                        "end": "2024-10-29 16:00:00",
                        "tagList": ["CL2101"]
                    }
                }""",
                'req': b"""{
                    "cmd": "reqValues",
                    "dest": "historian",
                    "param": {
                        "tagList": [
                            "TEST01",
                            "TEST02",
                            "TEST04"
                        ]
                    }
                }""",
                'fetch': b"""{
                    "cmd": "fetchValues",
                    "dest": "historian",
                    "param": {
                        "tagList": [
                            "TEST01",
                            "TEST02",
                            "TEST04"
                        ],
                        "start": "2024-09-11 16:14:00",
                        "end": "2024-09-11 16:14:10"
                    }
                }""",
                'write': b"""{
                    "cmd": "putValues",
                    "dest": "historian",
                    "param": {
                        "tagList": [
                            {
                                "name": "TEST01",
                                "values": [
                                    {
                                        "time": "2024-09-11 16:14:00",
                                        "val": "27.0",
                                        "type": "float"
                                    },
                                    {
                                        "time": "2024-09-11 16:14:10",
                                        "val": "27.0",
                                        "type": "float"
                                    }
                                ]
                            },
                            {
                                "name": "TEST02",
                                "values": [
                                    {
                                        "time": "2024-09-11 16:40:00",
                                        "val": "30.0",
                                        "type": "float"
                                    }
                                ]
                            }
                        ]
                    }
                }""",
                'revisn': b"""{
                    "cmd": "revisnData",
                    "dest": "historian",
                    "param": {
                        "tagList": ["TEST01"],
                        "start": "2024-09-11 16:14:00",
                        "end": "2024-09-11 16:14:10",
                        "auto": "false"
                    }
                }"""
            }
            if req_data is not None:
                _client_socket.sendall(req_data.encode('utf-8'))
            else:
                _client_socket.sendall(_switch_ex.get(cmd))
            _bytes_recv_data: bytes = _client_socket.recv(100 * 1024 * 1024)
            _str_result: str = _bytes_recv_data.decode(encoding='utf-8')
            print(_str_result)

# Socket().test(cmd='write')
# Socket().test(cmd='fetch')
# Socket().test(cmd='revisn')

"""
receive_data = '{"cmd": "revisnData","param": {"tagList": ["Cl2101"],"start": "2024-09-30 00:00:00","end": "2024-09-30 23:59:59","auto": "false"}}'
_dict_receive: dict = json.loads(receive_data)
_revisn = [{'TAG_SN': '10', 'TAG_NM': 'Cl2101', 'RULE_NM': '뒤의 값으로 채우기', 'RULE_CN': '${FRONT_FILL_VALUE}'}]
logics.Logics().revisn_data(dict_cmd=_dict_receive, list_revisn_tag=_revisn)
print(a)

a = '{"param":{"tagList":[{"values":[{"val":"0","time":"2024-09-26 14:06:00","type":"float"},{"val":"0","time":"2024-09-26 14:07:00","type":"float"},{"val":"0","time":"2024-09-26 14:08:00","type":"float"},{"val":"0","time":"2024-09-26 14:09:00","type":"float"}],"name":"INTAKE-RUN-STS"}]},"cmd":"putValues"}'
b = '{"param":{"tagList":[{"values":[{"val":"1","time":"2024-09-26 14:06:00","type":"float"},{"val":"1","time":"2024-09-26 14:07:00","type":"float"},{"val":"1","time":"2024-09-26 14:08:00","type":"float"},{"val":"1","time":"2024-09-26 14:09:00","type":"float"}],"name":"INTAKE-RUN-STS"}]},"cmd":"putValues"}'
Socket().test(cmd='write', req_data=a)
Socket().test(cmd='write', req_data=b)
"""
