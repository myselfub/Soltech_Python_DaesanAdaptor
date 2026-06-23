import copy
import json
from tkinter import XView

import test
from app_config import ConfigUtil
from communication import Socket, Historian

ConfigUtil()
Socket().start_server()
#test.Test().socket_test('', '{"param":{"tagList":["TEST0429"],"start":"2024-12-03 02:00:00","end":"2024-12-03 02:05:00"},"cmd":"importData","dest":"biznexus","timeout":60000}', 'localhost', 8180)
#test.Test().socket_test('', '{"param":{"tagList":["TEST0002"],"start":"2024-12-03 02:00:00","end":"2024-12-03 02:05:00"},"cmd":"importData","dest":"historian","timeout":60000}', 'localhost', 8180)
#test.Test().socket_test('', '{"param":{"tagList":["TEST0001"],"start":"2024-12-03 03:00:00","end":"2024-12-03 03:10:00"},"cmd":"stats","dest":"stats","timeout":120000}', 'localhost', 8180)
#test.Test().socket_test('', '{"param": {"start": "2024-12-02 23:59:00", "end": "2024-12-03 01:01:00", "tagList": ["SWP-RUN-STS", "INTAKE-RUN-STS", "DAF-RUN-STS", "DMGF-RUN-STS", "Cl482", "Cl2101", "TDS482", "pH482", "pH101", "M111-FO-STS"]}, "cmd": "fetchValues", "dest": "biznexus", "timeout": 60000}', 'localhost', 8180)
#{"param": {"tagList": ["TEST0001"], "start": "2024-12-02 23:59:00", "end": "2024-12-03 01:01:00"}, "cmd": "fetchValues", "dest": "biznexus", "timeout": 10000}
#{"param": {"start": "2024-12-02 23:59:00", "end": "2024-12-03 01:01:00", "tagList": ["SWP-RUN-STS", "INTAKE-RUN-STS", "DAF-RUN-STS", "DMGF-RUN-STS", "Cl482", "Cl2101", "TDS482", "pH482", "pH101", "M111-FO-STS"]}, "cmd": "fetchValues", "dest": "biznexus", "timeout": 60000}
#test.Test().socket_test('', '{"cmd":"importData","dest":"biznexus","param":{"start":"2024-12-02 20:00:00","end":"2024-12-02 21:00:00"}}', 'localhost', 8180)
#test.Test().socket_test('', '{"cmd":"importData","dest":"historian","param":{"start":"2024-12-02 20:00:00","end":"2024-12-02 21:00:00"}}', 'localhost', 8180)
#test.Test().socket_test('', '{"cmd":"importData","dest":"biznexus","param":{"start":"2024-11-21 00:00:00","end":"2024-11-21 12:00:00"}}', 'localhost', 8180)
#test.Test().socket_test('', '{"cmd":"stats","dest":"stats","param":{"start":"2024-10-28 02:00:00","end":"2024-10-28 04:00:00"}}', 'localhost', 8180)
#test.Test().socket_test('biz_fetch', None, 'localhost', 8180)
#create_tag_macro.create_biz_tag_macro('TEST', 52, 1000)