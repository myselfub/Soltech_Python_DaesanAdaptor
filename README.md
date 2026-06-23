# Soltech_Python_DaesanAdaptor
솔텍시스템-한국수자원공사/하니웰 대산해수담수화 어댑터 프로그램, Python

pip install pyodbc
pip install pythonnet
pip install DBUtils
pip install schedule
pip install pyinstaller
pip install py_mini_racer

pyinstaller --onefile --add-data="dll/IHUAPI.dll;dll" --add-data="dll/IHUAPI_32.dll;dll" --add-data="dll/Utilities.dll;dll" --add-data="dll/Utilities_32.dll;dll" --add-data="dll/TagAPI64.dll;dll" --add-data="icon.ico;." --add-data="config.ini;." --icon="icon.ico" -n=DaesanAdaptor main.py
