from threading import Timer

from pymysql import Connection
from pymysql.cursors import DictCursor


class MysqlManager(object):
    def __init__(self, config=None):
        self._cfg = config or {}
        self.__r = None
        self.__t = Timer(0.5, self._conn)

    def _conn(self):
        for i in range(2):
            try:
                self.__r = self.__r or self.__conn()
                self.__r.ping()
                if self.__t.is_alive():
                    self.__t.cancel()
                return self.__r
            except Exception as e:
                self.__r = None
                if not self.__t.is_alive():
                    self.__t.start()
                print("mysql 链接失败：{}".format(e))

    def conn(self):
        return self._conn()

    def __conn(self):
        _r = self._cfg

        host = _r.get("host")
        user = _r.get("user")
        port = _r.get("port")
        charset = _r.get("charset")
        password = _r.get("password")
        database = _r.get("database")
        kwargs = _r.get("kwargs", {})

        return Connection(
            host=host,
            user=user,
            password=password,
            database=database,
            port=port,
            charset=charset,
            cursorclass=DictCursor,
            **kwargs
        )
