# encoding=utf8

from pymysql import *


class MysqlHelper(object):
    # todo 数据库连接参数，可以定义多个，比如conn_params1，conn_params2，用于连接多个数据库，在类实例化时指定
    conn_params = {
        'host': '176.10.10.235',
        'port': 3306,
        'user': 'root',
        'password': 'root',
        'db': 'qa_db',
        'charset': 'utf8'}
    cgpt_conn_params = {
        'host': '176.10.10.235',
        'port': 3306,
        'user': 'root',
        'password': 'root',
        'db': 'cgpt',
        'charset': 'utf8'}

    # todo 类的构造函数，主要用于类的初始化
    def __init__(self, conn_params):
        self.__host = conn_params['host']
        self.__port = conn_params['port']
        self.__db = conn_params['db']
        self.__user = conn_params['user']
        self.__passwd = conn_params['password']
        self.__charset = conn_params['charset']

    # todo 建立数据库连接和打开游标
    def __connect(self):
        self.__conn = connect(host=self.__host,
                              port=self.__port,
                              db=self.__db,
                              user=self.__user,
                              passwd=self.__passwd,
                              charset=self.__charset)
        self.__cursor = self.__conn.cursor()

    # todo 关闭游标和关闭连接
    def __close(self):
        self.__cursor.close()
        self.__conn.close()

    # todo 取一条数据
    def get_one(self, sql, params):
        result = None
        try:
            self.__connect()
            self.__cursor.execute(sql, params)
            result = self.__cursor.fetchone()
            self.__close()
        except Exception as e:
            print(e)
        return result

    # todo 取所有数据
    def get_all(self, sql, params):
        lst = ()
        try:
            self.__connect()
            self.__cursor.execute(sql, params)
            lst = self.__cursor.fetchall()
            self.__close()
        except Exception as e:
            print(e)
        return lst

    # todo 增加数据
    def insert(self, sql, params):
        return self.__edit(sql, params)

    # todo 修改数据
    def update(self, sql, params):
        return self.__edit(sql, params)

    # todo 删除数据
    def delete(self, sql, params):
        return self.__edit(sql, params)
    
    # 回滚
    def rollback(self):
        self.__conn.rollback()

    # todo 写数据操作具体实现，增删改操作都是调用这个方法来实现，这是个私有方法，不允许类外部调用
    def __edit(self, sql, params):
        count = 0
        try:
            self.__connect()
            count = self.__cursor.execute(sql, params)
            self.__conn.commit()
            self.__close()
        except Exception as e:
            print(e)
        return count

if __name__ == "__main__":
    #查询数据
    mysqlhelper = MysqlHelper(MysqlHelper.conn_params)
    sql = "select * from user where name=%s"
    params = ('徐军',)
    result = mysqlhelper.get_all(sql,params)
    print(row for row in result)