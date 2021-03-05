import pymysql
import pandas as pd

class MariaDB():
    def __init__(self, host='localhost', user='root', password='sa1234', db='jamoo', charset='utf8'):
        self.connect = pymysql.connect(host=host, user=user, password=password,db=db, charset=charset)
        self.cur = self.connect.cursor()

    def insertData(self, tablename, data):
        try:
            sql = "insert into {} values {};".format(tablename, data)
            self.cur.execute(sql)
        except Exception as e:
            print(sql)
            print(e.args)

    def commitDB(self):
        self.connect.commit()

    def showData(self, sql):
        try:
            # sql = "SELECT * FROM {}".format(tablename)
            self.cur.execute(sql)
            df = self.cur.fetchall()
            field_names = [i[0] for i in self.cur.description]
            df = pd.DataFrame(df, columns=field_names)
            #print(df)
            return df
        except Exception as e:
            print(e)
            pass

    def createTable(self, tablename, columns, columns_type):
        try:
            if tablename in self.getTables():
                return
            sql = "CREATE TABLE {} ({} {} PRIMARY KEY".format(tablename, columns[0], columns_type[0])
            for i in range(1, len(columns)):
                sql += ", {} {}".format(columns[i], columns_type[i])
            sql += ');'
            self.cur.execute(sql)
        except Exception as e:
            print(sql)
            print(e)
            pass

    def getTables(self):
        sql = "SHOW tables;"
        self.cur.execute(sql)
        df = pd.DataFrame(self.cur.fetchall())
        df.columns = ['tables']
        return list(df['tables'])

