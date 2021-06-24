import pymysql
import pandas as pd

class Maria:
    def __init__(self):
        self.is_chrome = False
        self.path = None
        self.connect = None
        self.cur = None

    def setMaria(self, host='localhost', user='root', password='sa1234', db='jaemoo', charset='utf8', port=3306):
        self.connect = pymysql.connect(host=host, user=user, password=password, db=db, charset=charset, port=port)
        self.cur = self.connect.cursor()
        return

    def mariaReplaceData(self, tablename, data, start):
        df = self.mariaShowData(tablename)
        cols = list(df.columns)
        sql = "replace into {}{} ".format(tablename, tuple(cols[start:])).replace('\'', '')
        sql += ' values {};'.format(data)
        self.cur.execute(sql)

    def mariaInsertData(self, tablename, data):
        sql = "insert into {} values {};".format(tablename, data)
        try:
            self.cur.execute(sql)
        except Exception as e:
            print(sql)
            print(e.args)
        return

    def mariaCommitDB(self):
        self.connect.commit()
        return

    def mariaShowData(self, tablename, sql=None):
        try:
            if sql is None:
                self.cur.execute('select * from ' + tablename)
                df = self.cur.fetchall()
                field_names = [i[0] for i in self.cur.description]
                df = pd.DataFrame(df, columns=field_names)
                return df
            else:
                self.cur.execute(sql)
                df = self.cur.fetchall()
                field_names = [i[0] for i in self.cur.description]
                df = pd.DataFrame(df, columns=field_names)
                return df
        except Exception as e:
            print(e)
            return None

    def mariaSql(self, sql):
        self.cur.execute(sql)
        self.cur.fetchall()
        return

    def mariaCreateTable(self, tablename, columns, columns_type):
        sql = "CREATE TABLE {} ({} {} PRIMARY KEY".format(tablename, columns[0], columns_type[0])
        try:
            for i in range(1, len(columns)):
                sql += ", {} {}".format(columns[i], columns_type[i])
            sql += ');'
            self.cur.execute(sql)
        except Exception as e:
            print(sql)
            print(e)
            pass
        return

    def mariaShowTables(self, sql=None):
        if sql is None:
            self.cur.execute('show tables')
            df = self.cur.fetchall()
            field_names = [i[0] for i in self.cur.description]
            df = pd.DataFrame(df, columns=field_names)
        else:
            try:
                self.cur.execute(sql)
                df = self.cur.fetchall()
                field_names = [i[0] for i in self.cur.description]
                df = pd.DataFrame(df, columns=field_names)
            except Exception as e:
                df = None
        return df
