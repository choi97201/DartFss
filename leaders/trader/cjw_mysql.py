import pymysql
import pandas as pd

class Maria():
    def setMaria(self, host='15.165.29.213', user='lt_user', password='De4IjOY32e7o', db='leaderstrading',
                 charset='utf8'):
        self.connect = pymysql.connect(host=host, user=user, password=password, db=db, charset=charset)
        self.cur = self.connect.cursor()
        return

    def mariaInsertData(self, tablename, data, start):
        df = self.mariaShowData(tablename)
        cols = list(df.columns)
        sql = "replace into {}{} ".format(tablename, tuple(cols[start:])).replace('\'', '')
        sql += ' values {};'.format(data)
        self.cur.execute(sql)

    def mariaCommitDB(self):
        self.connect.commit()
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