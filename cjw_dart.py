import dart_fss as dart
import cjw_maria
import cjw_pykrx
import pandas as pd
import os
import csv

class MariaDart():
    def __init__(self):
        api_key = '601c51e82dfa122fa35a240ad5722593a0cbc2b4'
        self.dart = dart
        self.dart.set_api_key(api_key=api_key)
        self.corp_list = self.dart.get_corp_list()
        self.report_tp = ['quarter']
        self.repott_tp_ = 'quarter'

        self.maria = cjw_maria.MariaDB()

    def getCorpCode(self, code):
        return self.corp_list.find_by_stock_code(code)

    def getJamoo(self, code, bgn_de):
        fs = self.getCorpCode(code).extract_fs(bgn_de=bgn_de, report_tp=self.report_tp)
        return fs

    def saveExcelJamoo(self, code, bgn_de):
        fs = self.getJamoo(code, bgn_de)
        filename = '{}_{}_{}.xlsx'.format(code, bgn_de, self.repott_tp_)
        fs.save(filename)
    
    def saveDBJamoo(self, code, bgn_de, columns, columns_type, tablename):
        # create table
        tables = self.maria.getTables()
        if tablename not in tables:
            _columns = ['code']
            for c in columns:
                _columns.append(c)
            self.maria.createTable(tablename, _columns, columns_type)
        filename = '{}_{}_{}.xlsx'.format(code, bgn_de, self.repott_tp_)
        if filename not in os.listdir('fsdata'):
            try:
                self.saveExcelJamoo(code, bgn_de)
            except:
                self.writeError('error1.csv', code, 'no file error')
                return
        fs = pd.read_excel(os.path.join('fsdata', filename), engine='openpyxl')
        data = [code]
        for c in columns:
            try:
                data.append(fs[fs['Unnamed: 2'] == c][fs.columns[9]].iloc[0])
            except Exception as e:
                self.writeError('error1.csv', code, 'columns error')
                data.append(0)
        self.maria.insertData(tablename, tuple(data))

    def writeError(self, filename, code, error):
        with open('{}.csv'.format(filename), 'w', newline='') as csvfile:
            spamwriter = csv.writer(csvfile, delimiter=' ', quotechar='|', quoting=csv.QUOTE_MINIMAL)
            spamwriter.writerow([code, error])

# mariaDart = MariaDart()
# for c in cjw_pykrx.getStockCode()[5:10]:
#     mariaDart.saveDBJamoo(c, '20200831', ['유동자산', '단기금융상품'], ['VARCHAR(20)', 'BIGINT', 'BIGINT'],'testtable9')
#     mariaDart.maria.commitDB()
# mariaDart.maria.connect.close()
