import OpenDartReader

class MariaDart():
    def __init__(self):
        api_key = '601c51e82dfa122fa35a240ad5722593a0cbc2b4'
        self.dart = OpenDartReader(api_key)
    
    def getData(self, corp, bsns_year):
        df = []
        for r in ['11013', '11012', '11014', '11011']:
            fs = dart.finstate_all(corp='005930', bsns_year=2019, reprt_code='11014')
            fs_bs = fs[fs['sj_div']=='BS'] # 자본총계, 무형자산, 이연법인세자산, 이연법인세부채, 자본금, 현금및현금성자산, 단기차입금, 장기차입금
            bs_list = ['자본총계', '무형자산', '이연법인세자산', '이연법인세부채', '자본금', '현금및현금성자산', '단기차입금', '장기차입금']
            df = []
            for b in bs_list:
                df.append(fs_bs[fs_bs['account_nm'] == b])
        df = pd.concat(df)
        return 