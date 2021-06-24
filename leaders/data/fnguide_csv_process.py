from fnguide import Data

# param
is_quarter = True
curr_qt = '2021_03'

# csv 파일 종목명으로 이름바꿔서 저장
fnguide = Data()
fnguide.modifyPathFnCsvFiles()

# db에 저장
fnguide.setMaria()
fnguide.insertFnData(gb='jaemusangtaepyo', is_quarter=is_quarter)
fnguide.insertFnData(gb='pogwalsoniggyesanseo', is_quarter=is_quarter)
fnguide.insertFnData(gb='hyeongeumheuleumpyo', is_quarter=is_quarter)
