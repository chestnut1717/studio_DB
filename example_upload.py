from utils.load_data import RequestData, RequestBusData, DataPreprocess
from utils.columns import Columns


import pandas as pd
import os

# warning 무시
import warnings
warnings.filterwarnings(action='ignore')

from tqdm import tqdm

# 모듈 수정할 때 사용
import importlib
# importlib.reload(utils.db_connector)

pd.set_option("display.max_columns", 50)

# db정보 들어있는 텍스트파일
db_info_path = 'secret_key/db_info.txt'
db_info_dict = {}

with open(db_info_path, 'r') as f:
    for line in f.readlines():
        line_info = line.split('=')
        key = line_info[0].strip()
        val = line_info[1].strip()
        
        db_info_dict[key] = val

# Mysql db 연결
from utils.db_connector import DBManagement


# localdata 데이터베이스에 connection
dbm = DBManagement(
    **db_info_dict
)

print(f'성공적으로 MySQL {db_info_dict["database"]} 데이터베이스에 연결 완료')

# csv 파일 적재
# 파일 경로 검색
# 절대경로
path = os.getcwd()

csv_path = os.getcwd() + '/csv_data'
in_csv_files = sorted(os.listdir(csv_path))

try:
    in_csv_files.remove('.DS_Store')
except:
    pass

columns_dict = {
                '개방자치단체코드': 'opnSfTeamCode',
                '관리번호': 'mgtNo',
                '개방서비스아이디': 'opnSvcId',
                '데이터갱신구분': 'updateGbn',
                '데이터갱신일자': 'updateDt',
                '사업장명': 'bplcNm',
                '소재지우편번호': 'sitePostNo',
                '소재지전체주소': 'siteWhlAddr',
                '도로명우편번호': 'rdnPostNo',
                '도로명전체주소': 'rdnWhlAddr',
                '영업상태구분코드': 'trdStateGbn',
                '상세영업상태코드': 'dtlStateGbn',
                '좌표정보(X)': 'x',
                '좌표정보(Y)': 'y',
                '최종수정시점': 'lastModTs',
                '업태구분명': 'uptaeNm',
               }

# 영문 column(API 요청할때나 MySQL 적재할 때 사용)
cols = Columns(columns_dict)

# MySQL에서 적재할 때, POINT type으로 추가로 위경도 저장하기 때문에
cols_eng = cols.eng + ['coordinates']

# MySQL 저장코드
for file_name in in_csv_files:


    # 파일 경로
    file_path = csv_path + '/' + file_name

    # 데이터 로드
    data_df = RequestData.get_csvdata(path = file_path)

    # 전처리
    data_preprocess_df = DataPreprocess.preprocess(data_df, columns_dict, is_bulk=True)
    
    # data_preprocess_df.sample(n=1000).to_csv(f'ex_{file_name[3:-4]}_5174.csv', index=False, encoding='cp949')
    # table 생성
    table_name = file_name[3:-4]
    dbm.create_table(table_name)
    dbm.commit()
    
    # DB에 데이터 젛기
    for _, row in tqdm(data_preprocess_df.iterrows(), mininterval=0.01):


        dbm.update_record(row, cols_eng)

    # 완료되면 commit
    dbm.commit()
    

    print(f"{file_name} 작업 완료")

data_preprocess_df.head()