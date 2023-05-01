import numpy as np
import pandas as pd
import pyproj

import requests
import json
import datetime
from pytz import timezone
import time
import re
from typing import List, Dict

# 인허가데이터 가져오는 객체
class RequestData():
    def __init__(self, url: str, auth_key: str, columns_dict: Dict[str, str],  *args: tuple):
        self.url = url
        self.auth_key = auth_key
        self.columns_dict = columns_dict

        if len(args)<2:
            self.start_date = datetime.datetime.now(timezone('Asia/Seoul')).strftime("%Y%m%d")
            self.end_date = datetime.datetime.now(timezone('Asia/Seoul')).strftime("%Y%m%d")
        else:
            self.start_date = args[0]
            self.end_date = args[1]

    
    @staticmethod
    # Get csv bulk data
    def get_csvdata(path: str, sep:str =',') -> pd.DataFrame:
        df = pd.read_csv(path, encoding='cp949', sep=sep)
        
        return df
    
    # api서버 요청
    def request_api(self, page_index: int, sector: str, page_size: int) -> dict:

        params = {'authKey': self.auth_key,
                  'resultType': 'json',
                  'pageIndex' : page_index,
                 'lastModTsBgn' : self.start_date,
                 'lastModTsEnd' : self.end_date,
                 'pageSize': page_size,
                 'opnSvcId': sector}

        response = requests.get(self.url, params=params)
        response_text = response.text

        # api호출 에러날 때(500,404 등) 예외처리코드 필요

        # text to json
        response_json = json.loads(response_text)
        return response_json
        
    
    def get_apidata(self, opnSvcId: str) -> pd.DataFrame:
        
        # info for request
        page_index = 1
        page_size = 500
        
        # Request
        response_json = self.request_api(page_index=page_index, sector= opnSvcId, page_size=page_size)
        
        # Count total to get info how many data are to be changed
        total_data_count = response_json['result']['header']['paging']['totalCount']
        iter_count = (total_data_count // page_size)

        # Make fundamental dataFrame
        response_dataframe = self.make_dataframe(response_json)
        
        # Iteration for full data
        for _ in range(iter_count):
            page_index += 1
            response_json = self.request_api(page_index=page_index, sector= opnSvcId, page_size=page_size)
            tmp_dataframe = self.make_dataframe(response_json)
            
            # data concat
            response_dataframe = pd.concat([response_dataframe, tmp_dataframe])
            time.sleep(1)
        
        # reset index after concatenation
        response_dataframe.reset_index(drop=True, inplace=True)
        return response_dataframe
    
    def make_dataframe(self, response_json: json) -> pd.DataFrame:
        df = pd.json_normalize(response_json['result']['body']['rows'][0]['row'])
        return df

# 버스정류장 데이터 가져오는 객체
class RequestBusData():

    seoul_bus_url = 'http://openapi.seoul.go.kr:8088/'
    # source : https://www.data.go.kr/data/15098534/openapi.do#/tab_layer_recommend_data
    all_bus_url = 'https://apis.data.go.kr/1613000/BusSttnInfoInqireService/getSttnNoList'
    all_possible_list_url = 'http://apis.data.go.kr/1613000/BusSttnInfoInqireService/getCtyCodeList'


    def __init__(self, auth_key_seoul: str, auth_key_all: str):
        self.auth_key_seoul = auth_key_seoul
        self.auth_key_all = auth_key_all
    
    # 버스 조회가 가능한 도시 찾는 메소드
    def search_possible_city(self) -> pd.DataFrame:
        
        url = RequestBusData.all_possible_list_url
        params={'serviceKey' : self.auth_key_all,
                '_type' : 'json',
                }

        response = requests.get(url, params=params)
        response_text = response.text
        response_json = json.loads(response_text)
        possible_city_df = pd.DataFrame(response_json['response']['body']['items']['item'])

        return possible_city_df
    
    def get_seoul_bus_data(self) -> pd.DataFrame:
        # info for request
        start_index = 1
        end_index = 1000
        page_size = 1000
        print(start_index, end_index)
        
        # Request
        response_dict = self.request_seoul_bus_api(start_index=start_index, end_index=end_index)
        
        # Count total to get info how many data are to be changed
        total_data_count = response_dict['busStopLocationXyInfo']['list_total_count']
        iter_count = (total_data_count // page_size)

        # Make fundamental dataFrame 나중에 수정
        response_dataframe = pd.json_normalize(response_dict['busStopLocationXyInfo']['row'])
        
        # Iteration for full data
        for _ in range(iter_count):

            start_index += page_size
            end_index += page_size

            response_dict = self.request_seoul_bus_api(start_index=start_index, end_index=end_index)
            tmp_dataframe = pd.json_normalize(response_dict['busStopLocationXyInfo']['row'])
            
            # data concat
            response_dataframe = pd.concat([response_dataframe, tmp_dataframe])
            time.sleep(1)
        
        # reset index after concatenation
        response_dataframe.reset_index(drop=True, inplace=True)

        # 서울 코드 붙이기
        response_dataframe['CityID'] = 11
        response_dataframe['CityName'] = '서울특별시'


        return response_dataframe
    
    # 서울 버스데이터는 start, end index로 전체 데이터 접근하는 방식
    def request_seoul_bus_api(self, start_index: int, end_index: int) -> Dict:

        key = self.auth_key_seoul
        type = 'json'
        service = 'busStopLocationXyInfo'


        url = RequestBusData.seoul_bus_url + f'{key}/{type}/{service}/{start_index}/{end_index}'

        response = requests.get(url)
        response_text = response.text
        response_dict = json.loads(response_text)

        return response_dict

    def request_bus_api(self, page_no: int, page_size: int, city_code: int) -> List[Dict]:
        
        page_size = 2000
        url = RequestBusData.all_bus_url
        params ={'serviceKey' : self.auth_key_all,
                'pageNo' : page_no,
                'numOfRows' : page_size,
                '_type' : 'json',
                'cityCode' : city_code,
                }

        response = requests.get(url, params=params)
        response_text = response.text
        response_dict = json.loads(response_text)

        return response_dict
    
    def get_all_bus_data(self) -> pd.DataFrame:
        
        # 가능한 city code 조회
        possible_city_df = self.search_possible_city()

        # concat 대상이 되는 첫번째 dataframe
        code, city = possible_city_df.loc[0]
        all_dataframe =  self.get_bus_data(code, city)
        print(f"{city} 완료")
        
        for _, row in possible_city_df.loc[1:].iterrows():
            code, city = row['citycode'], row['cityname']

            try:
                tmp_dataframe = self.get_bus_data(code, city)
                all_dataframe = pd.concat([all_dataframe, tmp_dataframe])
                print(f"{city} 완료")
            except:
                print(f"{city} 도시는 데이터가 없습니다")

            time.sleep(1)
        
        all_dataframe.reset_index(drop=True, inplace=True)

        return all_dataframe

    def get_bus_data(self, code: int, city: str) -> pd.DataFrame:

        # info for request
        page_no = 1
        page_size = 2000
        
        # Request
        response_dict = self.request_bus_api(page_no=page_no, page_size=page_size, city_code=code)
        
        # Count total to get info how many data are to be changed
        total_data_count = response_dict['response']['body']['totalCount']
        iter_count = (total_data_count // page_size)

        # Make fundamental dataFrame
        response_dataframe = self.make_dataframe(response_dict)
    
        # Iteration for full data
        for _ in range(iter_count):
            page_no += 1
            response_dict = self.request_bus_api(page_no=page_no, page_size=page_size, city_code=code)
            tmp_dataframe = self.make_dataframe(response_dict)
            
            # data concat
            response_dataframe = pd.concat([response_dataframe, tmp_dataframe])
            time.sleep(1)
    
        # reset index after concatenation
        response_dataframe.reset_index(drop=True, inplace=True)

        # Add city id and city name
        response_dataframe['CityID'] = code
        response_dataframe['CityName'] = city

        return response_dataframe

    def make_dataframe(self, response_dict: dict) -> pd.DataFrame:
        df = pd.json_normalize(response_dict['response']['body']['items']['item'])
        return df

# 데이터 전처리
class DataPreprocess:

    # 한 번에 전처리하는 코드
    @staticmethod
    def preprocess(df: pd.DataFrame, columns_dict: Dict[str, str], is_bulk: bool) -> pd.DataFrame:
        
        ## column 다시 
        df = DataPreprocess.column_realign(df, columns_dict)

        df_name = df['opnSvcId'][0]

        ## 업체명, 주소에 따옴표로 인한 문제 발생 => 잘 치환
        replace_columns = ['bplcNm', 'siteWhlAddr', 'rdnWhlAddr']
        df = DataPreprocess.replace_quote(df, replace_columns)

        if is_bulk:
            df = DataPreprocess.remove_closed_shop(df)

        else:
            ## API로 가져온 데이터이면,특정 column의 type값을 바꿔줘야 함
            change_cols = ['trdStateGbn', 'dtlStateGbn']
            df = DataPreprocess.type_change(df, change_cols, change_type=int)

        # 휴게음식점인 경우에만 카페 필터링
        if df_name == '07_24_05_P':
            df = DataPreprocess.filter_cafe(df)
        
        # 담배소매업일 경우에만 편의점 필터링
        if df_name == '11_43_02_P':
            df = DataPreprocess.filter_convenience_store(df)

        # 결측값 변환 후 반환
        df = DataPreprocess.replace_nan(df)

        # 기존 좌표계를 다 위경도 값 바꿔주기
        original_coord = np.array(df.loc[:, ['x', 'y']])
        input_type = "epsg:5174"
        output_type = "epsg:4326"

        transformed_coord = DataPreprocess.project_array(original_coord, input_type, output_type)
        df.loc[:, ['lat', 'lon']] = transformed_coord
        return df

    # 한글 column 영문으로 바꾸고 재배치, 필요 column만 추출하는 코드
    ## data field는 api의 column기준으로 맞춤
    @staticmethod
    def column_realign(df: pd.DataFrame, columns_dict: Dict[str, str]) -> pd.DataFrame:

        columns_values = list(columns_dict.values())
        new_df = df.rename(columns = columns_dict)[columns_values]
        
        return new_df

    # replace_quote에 넣을 함수
    @staticmethod
    def add_backslash(text: str) -> str:
        pattern = r'(["\'])'  # 큰따옴표 또는 작은따옴표 패턴
        replace = r'\\\1'  # 백슬래시와 해당 따옴표

        return re.sub(pattern, replace, f'{text}') # 상호명이 숫자로만 이루어진 경우도 있었음(ex.676)

    # 큰따옴표, 작은따옴표 처리
    @staticmethod
    def replace_quote(df: pd.DataFrame, replace_columns: List[str]) -> pd.DataFrame:
        for column in replace_columns:
            df[column] = df[column].map(DataPreprocess.add_backslash, na_action='ignore')
        
        return df

    # 여러 field의 type을 한 번에 바꾸려 하기 위함
    @staticmethod
    def type_change(df: pd.DataFrame, columns_list: List[str], change_type: str):

        for column in columns_list:
            try:
                df[column] = df[column].astype(change_type)
            
            # BBBB일 경우
            except ValueError:
                df.loc[df[column] == 'BBBB', column] = 13
                df[column] = df[column].astype(change_type)

        
        return df

    # 결측치 처리 함수
    @staticmethod
    def replace_nan(df: pd.DataFrame) -> pd.DataFrame:
        
        # 좌표값이 결측치인 경우, 공백인 경우를 모두 제거한다
        ## 좌표값이 공백인 경우를 모두 결측치로 변경
        df.loc[:, ['x', 'y']] = df.loc[:, ['x', 'y']].replace(r'^\s*$', np.nan, regex=True)

        ## 좌표값이 결측치인 경우 삭제 후 
        df = df[~df['x'].isna() & ~df['y'].isna()]

        # 나머지 값들은 결측치나 공백으로 이루어진 것을 null로 바꿈
        ## MySQL에서 NULL값으로 인식하도록 하기 위함
        df.fillna('NULL', inplace=True)
        df.replace(r'^\s*$', 'NULL', regex=True, inplace=True)
        
        return df
    
    @staticmethod
    def remove_closed_shop(df: pd.DataFrame) -> pd.DataFrame:
        df.drop(df[( df['trdStateGbn'] == 3 ) | ( df['trdStateGbn'] == 4 ) | ( df['trdStateGbn'] == 5 )].index, inplace=True)
        return df
    
    @staticmethod
    def project_array(coord: np.array, input_type: str, output_type: str) -> np.array:
        """
        좌표계 변환 함수
        - coord: x, y 좌표 정보가 담긴 NumPy Array
        - p1_type: 입력 좌표계 정보 ex) epsg:2097
        - p2_type: 출력 좌표계 정보 ex) epsg:4326
        """
        # 보정된 중부원점(EPSG:5174)
        input = pyproj.Proj(init=input_type)
        output = pyproj.Proj(init=output_type)
        fx, fy = pyproj.transform(input, output, coord[:, 0], coord[:, 1])
        return np.dstack([fx, fy])[0]    
    
    @staticmethod
    def filter_cafe(df: pd.DataFrame) -> pd.DataFrame:
        allowed_shop = ['과자점', '기타 휴게음식점', '다방', '아이스크림', '전통찻집', '커피숍']
        keywords = ['커피', '카페', 'coffee', '까페', '스타벅스', '이디야', '빽다방', '파스쿠찌', '투썸플레이스', '폴바셋', '할리스', '더벤티', '탐앤탐스', '매머드', '공차', '스킨라빈스', '와플대학', '던킨', '크리스피']
        restricted_keywords = ['PC']

        # Delete rows based on 업태구분
        df_new = df[df['uptaeNm'].isin(allowed_shop)]

        # Delete rows based on 사업장명
        df_new = df_new[df_new['bplcNm'].str.contains('|'.join(keywords), case=False)]

        # Delete rows that contain 'PC' in 사업장명
        df_new = df_new[~df_new['bplcNm'].str.contains('|'.join(restricted_keywords), case=False)]

        return df_new
    
    @staticmethod
    def filter_convenience_store(df: pd.DataFrame) -> pd.DataFrame:

        # Filter values in the '사업장명' column for the first file
        keywords = ['씨유', 'CU', '세븐', '지에스', 'GS', '이마트24', '미니스톱', 'MINISTOP']
        
        df_new = df[df['bplcNm'].str.contains('|'.join(keywords))]

        return df_new