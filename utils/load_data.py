import numpy as np
import pandas as pd

import requests
import json

import datetime
from pytz import timezone
import time
import re
from typing import List, Dict
from abc import *

# Exception for Empty Data
class EmptyDataFromResponse(Exception):
    def __init__(self):
        super().__init__('해당 일자에 데이터가 없습니다.')

# Interface
class RequestData(metaclass=ABCMeta):
    
    # @abstractmethod
    # def get_csvdata(self, path: str, sep: str =',') -> pd.DataFrame:
    #     """
    #     Get CSV bulk data from local path
    #     """
    #     pass
    
    @abstractmethod
    def request_api(self, params: Dict) -> Dict:
        """
        Get json from API server
        """
        pass

    @abstractmethod
    def get_apidata(self, info: Dict) -> pd.DataFrame:
        pass

    @abstractmethod
    def make_dataframe(self, response_dict: Dict) -> pd.DataFrame:
        pass

    # @abstractmethod
    # def to_csv(self, path: str) -> None:
    #     pass


class RequestLocalData(RequestData):

    url = "http://www.localdata.go.kr/platform/rest/TO0/openDataApi"

    def __init__(self, auth_key: str, *args: tuple):
        self.auth_key = auth_key

        if len(args)<2:
            self.start_date = datetime.datetime.now(timezone('Asia/Seoul')).strftime("%Y%m%d")
            self.end_date = datetime.datetime.now(timezone('Asia/Seoul')).strftime("%Y%m%d")
        else:
            self.start_date = args[0]
            self.end_date = args[1]

    @staticmethod
    def get_csvdata(path: str, sep:str =',') -> pd.DataFrame:
        """
        Get CSV bulk data from local path
        """
        df = pd.read_csv(path, encoding='cp949', sep=sep)
        
        return df
    
    def to_csv(self, path: str) -> None:
        pass

    def request_api(self, params: Dict) -> Dict:
        """
        Get json data from Localdata API server
        """

        # Request Localdata API server and get response
        response = requests.get(RequestLocalData.url, params=params)
        response_text = response.text

        # Need Exception handling when request data from API server(404, 500)

        # text to json
        response_dict = json.loads(response_text)
        return response_dict
    
    def get_apidata(self, info: Dict) -> pd.DataFrame:
        
        page_index = info['pageIndex']
        page_size = info['pageSize']
        opnSvcId = info['opnSvcId']

        # Request API server
        response_dict = self.request_api(params=info)
        
        # Count total to get info how many data are to be changed
        try:
            total_data_count = response_dict['result']['header']['paging']['totalCount']
            if total_data_count < 1:
                raise EmptyDataFromResponse
        except Exception as e:
            print(f"{opnSvcId}서비스에는 {self.start_date}부터 {self.end_date}까지의 데이터가 없습니다")
            return None
        
        iter_count = (total_data_count // page_size)
        

        # Make fundamental dataFrame
        response_dataframe = self.make_dataframe(response_dict)
        
        # Iteration for full data
        for _ in range(iter_count):
            page_index += 1
            info['pageIndex'] = page_index

            response_dict = self.request_api(params=info)
            tmp_dataframe = self.make_dataframe(response_dict)
            
            # data concat
            response_dataframe = pd.concat([response_dataframe, tmp_dataframe])
            time.sleep(1)
        
        # Reset index after concatenation
        response_dataframe.reset_index(drop=True, inplace=True)
        print(f"{opnSvcId}서비스의 {self.start_date}부터 {self.end_date}의 데이터를 성공적으로 다운받았습니다")
        return response_dataframe
    
    def make_dataframe(self, response_dict: Dict) -> pd.DataFrame:
        df = pd.json_normalize(response_dict['result']['body']['rows'][0]['row'])
        return df


class RequestSeoulBusData(RequestData):
    seoul_bus_url = 'http://openapi.seoul.go.kr:8088/'

    def __init__(self, auth_key) -> None:
        self.auth_key = auth_key
        self.info = {
                    'key' : self.auth_key,
                    'type' : 'json',
                    'service' : 'busStopLocationXyInfo',
                    'start_index' : 1,
                    'end_index' : 1000,
                    'page_size' : 1000
                    }

    # 서울 버스데이터는 start, end index로 전체 데이터 접근하는 방식
    def request_api(self, params: Dict) -> Dict:
        
        key         = params['key']
        type        = params['type']
        service     = params['service']
        start_index = params['start_index']
        end_index   = params['end_index']


        url = self.seoul_bus_url + f'{key}/{type}/{service}/{start_index}/{end_index}'

        response = requests.get(url)
        response_text = response.text
        response_dict = json.loads(response_text)

        return response_dict
    
    def get_apidata(self) -> pd.DataFrame:
        # info for request
        start_index = self.info['start_index']
        end_index = self.info['end_index']
        page_size = self.info['page_size']

        # Request
        response_dict = self.request_api(params=self.info)
        
        # Count total to get info how many data are to be changed
        total_data_count = response_dict['busStopLocationXyInfo']['list_total_count']
        iter_count = (total_data_count // page_size)

        # Make fundamental dataFrame
        response_dataframe = self.make_dataframe(response_dict)
        
        # Iteration for full data
        for _ in range(iter_count):
            
            # Renew index 
            start_index += page_size
            end_index += page_size

            self.info['start_index'] = start_index
            self.info['end_index'] = end_index

            # Request Seoul Bus API
            response_dict = self.request_api(params=self.info)
            tmp_dataframe = self.make_dataframe(response_dict)
            
            # Data concat
            response_dataframe = pd.concat([response_dataframe, tmp_dataframe])
            time.sleep(1)
        
        # reset index after concatenation
        response_dataframe.reset_index(drop=True, inplace=True)

        # 서울 코드 붙이기
        response_dataframe['CityID'] = 11
        response_dataframe['CityName'] = '서울특별시'

        print('서울특별시 완료')
        return response_dataframe
    
    def make_dataframe(self, response_dict: dict) -> pd.DataFrame:
        df = pd.json_normalize(response_dict['busStopLocationXyInfo']['row'])
        return df


class RequestOtherBusData(RequestData):
    other_bus_url = 'https://apis.data.go.kr/1613000/BusSttnInfoInqireService/getSttnNoList'
    other_possible_list_url = 'http://apis.data.go.kr/1613000/BusSttnInfoInqireService/getCtyCodeList'

    def __init__(self, auth_key) -> None:
        self.auth_key = auth_key
        self.info = {
                    'page_size' : 2000,
                    'serviceKey' : self.auth_key,
                    'pageNo' : 1,
                    'numOfRows' : 2000,
                    '_type' : 'json',
                    }

    def request_api(self, params: Dict) -> Dict:

        url = self.other_bus_url

        response = requests.get(url, params=params)
        response_text = response.text
        response_dict = json.loads(response_text)

        return response_dict       

    def get_apidata(self) -> pd.DataFrame:
        
        # 가능한 city code 조회
        possible_city_df = self.search_possible_city()

        # Make base dataframe to concat
        code, city = possible_city_df.loc[0]
        self.info['cityCode'] = code

        all_dataframe = self.get_city_data(city=city)

        print(f"{city} 완료")
        
        # Traverse all city to get bus data
        for _, row in possible_city_df.loc[1:].iterrows():
            code, city = row['citycode'], row['cityname']
            
            # pageNo 항상 초기화
            self.info['pageNo'] = 1
            self.info['cityCode'] = code

            try:
                tmp_dataframe = self.get_city_data(city=city)
                all_dataframe = pd.concat([all_dataframe, tmp_dataframe])
                print(f"{city} 완료")

            except Exception as e:
                print(e)
                print(f"{city} 도시는 데이터가 없습니다")

            time.sleep(1)
        
        all_dataframe.reset_index(drop=True, inplace=True)

        return all_dataframe
        
    def make_dataframe(self, response_dict: dict) -> pd.DataFrame:
        df = pd.json_normalize(response_dict['response']['body']['items']['item'])
        return df
    
    def get_city_data(self, city: str) -> pd.DataFrame:

        page_no = self.info['pageNo']
        page_size = self.info['numOfRows']
        code = self.info['cityCode']

        # Request
        response_dict = self.request_api(params=self.info)
        
        # Count total to get info how many data are to be changed
        total_data_count = response_dict['response']['body']['totalCount']
        iter_count = (total_data_count // page_size)

        # Make fundamental dataFrame
        response_dataframe = self.make_dataframe(response_dict)
    
        # Iteration for full data
        for _ in range(iter_count):
            page_no += 1
            self.info['pageNo'] = page_no

            response_dict = self.request_api(params=self.info)
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
    

    def search_possible_city(self) -> pd.DataFrame:
        
        url = self.other_possible_list_url
        params={'serviceKey' : self.auth_key,
                '_type' : 'json',
                }

        response = requests.get(url, params=params)
        response_text = response.text
        response_json = json.loads(response_text)
        possible_city_df = pd.DataFrame(response_json['response']['body']['items']['item'])

        print('검색 가능한 도시 조회 완료\n')
        return possible_city_df
    

class RequestMetroData(RequestData):

    # https://data.kric.go.kr/rips/M_01_02/detail.do?id=183&service=convenientInfo&operation=stationInfo&page=2
    url = "https://openapi.kric.go.kr/openapi/convenientInfo/stationInfo"

    def __init__(self, auth_key: str,) -> None:
        self.auth_key = auth_key
        self.info = {
                    'serviceKey' : self.auth_key,
                    'format' : 'JSON',
                    'lnCd' : '1',
                    'railOprIsttCd' : "KR",
                    "railOprIsttCd" : "135",
                    "stinNm" : '용산',
                    }

    def request_api(self, params: Dict) -> Dict:
        """
        Get json data from Metrodata API server
        """

        # Request Localdata API server and get response
        response = requests.get(RequestMetroData.url, params=params)
        response_text = response.text

        # Need Exception handling when request data from API server(404, 500)

        # text to json
        response_dict = json.loads(response_text)
        return response_dict

    def get_apidata(self) -> pd.DataFrame:
        # page_index = info['pageIndex']
        # page_size = info['pageSize']
        # opnSvcId = info['opnSvcId']

        # Request API server
        response_dict = self.request_api(params=self.info)
        
        # Count total to get info how many data are to be changed
        try:
            total_data_count = response_dict['result']['header']['paging']['totalCount']
            if total_data_count < 1:
                raise EmptyDataFromResponse
        except Exception as e:
            print(f"{opnSvcId}서비스에는 {self.start_date}부터 {self.end_date}까지의 데이터가 없습니다")
            return None
        
        iter_count = (total_data_count // page_size)
        

        # Make fundamental dataFrame
        response_dataframe = self.make_dataframe(response_dict)
        
        # Iteration for full data
        for _ in range(iter_count):
            page_index += 1
            info['pageIndex'] = page_index

            response_dict = self.request_api(params=info)
            tmp_dataframe = self.make_dataframe(response_dict)
            
            # data concat
            response_dataframe = pd.concat([response_dataframe, tmp_dataframe])
            time.sleep(1)
        
        # Reset index after concatenation
        response_dataframe.reset_index(drop=True, inplace=True)
        print(f"{opnSvcId}서비스의 {self.start_date}부터 {self.end_date}의 데이터를 성공적으로 다운받았습니다")
        return response_dataframe

    def make_dataframe(self, response_dict: Dict) -> pd.DataFrame:
        pass
