import mysql.connector
import pandas as pd
import time
from typing import List

class DBManagement:
    def __init__(self, host: str, user: str, password: str, database: str) -> None:
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.cnx = mysql.connector.connect(host=self.host,
                                    user=self.user,
                                    password=self.password,
                                    database=self.database)
        self.cursor = self.cnx.cursor()
        self.__table_name = None

    @staticmethod
    def sqlquote(value: str) -> str:
        if value == 'NULL':
            return value
        return '"{}"'.format(str(value).replace("'", "''"))
    
    def table_size(self, table_name: str) -> int:
        size_query = f"""
                    SELECT COUNT(*)
                    FROM {table_name}
                    """
        self.cursor.execute(size_query)

        # query 결과값 가져오기
        result = self.cursor.fetchall()[0][0]

        self.commit()
        return result


    # getter    
    @property
    def table_name(self) -> str:
        return self.__table_name
    
    # setter
    @table_name.setter
    def table_name(self, table_name: str) -> None:
        self.__table_name = table_name


    def commit(self) -> None:
        self.cnx.commit()

    def create_table(self, table_name: str) -> None:
        self.cursor.execute(f"""CREATE TABLE {table_name} 
                        (
                            opnSfTeamCode CHAR(7) NOT NULL,
                            mgtNo VARCHAR(40) NOT NULL,
                            opnSvcId CHAR(10) NOT NULL,
                            updateGbn CHAR(1),
                            updateDt DATETIME,
                            bplcNm VARCHAR(200),
                            sitePostNo VARCHAR(7),
                            siteWhlAddr VARCHAR(500),
                            rdnPostNo VARCHAR(7),
                            rdnWhlAddr VARCHAR(500),
                            trdStateGbn VARCHAR(2),
                            dtlStateGbn VARCHAR(4),
                            x CHAR(20),
                            y CHAR(20),
                            lastModTs DATETIME,
                            uptaeNm VARCHAR(100),
                            coordinates POINT,

                        PRIMARY KEY(opnSfTeamCode, mgtNo, opnSvcId)
                        )
                        """)
        self.table_name = table_name

    # record단위로 commit하지 않음
    def update_record(self, row: pd.Series, columns: List[str]) -> None:

        # Query에 넣기 위한 문자열 모음
        columns_string = ', '.join(columns)

        # the last two value in row is the coordinates [.., lat, lon]
        value_query = ", ".join([DBManagement.sqlquote(r) for r in row[:-2]])
        point_query = f"ST_GeomFromText('POINT({row[-2]} {row[-1]})')"

        # If duplicated : Only update values not including coordinates(POINT)
        # If not duplicated : insert all values including coordinates 
        update_query = ",\n".join([f"{col}=IF({col}=VALUES({col}), {col}, VALUES({col}))" for col in columns[:-1]])
        insert_query = f"""
                        INSERT INTO {self.table_name} ({columns_string})
                        VALUES ({value_query}, {point_query})
                        ON DUPLICATE KEY 
                        UPDATE {update_query};
                        """
        
        self.cursor.execute(insert_query)

    def delete_record(self, opnSfTeamCode: str, mgtNo: str, opnSvcId: str) -> None:
        delete_query = f"""
                        DELETE FROM {self.table_name}
                        WHERE opnSfTeamCode = '{opnSfTeamCode}' and mgtNo = '{mgtNo}' and opnSvcId = '{opnSvcId}';
                        """
        self.cursor.execute(delete_query)