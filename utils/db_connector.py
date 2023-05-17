import mysql.connector
import pandas as pd
from typing import List
from tqdm import tqdm

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
    
    # Add quote for insert data to DB like address
    @staticmethod
    def replace_quote(value: str) -> str:
        if value == 'NULL':
            return value
        return '"{}"'.format(str(value).replace("'", "''"))

    # bring table information(columns, PK, etc)
    def set_table(self, table_path: str) -> pd.DataFrame:
        table_df = pd.read_csv(table_path)

        return table_df

    def get_columns(self, table_df: pd.DataFrame) -> List[str]:
        return table_df['Columns']


    def create_table(self, table_name: str, table: pd.DataFrame) -> None:
        # Check if the table exists
        self.cursor.execute(f"SHOW TABLES LIKE '{table_name}'")
        result = self.cursor.fetchone()

        # If the table does not exist, create table
        if not result:
            create_query = f"CREATE TABLE {table_name}\n" +"(\n"
            
            # Build create query
            iter_count = len(table)
            for i in range(iter_count):
                row = table.iloc[i,:-1]
                column_query = " ".join([i for i in row if isinstance(i, str)])
                create_query +=  column_query + ",\n"

            primary_query = ", ".join(list(table['Columns'][~table['Indexes'].isna()]))
            create_query += f"PRIMARY KEY({primary_query})\n);"

            self.cursor.execute(create_query)

        else:
            print(f"Table '{table_name}' already exists.")

    def insert_record(self, table_name:str, df:pd.DataFrame, columns: List[str]) -> None:
        base_value_query_list = []
        columns_string = ', '.join(columns)

        for _, row in df.iterrows():
            value_query = ", ".join([str(r) for r in row])
            # the last two value in row is the coordinates [.., lat, lon] or [.., x, y]
            point_query = f"ST_GeomFromText('POINT({row[-2]} {row[-1]})')"
            row_query = f"({value_query}, {point_query})"
            base_value_query_list.append(row_query)

        insert_query = f"""
                        INSERT INTO {table_name} ({columns_string})
                        VALUES {", ".join(base_value_query_list)};
                        """
        self.cursor.execute(insert_query)
        self.commit()
            
    # record단위로 commit하지 않음
    def update_record(self, table_name:str, df:pd.DataFrame, columns: List[str]) -> None:
        base_value_query_list = []
        columns_string = ', '.join(columns)

        for _, row in df.iterrows():
            value_query = ", ".join([str(r) for r in row])
            # the last two value in row is the coordinates [.., lat, lon] or [.., x, y]
            point_query = f"ST_GeomFromText('POINT({row[-2]} {row[-1]})')"
            row_query = f"({value_query}, {point_query})"
            base_value_query_list.append(row_query)

        
        # If duplicated : Only update values not including coordinates(POINT)
        # If not duplicated : insert all values including coordinates
        condition_query = ",\n".join([f"{col}=IF({col}=VALUES({col}), {col}, VALUES({col}))" for col in columns[:-1]])
        update_query = f"""
                        INSERT INTO {table_name} ({columns_string})
                        VALUES {", ".join(base_value_query_list)}
                        ON DUPLICATE KEY
                        UPDATE {condition_query};
                        """
        # print(update_query)
        self.cursor.execute(update_query)


    def delete_record(self, table_name: str, opnSfTeamCode: str, mgtNo: str, opnSvcId: str) -> None:
        delete_query = f"""
                        DELETE FROM {table_name}
                        WHERE opnSfTeamCode = '{opnSfTeamCode}' and mgtNo = '{mgtNo}' and opnSvcId = '{opnSvcId}';
                        """
        self.cursor.execute(delete_query)

    def create_spatial_index(self, table_name: str, coordinates_column: str) -> None:

                             
        query = f"CREATE SPATIAL INDEX spatial_index ON {table_name}({coordinates_column});"
        self.cursor.execute(query)

    
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


    def commit(self) -> None:
        self.cnx.commit()

<<<<<<< HEAD
    def drop_table(self, table_name: str) -> None:
        
        drop_query = f"drop table {table_name}"
        self.cursor.execute(drop_query)
        self.commit()

=======
    def create_table(self, table_name: str) -> None:
        # Check if the table exists
        self.cursor.execute(f"SHOW TABLES LIKE '{table_name}'")
        result = self.cursor.fetchone()

        # If the table does not exist, create it
        if not result:
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
            self.commit()
        else:
            print(f"Table '{table_name}' already exists.")


    # record단위로 commit하지 않음
    def update_record(self, table_name: str, row: pd.Series, columns: List[str]) -> None:
        # Query에 넣기 위한 문자열 모음
        columns_string = ', '.join(columns)

        # the last two value in row is the coordinates [.., lat, lon]
        value_query = ", ".join([DBManagement.sqlquote(r) for r in row[:-2]])
        point_query = f"ST_GeomFromText('POINT({row[-2]} {row[-1]})')"

        # If duplicated : Only update values not including coordinates(POINT)
        # If not duplicated : insert all values including coordinates
        update_query = ",\n".join([f"{col}=IF({col}=VALUES({col}), {col}, VALUES({col}))" for col in columns[:-1]])
        insert_query = f"""
                        INSERT INTO {table_name} ({columns_string})
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
>>>>>>> origin/main
        
    def insert_image_path(self, image_path: str, related_table_name: str, related_table_id: int) -> None:
        insert_query = f"""
                    INSERT IGNORE INTO image_table (image_path, related_table_name, related_table_id)
                    VALUES (%s, %s, %s)
                    """
        self.cursor.execute(insert_query, (image_path, related_table_name, related_table_id))
        self.commit()
