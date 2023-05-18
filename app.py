from flask import Flask, request, jsonify, Response
from utils.db_connector import DBManagement
import json

app = Flask(__name__)

# def 

@app.route('/')
def index():
    return "Hello Flask"

@app.route('/db_check', methods=['GET'])
def db_check():
    if request.method == 'GET':

        # DB connect
        db_info_path = 'secret_key/db_info.txt'
        db_info_dict = {}

        with open(db_info_path, 'r') as f:
            for line in f.readlines():
                line_info = line.split('=')
                key = line_info[0].strip()
                val = line_info[1].strip()
                
                db_info_dict[key] = val

        dbm = DBManagement(**db_info_dict)
        print(f'성공적으로 MySQL {db_info_dict["database"]} 데이터베이스에 연결 완료')

        # 문자열 꼴로 들어온 것들을 list 로 변환하기 위함
        facilities_type = json.loads(request.form['facilities_type'])
        lat = json.loads(request.form['lat'])
        lon = json.loads(request.form['lon'])
        radius = json.loads(request.form['radius'])
        
        # variable for MySQL
        location = f"POINT({lon}, {lat})"
        radius_kilo = radius / 100000
        radius_query_list = []
        
        # Query 저장
        ## distance = meter
        for facility in facilities_type:
            if facility == 'bus':
                radius_query = f"""
                SELECT StationName AS Name, '{facility}' AS Kind, ST_Distance_Sphere({location}, coordinates) AS distance, lat, lon
                FROM {facility}
                WHERE ST_Contains(ST_Buffer({location}, {radius_kilo}), coordinates) AND ST_Distance_Sphere({location}, coordinates) < {radius}
                """ 
            elif facility == 'metro':
                radius_query = f"""
                SELECT StationName AS Name, '{facility}' AS Kind, ST_Distance_Sphere({location}, coordinates) AS distance, lat, lon
                FROM {facility}
                WHERE ST_Contains(ST_Buffer({location}, {radius_kilo}), coordinates) AND ST_Distance_Sphere({location}, coordinates) < {radius}
                """
            else:
                radius_query = f"""
                SELECT bplcNm AS Name, '{facility}' AS Kind, ST_Distance_Sphere({location}, coordinates) AS distance, lat, lon
                FROM {facility}
                WHERE ST_Contains(ST_Buffer({location}, {radius_kilo}), coordinates) AND ST_Distance_Sphere({location}, coordinates) < {radius}
                """
            
            radius_query_list.append(radius_query)

        radius_query = " UNION ALL".join(radius_query_list) + "ORDER BY distance;"
        
        # execute
        dbm.cursor.execute(radius_query)
        query_result = dbm.cursor.fetchall()
        total_count = len(query_result)

        # save body
        query_result_body = []
        for row in query_result:
            query_result_body.append( {'name': row[0],
                                       'kind': row[1],
                                       'distance':int(row[2]),
                                       'lat': row[3],
                                       'lon': row[4]
                                       }
                                    )

              
        # Response
        r = {'status' : 200,
            'total_count': total_count,
            'body' : query_result_body
            }
        
        resp = Response(json.dumps(r), mimetype='application/json', status=200)

        # dbm.cnx.close()
        print('데이터베이스 연결 종료')
        
        return resp





        # return radius_query
    else:
        return 'Not GET request', 404

if __name__ == "__main__":
    app.run(debug=True)


