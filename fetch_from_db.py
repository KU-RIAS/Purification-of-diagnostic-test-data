
# 1. config파일에 등록된 정보를 바탕으로 파이썬 서버에서 DB서버로 접속 및 로그인
# 2. config파일에 등록된 정보를 바탕으로 필요한 테이블을 순차적으로 csv파일로 저장(input_dir에 저장)
    
    
import configparser
import pymssql
import getopt, sys
import pandas as pd
import pyodbc


# 인코딩 문제로 사용 x 

if __name__ == '__main__':
    
    # display = pd.options.display
    # display.max_columns = 100
    # display.max_rows = 200
    # display.max_colwidth = 199
    # display.width = None
    # pd.set_option('display.display.max_colwidth', -1)
    # pd.set_option('display.max_row', -1)
    # 최대 줄 수 설정
    pd.set_option('display.max_rows', 2000)
    # 최대 열 수 설정
    pd.set_option('display.max_columns', 2000)
    # 표시할 가로의 길이
    pd.set_option('display.width', 2000)
    
    config_path = r'./Data/config.ini'

    def main(argv):
            file_name = argv[0]
            global config_path

            try:
                opts, etc_args = getopt.getopt(argv[1:], "hc:d:", ["help", "config-path="])

            except getopt.GetoptError:  # 옵션지정이 올바르지 않은 경우
                print(file_name, '-c <config file path>')
                sys.exit(2)

            for opt, arg in opts:  # 옵션이 파싱된 경우
                if opt in ("-h", "--help"):  # HELP 요청인 경우 사용법 출력
                    print(file_name, '-c <config file path>')
                    sys.exit(0)

                elif opt in ("-c", "--config-path"):  # 인스턴명 입력인 경우
                    config_path = arg

    main(sys.argv)
    
    config = configparser.ConfigParser()
    config.read(config_path, encoding='utf-8')

    input_dir = config['path']['input_dir']
    table_name_list = config['Database']['table_name_list']
    db_server = config['Database']['server']
    db_name = config['Database']['db_name']
    user_name = config['Database']['user_name']
    passwd = config['Database']['password']
    
    if user_name =='None' and passwd == 'None':
        user_name = None
        passwd = None
        db_server = 'localhost'
    
    def fetch_data_from_db(_table_list, _server, _db_name, _user_name, _passwd, _dir_to_save):
        name_lst = _table_list.split(' ')
        print('Names of tables:', name_lst)
        for i in range(len(name_lst)):
            conn = pymssql.connect(server=_server, database=_db_name, user=_user_name, password=_passwd, charset='utf8')
            cursur = conn.cursor(as_dict=True)
            cursur.execute(f'select * from {name_lst[i]}')
            count = 0
            for row in cursur:
                if count is 200:
                    break;
                print(row['RSLT1'])
                print(row['RSLT1'].encode('ISO-8859-1').decode('euc-kr')) # 된다;;;;;
                
                count += 1
                
            # conn.cursor()
            # _input_df = pd.read_sql(f'select * from {name_lst[i]}', conn) 
            # print(_input_df.iloc[:300])
            # chunk_list = pd.read_sql(f'select * from {name_lst[i]}', conn, chunksize=chunk_size)  << conn.close()가 애매해짐.
            conn.close()
            

    fetch_data_from_db(table_name_list, db_server, db_name, user_name, passwd, input_dir)
    
    # def fetch_data_from_db(_table_list, _server, _db_name, _user_name, _passwd, _dir_to_save):
    #     name_lst = _table_list.split(' ')
    #     print('Names of tables:', name_lst)
    #     for i in range(len(name_lst)):
    #         # conn = pymssql.connect(server=_server, database=_db_name, user=_user_name, password=_passwd)
    #         conn = pyodbc.connect('Driver={SQL Server};'
    #                   'Server=_server;'
    #                   'Database=_db_name;'
    #                   'Trusted_Connection=yes;')
    #         cursor = conn.cursor()
    #         cursor.execute('SELECT * FROM database_name.table')

    #         for row in cursor:
    #             print(row)
    #         conn.close()
            

    # fetch_data_from_db(table_name_list, db_server, db_name, user_name, passwd, input_dir)