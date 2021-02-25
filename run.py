import os
import pandas as pd
from App.algorithm import FilteringAlg as Alg
import multiprocessing
import time
# from App.multi_thread import MultiThread
from App.multi_process import MultiProcess
import configparser
import pymssql
import glob
import platform
import sys
import getopt

OS_ENV = platform.system()

def set_file_name_list_from_dir(_inputs_path_lst, _name_lst): # 모듈용 
        for i in range(len(_inputs_path_lst)):
            if OS_ENV == "Windows":
                _name_lst.append(_inputs_path_lst[i].split('.')[-2].split('\\')[-1])
            elif OS_ENV == "Linux":
                _name_lst.append(_inputs_path_lst[i].split('.')[-2].split(r'/')[-1])
            else:
                sys.exit('Our program can be used in Windows or Linux environment...')

if __name__ == '__main__':

    display = pd.options.display
    display.max_columns = 100
    display.max_rows = 200
    display.max_colwidth = 199
    display.width = None

    pd.set_option('display.max_row', 500)

    pass_num = 0

    # OS_ENV = platform.system()
    NUM_OF_CORES = multiprocessing.cpu_count()

    ########################
    # init by commend line #
    ########################

    config_path = r'./Data/config.ini'
    DB_connection = 'False'
    # save_to_output_dir = False

    def main(argv):
        file_name = argv[0]
        global config_path
        global DB_connection

        try:
            opts, etc_args = getopt.getopt(argv[1:], "hc:d:", ["help", "config-path=", "db-connection="])

        except getopt.GetoptError:  # 옵션지정이 올바르지 않은 경우
            print(file_name, '-c <config file path> -d <is db connection True/False>')
            sys.exit(2)

        for opt, arg in opts:  # 옵션이 파싱된 경우
            if opt in ("-h", "--help"):  # HELP 요청인 경우 사용법 출력
                print(file_name, '-c <config file path>')
                sys.exit(0)

            elif opt in ("-c", "--config-path"):  # 인스턴명 입력인 경우
                config_path = arg
            elif opt in ("-d", "--db-connection"):
                DB_connection = arg

    main(sys.argv)

    # DB_connection = 'True' # 테스트용 
    if DB_connection == 'True':
        DB_connection = True
    elif DB_connection == 'False':
        DB_connection = False
    else:
        print("The 'db-connection' must be True or False.")
        sys.exit(2)

    print('Working dir :', os.getcwd())
    print('the number of cores in your computer', NUM_OF_CORES)

    def config_generator():
        _config = configparser.ConfigParser()
        _config['system'] = {}
        _config['system']['title'] = 'The system processing a result data of a medical examination'
        _config['system']['version'] = '1.0'
        _config['system']['cores_to_use'] = '1'

        _config['path'] = {}
        _config['path']['input_dir'] = './Data/Input/'
        _config['path']['output_dir'] = './Data/Output/'
        _config['path']['filter_file_path'] = './Data/Filter/filters.csv'

        _config['data'] = {}
        _config['data']['input_column_name'] = 'RSLT1'
        _config['data']['chunk_size'] = '400'
        # _config['data']['input_encoding_format'] = 'cp949'
        # _config['data']['filter_encoding_format'] = 'cp949'

        _config['Database'] = {}
        _config['Database']['server'] = 'localhost'
        _config['Database']['db_name'] = 'resTest'
        _config['Database']['user_name'] = 'id'
        _config['Database']['password'] = 'password'
        _config['Database']['table_name_list'] = 'result result3 result2'

        with open('./Data/config.ini', 'w', encoding='utf-8') as configfile:
            _config.write(configfile)

    # config_generator()  # 차후 빼주고, 유저 인풋 지향적으로 config 내용 체인지
    config = configparser.ConfigParser()
    config.read(config_path, encoding='cp949')
    # exit(0)

    #######################
    # init by config file #
    #######################

    filter_path = config['path']['filter_file_path']
    input_dir = config['path']['input_dir']
    output_dir = config['path']['output_dir']
    table_name_list = config['Database']['table_name_list']
    input_col_name = config['data']['input_column_name']
    NUM_OF_CORES_TO_USE = int(config['system']['cores_to_use'])
    chunk_size = int(config['data']['chunk_size'])
    # input_encoding_format = config['data']['input_encoding_format']

    db_server = config['Database']['server']
    db_name = config['Database']['db_name']
    user_name = config['Database']['user_name']
    passwd = config['Database']['password']

    if user_name =='None' and passwd == 'None':
        user_name = None
        passwd = None
        db_server = 'localhost'

    # db에서 실시간으로 데이터를 받아와서 처리 --> pymssql 인코딩 문제
    def call_data_from_db(_table_list, _server, _db_name, _user_name, _passwd, _filter_df):
        name_lst = _table_list.split(' ')
        print('Names of tables:', name_lst)
        conn = pymssql.connect(server=_server, database=_db_name, user=_user_name, password=_passwd)
        for i in range(len(name_lst)):
            chunk_list = pd.read_sql(f'select * from {name_lst[i]}', conn, chunksize=chunk_size)
            print(fr"Progress - {i + 1}/{len(name_lst)} - Start")
            for chunk in chunk_list:
                print(chunk)
                # exit(0)
                start_system(chunk, _filter_df, _table_name=name_lst[i]) 
            print(fr"Progress - {i + 1}/{len(name_lst)} - End")
        conn.close()
        
    # def set_file_name_list_from_dir(_inputs_path, _name_lst):
    #     for i in range(len(_inputs_path)):
    #         if OS_ENV == "Windows":
    #             _name_lst.append(_inputs_path[i].split('.')[-2].split('\\')[-1])
    #         elif OS_ENV == "Linux":
    #             _name_lst.append(_inputs_path[i].split('.')[-2].split(r'/')[-1])
    #         else:
    #             sys.exit('Our program can be used in Windows or Linux environment...')

    def call_data_from_input_dir(_filter_df):
        # 인풋 디렉토리 순회 -> path list 확보 -> name 따기 -> run & save in output_dir
        name_lst = []
        inputs_path = glob.glob(r'./Data/Input/*.csv')
        
        set_file_name_list_from_dir(inputs_path, name_lst)

        for i in range(len(inputs_path)):
            chunk_list = pd.read_csv(inputs_path[i], encoding='CP949', iterator=True, chunksize=chunk_size)
            print(fr"Progress - {i + 1}/{len(name_lst)} - Start")
            for chunk in chunk_list:
                start_system(chunk, _filter_df, _table_name=name_lst[i]) 
            print(fr"Progress - {i + 1}/{len(name_lst)} - End")
            # print("Delete used input_data")
            # os.remove(inputs_path[i])
            

    def start_system(_input_df, _filter_df, _table_name, _num_of_core=NUM_OF_CORES_TO_USE, _output_dir=output_dir):
        save_to_file_name = _table_name+'_output.csv'
        output_path = fr'{_output_dir}{save_to_file_name}'

        print('Foreparts your input data:\n', _input_df.head())
        print('Foreparts your filter data:\n', filter_df.head())

        print('The number of cores in your computer:', NUM_OF_CORES)
        print('The number of cores this program will use:', NUM_OF_CORES_TO_USE)

        start = time.time()

        _input_df[input_col_name + '_Original'] = _input_df[input_col_name]
        input_df = _input_df[[input_col_name + '_Original', input_col_name]]
        
        alg = Alg(input_col_name, _filter_df)

        print('Starting the program...')

        # try:
        #     if not os.path.exists(output_path):
        #         MultiProcess.multi_process(alg.run_alg, input_df, _num_of_core).to_csv(output_path, 
        #                                                                               header=True, 
        #                                                                               index=False, 
        #                                                                               mode='w',
        #                                                                               encoding="cp949")
        #     else:
        #         MultiProcess.multi_process(alg.run_alg, input_df, _num_of_core).to_csv(output_path, 
        #                                                                                 header=False, 
        #                                                                                 index=False, 
        #                                                                                 mode='a',
        #                                                                                 encoding="cp949")
        # except UnicodeEncodeError:
        #     global pass_num
        #     print(pass_num, '회 예외 발생')
        #     pass_num += 1
            
        #     pass
        
        if not os.path.exists(output_path):
            MultiProcess.multi_process(alg.run_alg, input_df, _num_of_core).to_csv(output_path, 
                                                                                    header=True, 
                                                                                    index=False, 
                                                                                    mode='w',
                                                                                    encoding='CP949')
            # MultiProcess.multi_process(alg.run_alg, input_df, _num_of_core).to_excel(output_path,
            #                                                                          header=True,
            #                                                                          index=False,
            #                                                                          mode='w',
            #                                                                          encoding='utf-8')
        else:
            MultiProcess.multi_process(alg.run_alg, input_df, _num_of_core).to_csv(output_path, 
                                                                                    header=False, 
                                                                                    index=False, 
                                                                                    mode='a',
                                                                                    encoding='CP949') # 차후 이걸로 읽어서 데이터베이스에 원하는 인코딩 형식으로 save
            # MultiProcess.multi_process(alg.run_alg, input_df, _num_of_core).to_excel(output_path,
            #                                                                          header=False,
            #                                                                          index=False,
            #                                                                          mode='a',
            #                                                                          encoding='utf-8') # 차후 이걸로 읽어서 데이터베이스에 원하는 인코딩 형식으로 save


        print("Elapsed time :", time.time() - start)
        
        
    # ==========================================
    # INPUT 데이터 프레임 세팅 및 알고리즘 적용 |
    # ==========================================

    # input_dir에 *.csv가 아닌 파일이 있다면 에러출력 추가

    if glob.glob(output_dir+'*'):
        print('ERR: output directory must be empty...')
        sys.exit(2)

    filter_df = pd.read_csv(filter_path, encoding='CP949')
    
    # multiprocessing.freeze_support()
    if DB_connection is True:
        input_dir = None
        print('Because "DB_connection" is True, "Input" directory is deactivated.')
        call_data_from_db(table_name_list, db_server, db_name, user_name, passwd, filter_df)
    else:
        print('Because "DB_connection" is False, "Input" directory is activated.')
        call_data_from_input_dir(filter_df)



