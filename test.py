"""
python -m pip install pymssql

"""
import pymssql
import pandas as pd
import os
import glob
import platform
import sys
import multiprocessing as mp

# # conn = pymssql.connect(server="DB IP", user="계정", password="비번", database="DB명")
# conn = pymssql.connect(server="localhost", user="jun", password="1234", database="LargeDB")
# cursor = conn.cursor()
#
# cursor.execute("select * from student")
#
# row = cursor.fetchone()
#
# while row:
#     print(f"mem_id = {row[0]}, mem_name = {row[1]}")
#     row = cursor.fetchone()
#
# conn.close()


# def call_data(query):
#     server = '서버주소'
#     database = 'master'
#     username = '아이디'
#     passward = '비밀번호'
#     # conn = pymssql.connect(server="localhost", user="jun", password="1234", database="resTest")
#     conn = pymssql.connect(server="localhost", database="resTest")
#     v = pd.read_sql(query, conn)
#     conn.close()
#     return v
#
# data = pd.DataFrame(call_data(f'select * from result2'))
#
# print(data)
# print(type(data))

# print(os.getcwd())
# os.chdir('./Data/Input/')
# print(os.getcwd())

# display = pd.options.display
# display.max_columns = 100
# display.max_rows = 100
# display.max_colwidth = 199
# display.width = None
#
# o_df = pd.read_csv(r'./Data/Output/output.csv', encoding='CP949')
# print(o_df)


# input_d = glob.glob(r'./Data/Input/*.csv')
# print(range(len(input_d)))
# for i in range(len(input_d)):
#     print(i)
#
# # print(input_d[0].split(r'\\'))
#
# print(input_d[0].split('.')[-2].split('\\')[-1])
#
# win_path = input_d[0].split('.')[-2].split('\\')[-1]
# ubuntu_path = input_d[0].split('.')[-2].split(r'/')[-1]
#
# print("우분투", ubuntu_path)
#
# my_os = platform.system()
#
# if my_os == "Windows":
#     # sys.exit('Our program dose not support your OS...')
#     print("윈도우")
#
# else:
#     print("리눅스")


# tmp_path = './Data/Output/'
# output_d = glob.glob(tmp_path+'*')
# # output_d.append('1')
# print(output_d)
# if output_d:
#     print("찬 리스트")
# else:
#     print("빈리스트")


# def test(my, _my):
#     print('my:', my)
#     print('_my:', _my)
#
#
# test('my', '_my')





# config_path = r'./Data/config.ini'
# DB_connection = False
# save_to_output_dir = False
#
#
# def main():
#     global config_path
#     global DB_connection
#     global save_to_output_dir
#
#     config_path = '수정한 path'
#     DB_connection = True
#     save_to_output_dir = True
#
# main()
#
# print(config_path)
# print(DB_connection)
# print(save_to_output_dir)


import multiprocessing as mp
import time

# def worker():
#     proc = mp.current_process()
#     print(proc.name)
#     print(proc.pid)
#     time.sleep(5)
#     print("SubProcess End")
#
#
# if __name__ == "__main__":
#     # main process
#     proc = mp.current_process()
#     print(proc.name)
#     print(proc.pid)
#
#     # process spawning
#     p = mp.Process(name="SubProcess", target=worker)
#     p.start()
#
#     q = mp.Process(name="SubProcess", target=worker)
#     q.start()
#
#     print("MainProcess End")



# from multiprocessing import Pool

# def work(x):
#     print(x)

# if __name__ == "__main__":
#     pool = Pool(4)
#     data = range(1, 100)
#     pool.map(work, data)



# import chardet
# import pandas as pd

# filename = r'./Data/Input/input_u8.csv'
# with open(filename, 'rb') as f:
#     result = chardet.detect(f.readline())
#     print(result['encoding'])


# str_list = ['a', 'b', 'c']

# my_dict = {string:i for i,string in enumerate(str_list)}

# data_frame.columns.values.tolist()





# tmp = [1, 2, 3, 3, 3, 4, 5, 5, 5]

# print(max(tmp))

# print(tmp.index(5))






# a = '100'
# b = 100.0
# c = 'hundreds'

# aa = float(a)
# bb = float(b)
# # cc = float(c)

# print(aa, type(aa))
# print(bb, type(bb))
# # print(cc, type(cc))

# c = 'hundreds'

# try:
#     cc = float(c)
#     print(cc, type(cc))
# except ValueError:
#     print("str -> numeric err")



# import datetime

# now = datetime.datetime.now()
# now_date_time = now.strftime('%Y-%m-%d-%H'+'h'+'%M'+'m'+'%S'+'s')

# print(now_date_time)



