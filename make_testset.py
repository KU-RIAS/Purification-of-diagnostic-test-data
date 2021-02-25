import pandas as pd
import glob
import os
import run


# 1. test set에 대한 데이터 전처리 과정
    # 한바퀴 돌면서 row 하나 하나에 대해 numeric data 삭제하기(행을 날리는 방식)
    # 중복된 str 커트 df.drop_duplicate()
# 2. 받을 파리미터: 빅 인풋파일 path, 컬럼네임, 청크사이즈


BIG_INPUT_DIR = r'./Modify/TestSet/BigInputs/*'
SAVE_DIR = r'Modify/TestSet/TestSets'

col_name = 'RSLT1'
chunk_size = 800

c = 'hundreds'
try:
    cc = float(c)
    print(cc, type(cc))
except ValueError:
    print("str -> numeric err")

big_input_list = glob.glob(BIG_INPUT_DIR)
input_name_list = []
run.set_file_name_list_from_dir(big_input_list, input_name_list)
print(input_name_list)


for i in range(len(big_input_list)):
    chunk_list = pd.read_csv(big_input_path, encoding='CP949', iterator=True, chunksize=chunk_size)
    for chunk in chunk_list:
        if not os.path.exists(output_path):
            chunk.to_csv(output_path, header=True, index=False, mode='w', encoding='CP949')
        else:
            chunk.to_csv(output_path, header=False, index=False, mode='a', encoding='CP949')

    


# chunk_list = pd.read_csv(big_input, encoding='CP949', iterator=True, chunksize=chunk_size)
# print(fr"Progress - {i + 1}/{len(name_lst)} - Start")
# for chunk in chunk_list:
#     start_system(chunk, _filter_df, _table_name=name_lst[i]) 
# print(fr"Progress - {i + 1}/{len(name_lst)} - End")

