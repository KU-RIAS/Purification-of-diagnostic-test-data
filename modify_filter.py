
# modify the filter.

# python modify_filter.py -f ./Data/Filter/filter.csv -c "rm_data" -s "[+][+]"

# 입력 결과 출력 및 (y/n) 받기

# modified_filter dir must be empty!!!

# 더미 인풋셋 준비 

# filter_df의 받은 col_name에 해당하는 컬럼 따기 --> 컬럼 내에 존재하는 row 갯수 따기 (len_idx)

# tmp_df_list 만들기(). 요소의 갯수는 len_idx개 -->  

from numpy.lib.histograms import _histogram_bin_edges_dispatcher
import pandas as pd
import numpy as np
import run
import glob
from App.algorithm import FilteringAlg as Alg
from App.multi_process import MultiProcess
import os
import multiprocessing
import platform
import datetime


#####################
# USER INPUT PARAMS #
#####################

OS_ENV = platform.system()

multiprocessing.freeze_support()

filter_path = r'./Data/Filter/filters.csv'
TARGET_COL_NAME = "ext_comment_pm"
# TARGET_COL_NAME = 'ext_cat1-general_pm'
INPUT_STR = 'myTest.+'
INPUT_COL_NAME = 'RSLT1'
NUM_OF_CORES_TO_USE = 1
CHUNK_SIZE = 1000

filter_df = pd.read_csv(filter_path, encoding='CP949') # 34 x 21
# print(filter_df)
col_name_list = filter_df.columns.values.tolist()
col_name_dict = {string:i for i,string in enumerate(col_name_list)} # 컬럼명을 key로 받아 pos_num 출력
TARGET_COL_INDEX = col_name_dict[TARGET_COL_NAME]

tmp_output_dir = r'./tmp_dir/'

# index_list = list(filter_df[TARGET_COL_NAME].values)

poped_col = filter_df.pop(TARGET_COL_NAME)
# print(filter_df) # (34, 20)

# filter_df.to_csv(tmp_output_dir+'filter.csv', encoding='CP949')
# exit(0)

is_nan_lst = poped_col.apply(lambda row : row is np.nan).to_list()
last_idx = [(i, element) for i, element in enumerate(is_nan_lst) if element is True][0][0] # 첫번째 NaN이 뜨는곳이 last_idx
poped_col[last_idx] = 'tmp'

# 한칸 밀기
for i in range(last_idx):
    cur_last_idx = last_idx - i
    cur_pre_last_idx = cur_last_idx - 1
    poped_col[cur_last_idx] = poped_col[cur_pre_last_idx]
    
poped_col.iloc[0] = INPUT_STR

test_input_name_lst = []
test_inputs_path = glob.glob(r'./Data/Input/*.csv') # test input dir로 수정 할것

run.set_file_name_list_from_dir(test_inputs_path, test_input_name_lst)

# 필터 인덱스의 의미?? 0~last-1의 넘버링이 붙음. 각 넘버의 의미는 newStr값이 들어간 위치임..
# 즉, 튜플의 첫요소로 newStr의 위치를 정할 수 있고, 두번째 요소로 어떤 튜플을 정할지 결정할 수 있다.
# 만약 두번째 값이 중복이라면,,, 가장 인덱스 넘버가 큰 튜플을 선택하도록 설정한다. 

filter_performance = []
filter_performance_with_idx = []

# print(filter_df)
for target_idx in range(last_idx): # 필터는 last_index개 만큼 생긴다
    print('\033[96m'+f'################ Progress .... {target_idx}/{last_idx} ################'+'\033[0m')
    filter_df.insert(loc=col_name_dict[TARGET_COL_NAME], 
                     column=TARGET_COL_NAME,
                     value=poped_col)
    # print(filter_df[TARGET_COL_NAME])
    alg = Alg(INPUT_COL_NAME, filter_df)
    
    sum_of_remain_val = 0
    for i in range(len(test_inputs_path)):
        chunk_list = pd.read_csv(test_inputs_path[i], encoding='CP949', iterator=True, chunksize=CHUNK_SIZE)
        for chunk_df in chunk_list:
            if OS_ENV == "Windows" and NUM_OF_CORES_TO_USE == 1: # window환경은 modify 작업 멀티프로세스 x 
                output_df = alg.run_alg(chunk_df)
            elif OS_ENV == "Windows":
                try:
                    raise Exception
                except:
                    exit('\033[31m'+'When executing "modify_filter.py", multiprocessing is not possible on Windows OS.\nSet variable "NUM_OF_CORES_TO_USE" to 1 and try again.'+'\033[0m')
            else:     
                output_df = MultiProcess.multi_process(alg.run_alg, chunk_df, NUM_OF_CORES_TO_USE)
                
            res_col = output_df.pop(INPUT_COL_NAME)
            res_col = res_col.dropna()
            the_number_of_remain_val = res_col.shape[0]
            sum_of_remain_val += the_number_of_remain_val
            
        # test_input_df = pd.read_csv(test_inputs_path[i], encoding='cp949') # 청크 적용해 줄 것 
        # print(test_input_df.shape) # (1312, 24)
        
        # if OS_ENV == "Windows" and NUM_OF_CORES_TO_USE == 1: # window환경은 modify 작업 멀티프로세스 x 
        #     output_df = alg.run_alg(test_input_df)
        # elif OS_ENV == "Windows":
        #     try:
        #         raise Exception
        #     except:
        #         exit('\033[31m'+'When executing "modify_filter.py", multiprocessing is not possible on Windows OS.\nSet variable "NUM_OF_CORES_TO_USE" to 1 and try again.'+'\033[0m')
              
        # print(fr"Progress - {i + 1}/{len(test_input_name_lst)} - Start")
        # output_df = MultiProcess.multi_process(alg.run_alg, test_input_df, NUM_OF_CORES_TO_USE)
        # print(filter_df[TARGET_COL_NAME])
        # exit(0)
        
        # print(filter_df.shape)
        # filter_df.to_csv(tmp_output_dir+'filter.csv', encoding='CP949')
        # exit(0)
        
        # output_df.to_csv(tmp_output_dir+f'{test_input_name_lst[i]}.csv', encoding='CP949') # 아웃풋 확인해야 함. 퍼포먼스 점수에 0점이 이상함. 청크여부도 확인 어디서 적용되는지
        # exit(0)
        # print(fr"Progress - {i + 1}/{len(test_input_name_lst)} - End")
        
        # res_col = output_df.pop(INPUT_COL_NAME)
        # res_col = res_col.dropna()
        # the_number_of_remain_val = res_col.shape[0]
        # sum_of_remain_val += the_number_of_remain_val
    
    # filter_performance.append((target_idx, sum_of_remain_val))
    filter_performance.append(sum_of_remain_val)
    filter_performance_with_idx.append((target_idx, sum_of_remain_val))
    
    filter_df.pop(TARGET_COL_NAME)
    
    # print(poped_col)
    
    # swap
    tmp = poped_col[target_idx]
    poped_col[target_idx] = poped_col[target_idx + 1]
    poped_col[target_idx + 1] = tmp

min_val = min(filter_performance)
filter_performance_with_idx.reverse()

result_idx = 0
for i in range(len(filter_performance_with_idx)):
    if min_val == filter_performance_with_idx[i][1]:
        result_idx = filter_performance_with_idx[i][0]
        break

result_idx += 1 # 0번째 보정

# print(f"우리는 제안한다 {INPUT_STR}을 {result_idx}에 넣는것을.")
print('필터에 대한 퍼포먼스 결과는 아래와 같다')
print('format: (index, the_number of unfiltered data) ')
print(filter_performance_with_idx)
print(f"We propose to put '{INPUT_STR}' in the index: {result_idx}.")

print(last_idx) # 25 = input_str의 값을 가리키고 있다.
poped_col.iloc[last_idx] = np.NaN

for i in range(last_idx - result_idx):
    cur_last_idx = last_idx - i
    cur_pre_last_idx = cur_last_idx - 1
    poped_col[cur_last_idx] = poped_col[cur_pre_last_idx]

poped_col.iloc[result_idx] = INPUT_STR

filter_df.insert(loc=col_name_dict[TARGET_COL_NAME], 
                     column=TARGET_COL_NAME,
                     value=poped_col)

print(filter_df[TARGET_COL_NAME])

now = datetime.datetime.now()
now_date_time = now.strftime('%Y-%m-%d-%H'+'h'+'%M'+'m'+'%S'+'s')

filter_df.to_csv(tmp_output_dir+f'filter_modified_{now_date_time}.csv', encoding='CP949')

# filter_df index 열 제거 가능하면 제거해줄것 

exit(0)
