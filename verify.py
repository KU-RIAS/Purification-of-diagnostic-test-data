import glob
from os import kill
import pandas as pd
import sys
import platform
import configparser

if __name__ == '__main__':
    
    OS_ENV = platform.system()

    output_file_paths = glob.glob(r'Data/Output/*.csv')
    print('The verification reports are stored in the "Verify" directory.')

    name_lst = []
    for i in range(len(output_file_paths)):
        if OS_ENV == "Windows":
            name_lst.append(output_file_paths[i].split('.')[-2].split('\\')[-1])
        elif OS_ENV == "Linux":
            name_lst.append(output_file_paths[i].split('.')[-2].split(r'/')[-1])
        else:
            sys.exit('Our program dose not support your OS...')

    for i in range(len(output_file_paths)):
        output_df = pd.read_csv('./'+output_file_paths[i], encoding='CP949')
        res_col = output_df.pop('RSLT1') # input_col_name으로 대체
        res_col = res_col.dropna()
        res_col.to_csv(rf'./Data/Verify/{name_lst[i]}_verify.txt', index=True, header=True, sep="\t")

        a = open(rf'./Data/Verify/{name_lst[i]}_verify.txt', mode='at', encoding='utf-8')
        a.writelines([f'\n\nThe number of total data: {output_df.shape[0]}\n',
                      f'The number of left data: {res_col.shape[0]}'])
        a.close()
