"""
Developer's e-mail address: yajumo93@gmail.com
"""

import re
import numpy as np


class FilteringAlg:

    def __init__(self, input_col_name, filter_df):
        self.input_col_name = input_col_name
        self.filter_df = filter_df
        
    def run_alg(self, input_df):
        FilteringAlg.__rm_garbage_data(input_df, self.input_col_name, self.filter_df); print("1/15")
        FilteringAlg.__sep_text1(input_df, self.input_col_name, self.filter_df); print("2/15")
        FilteringAlg.__rm_long_data(input_df, self.input_col_name, self.filter_df);
        FilteringAlg.__to_lower(input_df, self.input_col_name);
        FilteringAlg.__extract_sign(input_df, self.input_col_name, self.filter_df); print("3/15")
        FilteringAlg.__extract_unit_1(input_df, self.input_col_name, self.filter_df); print("4/15")
        FilteringAlg.__extract_comment(input_df, self.input_col_name, self.filter_df); print("5/15")
        FilteringAlg.__extract_cate_1_general(input_df, self.input_col_name, self.filter_df); print("6/15")
        FilteringAlg.__extract_cate_2_ratio(input_df, self.input_col_name, self.filter_df); print("7/15")
        FilteringAlg.__extract_cate_3_grade(input_df, self.input_col_name, self.filter_df); print("8/15")
        FilteringAlg.__extract_cate_4_type(input_df, self.input_col_name, self.filter_df); print("9/15")
        FilteringAlg.__sep_text2(input_df, self.input_col_name, self.filter_df); print("10/15")
        FilteringAlg.__extract_unit_2(input_df, self.input_col_name, self.filter_df); print("11/15")
        FilteringAlg.__pre_processing_num_extract(input_df, self.input_col_name, self.filter_df); print("12/15")
        FilteringAlg.__extract_number(input_df, self.input_col_name, self.filter_df); print("13/15")
        FilteringAlg.__extract_cate_3_additionally(input_df, self.input_col_name, self.filter_df); print("14/15")
        FilteringAlg.__rm_remain_useless_data(input_df, self.input_col_name, self.filter_df); print("15/15")
        return input_df
    
    # @classmethod
    # def run_alg(cls, input_df, input_col_name, filter_df):
    #     FilteringAlg.__rm_garbage_data(input_df, input_col_name, filter_df); print("1/15")
    #     FilteringAlg.__sep_text1(input_df, input_col_name, filter_df); print("2/15")
    #     FilteringAlg.__rm_long_data(input_df, input_col_name, filter_df); # print("3/17")
    #     FilteringAlg.__to_lower(input_df, input_col_name); # print("4/17")
    #     FilteringAlg.__extract_sign(input_df, input_col_name, filter_df); print("3/15")
    #     FilteringAlg.__extract_unit_1(input_df, input_col_name, filter_df); print("4/15")
    #     FilteringAlg.__extract_comment(input_df, input_col_name, filter_df); print("5/15")
    #     FilteringAlg.__extract_cate_1_general(input_df, input_col_name, filter_df); print("6/15")
    #     FilteringAlg.__extract_cate_2_ratio(input_df, input_col_name, filter_df); print("7/15")
    #     FilteringAlg.__extract_cate_3_grade(input_df, input_col_name, filter_df); print("8/15")
    #     FilteringAlg.__extract_cate_4_type(input_df, input_col_name, filter_df); print("9/15")
    #     FilteringAlg.__sep_text2(input_df, input_col_name, filter_df); print("10/15")
    #     FilteringAlg.__extract_unit_2(input_df, input_col_name, filter_df); print("11/15")
    #     FilteringAlg.__pre_processing_num_extract(input_df, input_col_name, filter_df); print("12/15")
    #     FilteringAlg.__extract_number(input_df, input_col_name, filter_df); print("13/15")
    #     FilteringAlg.__extract_cate_3_additionally(input_df, input_col_name, filter_df); print("14/15")
    #     FilteringAlg.__rm_remain_useless_data(input_df, input_col_name, filter_df); print("15/15")
    #     return input_df

    @staticmethod
    def __str_extract(extract_patt, input_data): # 부분 일치 내용 리턴
        if extract_patt is not re.compile('nan'):
            # print("필터:", extract_patt, "데이터:", input_data[0])
            if input_data is np.nan:
                return np.nan
            patt_search = extract_patt.search(input_data)
            if patt_search:  # 매치되는것이 있다면
                return patt_search.group()  # 매칭된 내용을 리턴
            else:
                return np.nan  # 없다면 nan
        else:
            return np.nan

    @staticmethod
    def __write_to_new_col_whole(input_df, input_index, col_name, name_to):
        input_df.loc[input_index, col_name] = name_to

    @staticmethod
    def __set_nan(input_df, input_index, col_name):
        input_df.loc[input_index, col_name] = np.nan

    @staticmethod
    def __patt_replace(input_df, input_col_name, input_index, input_data, compiled_patt, to_str):
        input_df.loc[input_index, input_col_name] = compiled_patt.sub(to_str, input_data)

    @staticmethod
    def __process_split_blank(filter_value):
        tmp_list = filter_value.split(' ')
        try:
            if tmp_list[1]:
                name_to = tmp_list[1]
                return name_to
        except IndexError:
            name_to = None
            return name_to

    @staticmethod
    def __check_kind_of_filter(filter_value):
        if filter_value is np.nan or type(filter_value) is float:
            return np.nan
        # print('(값, 타입)', filter_value, type(filter_value))
        tmp_lst = filter_value.split(' ') # 리스트나 딕셔너리 폼으로 리턴
        try:
            if tmp_lst[2]:  # 딕셔너리 폼, "A|B|C a b c" 알고리즘
                tmp_list1 = filter_value.split('|')
                tmp_string2 = tmp_list1[len(tmp_list1) - 1]
                tmp_list2 = tmp_string2.split(' ')
                tmp_list1.pop()
                cat_list = tmp_list1 + tmp_list2
                last_target_idx = int((len(cat_list) / 2) - 1)  # 추출한 요소 마지막 인덱스
                first_translate_idx = last_target_idx + 1  # 번역할 요소 첫 인덱스
                tmp_dict = dict()
                for i in range(last_target_idx + 1):
                    tmp_dict[cat_list[i]] = cat_list[first_translate_idx]
                    first_translate_idx += 1
                # print(tmp_dict)  # {'A': 'a', 'B': 'b', 'C': 'c'}
                return tmp_dict
        except IndexError:
            try:
                if tmp_lst[1]:  # 리스트 폼
                    tmp_dict2 = dict()
                    tmp_dict2[tmp_lst[0]] = tmp_lst[1]
                    return tmp_dict2
            except IndexError:
                # 단일 str 폼
                tmp_dict3 = dict()
                tmp_dict3[tmp_lst[0]] = np.nan
                return tmp_dict3

    @staticmethod
    def __filtering(input_df, input_col_name, filter_df, filter_title, tmp_col_name, is_full_match, is_replace_na):
        count = 0  # 필터의 카운트
        for filter_index, filter_value in filter_df[filter_title].iteritems():
            if filter_value is np.nan:
                continue
            checked_dict = FilteringAlg.__check_kind_of_filter(filter_value)
            if checked_dict is np.nan:
                continue
            for key in checked_dict.keys():  # key=filter, value=name_to
                new_col_name = fr'{tmp_col_name}{count}'
                input_df[new_col_name] = ''  # 초기화
                name_to = checked_dict[key]
                patt = re.compile(rf'{key}')
                for data_index in input_df.index:
                    input_data = input_df.at[data_index, input_col_name]
                    extracted_str = FilteringAlg.__str_extract(patt, input_data)
                    if is_full_match is True:
                        if extracted_str == input_data:
                            if name_to is np.nan:
                                name_to = extracted_str
                            if extracted_str is not np.nan:  # 존재하면
                                FilteringAlg.__write_to_new_col_whole(input_df, data_index, new_col_name, name_to)
                                name_to = np.nan
                                if is_replace_na is True:
                                    FilteringAlg.__set_nan(input_df, data_index, input_col_name)
                                else:
                                    FilteringAlg.__patt_replace(input_df, input_col_name, data_index, input_data, patt, '')
                            else:
                                name_to = np.nan
                    else:
                        if name_to is np.nan:
                            name_to = extracted_str
                        if extracted_str is not np.nan:  # 존재하면
                            FilteringAlg.__write_to_new_col_whole(input_df, data_index, new_col_name, name_to)
                            name_to = np.nan
                            if is_replace_na is True:
                                FilteringAlg.__set_nan(input_df, data_index, input_col_name)
                            else:
                                FilteringAlg.__patt_replace(input_df, input_col_name, data_index, input_data, patt, '')
                        else:
                            name_to = np.nan
                count += 1
        return count

    @staticmethod
    def __paste_tmp_cols(input_df, count_value, tmp_col_except_num, set_col_name, is_numeric=False):
        input_df[set_col_name] = ''  # 초기화
        tmp_text_col_name_lst = list()
        for i in range(count_value):  # row 개
            tmp_text_col_name_lst.append(fr'{tmp_col_except_num}{i}')
        if is_numeric is True:
            for i in range(len(input_df.index)):
                for j in range(len(tmp_text_col_name_lst)):
                    if input_df.loc[i, tmp_text_col_name_lst[j]] != '':
                        input_df.loc[i, set_col_name] = float(input_df.loc[i, tmp_text_col_name_lst[j]])
                        break
        else:
            input_df[set_col_name] = input_df[tmp_text_col_name_lst].apply(lambda row: ''.join(row.values.astype(str)),
                                                                           axis=1)  # 1 = 행단위
        input_df.drop(tmp_text_col_name_lst, axis='columns', inplace=True)
        input_df.apply(lambda x: x.str.strip(), axis=1)
        input_df[set_col_name] = input_df[set_col_name].apply(lambda x: np.nan if x == '' else x)
        # print(input_df)

    @staticmethod
    def __cat_columns(input_df, col_name1, col_name2, final_col_name, drop_old):
        input_df.loc[input_df[col_name1].isnull(), col_name1] = ''
        input_df.loc[input_df[col_name2].isnull(), col_name2] = ''
        input_df[final_col_name] = input_df[col_name1] + ' ' + input_df[col_name2]
        if drop_old is True:
            input_df.drop([col_name1, col_name2], axis='columns', inplace=True)
        input_df[final_col_name] = input_df[final_col_name].str.strip()
        input_df.loc[input_df[final_col_name] == '', final_col_name] = np.nan

    @staticmethod
    def __rm_debris(input_df, input_col_name, debris_lst):
        for rm in debris_lst:
            input_df.loc[input_df[input_col_name] == rm, input_col_name] = np.nan

    # 실행 메소드

    @staticmethod
    def __rm_garbage_data(input_df, input_col_name, filter_df):
        # pm
        for filter_index, filter_value in filter_df['rm_data_pm'].iteritems():
            patt = re.compile(rf'{filter_value}')
            for data_index in input_df.index:
                input_data = input_df.at[data_index, input_col_name]
                extracted_str = FilteringAlg.__str_extract(patt, input_data)
                if extracted_str is not np.nan:
                    input_df.loc[data_index, input_col_name] = np.nan
        input_df.dropna(inplace=True)
        input_df.reset_index(drop=True, inplace=True)

        # fm
        for filter_index, filter_value in filter_df['rm_data_fm'].iteritems():
            patt = re.compile(rf'{filter_value}')
            for data_index in input_df.index:
                input_data = input_df.at[data_index, input_col_name]
                extracted_str = FilteringAlg.__str_extract(patt, input_data)
                if extracted_str == input_data:  # full match
                    input_df.loc[data_index, input_col_name] = np.nan
        input_df.dropna(inplace=True)
        input_df.reset_index(drop=True, inplace=True)
        # print(input_df)

    @staticmethod
    def __sep_text1(input_df, input_col_name, filter_df):
        count = FilteringAlg.__filtering(input_df, input_col_name, filter_df,
                                         filter_title='separate_txt-1_pm', tmp_col_name='text1_',
                                         is_full_match=False, is_replace_na=True)
        FilteringAlg.__paste_tmp_cols(input_df, count, tmp_col_except_num='text1_', set_col_name='text1')

    @staticmethod
    def __rm_long_data(input_df, input_col_name, filter_df):
        pass

    @staticmethod
    def __to_lower(input_df, input_col_name):
        input_df[input_col_name] = input_df[input_col_name].str.lower()
        # input_df[input_col_name + '_ref'] = input_df[input_col_name]

    @staticmethod
    def __extract_sign(input_df, input_col_name, filter_df):
        count = FilteringAlg.__filtering(input_df, input_col_name, filter_df,
                                         filter_title='ext_sign_pm', tmp_col_name='sign_',
                                         is_full_match=False, is_replace_na=False)
        FilteringAlg.__paste_tmp_cols(input_df, count, tmp_col_except_num='sign_', set_col_name='sign')

    @staticmethod
    def __extract_unit_1(input_df, input_col_name, filter_df):
        count = FilteringAlg.__filtering(input_df, input_col_name, filter_df,
                                         filter_title='ext_unit-1_pm', tmp_col_name='unit-1_',
                                         is_full_match=False, is_replace_na=False)
        FilteringAlg.__paste_tmp_cols(input_df, count, tmp_col_except_num='unit-1_', set_col_name='unit1')

    @staticmethod
    def __extract_comment(input_df, input_col_name, filter_df):
        count_1 = FilteringAlg.__filtering(input_df, input_col_name, filter_df,
                                           filter_title='ext_comment_fm', tmp_col_name='comm-fm_',
                                           is_full_match=True, is_replace_na=False)
        FilteringAlg.__paste_tmp_cols(input_df, count_1, tmp_col_except_num='comm-fm_', set_col_name='comm_fm')
        count_2 = FilteringAlg.__filtering(input_df, input_col_name, filter_df,
                                           filter_title='ext_comment_pm', tmp_col_name='comm-pm_',
                                           is_full_match=False, is_replace_na=False)
        FilteringAlg.__paste_tmp_cols(input_df, count_2, tmp_col_except_num='comm-pm_', set_col_name='comm_pm')

        FilteringAlg.__cat_columns(input_df, 'comm_fm', 'comm_pm', 'comment', drop_old=True)

    @staticmethod
    def __extract_cate_1_general(input_df, input_col_name, filter_df):
        count_1 = FilteringAlg.__filtering(input_df, input_col_name, filter_df,
                                           filter_title='ext_cat1-general_pm', tmp_col_name='cat1-pm_',
                                           is_full_match=False, is_replace_na=False)
        FilteringAlg.__paste_tmp_cols(input_df, count_1, tmp_col_except_num='cat1-pm_', set_col_name='cat1_pm')

        count_2 = FilteringAlg.__filtering(input_df, input_col_name, filter_df,
                                           filter_title='ext_cat1-general_fm', tmp_col_name='cat1-fm_',
                                           is_full_match=True, is_replace_na=True)

        FilteringAlg.__paste_tmp_cols(input_df, count_2, tmp_col_except_num='cat1-fm_', set_col_name='cat1_fm')

        FilteringAlg.__cat_columns(input_df, 'cat1_fm', 'cat1_pm', 'category_general', drop_old=True)

        FilteringAlg.__rm_debris(input_df, input_col_name, ['', '.', '..'])

    @staticmethod
    def __extract_cate_2_ratio(input_df, input_col_name, filter_df):
        count = FilteringAlg.__filtering(input_df, input_col_name, filter_df,
                                         filter_title='ext_cat2-ratio', tmp_col_name='cat2_',
                                         is_full_match=False, is_replace_na=False)
        FilteringAlg.__paste_tmp_cols(input_df, count, tmp_col_except_num='cat2_', set_col_name='category_ratio')

    @staticmethod
    def __extract_cate_3_grade(input_df, input_col_name, filter_df):
        count_1 = FilteringAlg.__filtering(input_df, input_col_name, filter_df,
                                           filter_title='ext_cat3-grade_pm', tmp_col_name='cat3-pm_',
                                           is_full_match=False, is_replace_na=False)
        FilteringAlg.__paste_tmp_cols(input_df, count_1, tmp_col_except_num='cat3-pm_', set_col_name='cat3_pm')

        count_2 = FilteringAlg.__filtering(input_df, input_col_name, filter_df,
                                           filter_title='ext_cat3-grade_fm', tmp_col_name='cat3-fm_',
                                           is_full_match=True, is_replace_na=True)
        FilteringAlg.__paste_tmp_cols(input_df, count_2, tmp_col_except_num='cat3-fm_', set_col_name='cat3_fm')

        FilteringAlg.__cat_columns(input_df, 'cat3_pm', 'cat3_fm', 'category_grade', drop_old=True)

        FilteringAlg.__rm_debris(input_df, input_col_name, ['', '.', '..'])

    @staticmethod
    def __extract_cate_4_type(input_df, input_col_name, filter_df):
        count_1 = FilteringAlg.__filtering(input_df, input_col_name, filter_df,
                                           filter_title='ext_cat4-type_fm', tmp_col_name='cat4-fm_',
                                           is_full_match=True, is_replace_na=True)
        FilteringAlg.__paste_tmp_cols(input_df, count_1, tmp_col_except_num='cat4-fm_', set_col_name='cat4_fm')

        count_2 = FilteringAlg.__filtering(input_df, input_col_name, filter_df,
                                           filter_title='ext_cat4-type_pm', tmp_col_name='cat4-pm_',
                                           is_full_match=False, is_replace_na=False)
        FilteringAlg.__paste_tmp_cols(input_df, count_2, tmp_col_except_num='cat4-pm_', set_col_name='cat4_pm')

        FilteringAlg.__cat_columns(input_df, 'cat4_fm', 'cat4_pm', 'category_type', drop_old=True)

        FilteringAlg.__rm_debris(input_df, input_col_name, ['', '.', '..'])

    @staticmethod
    def __sep_text2(input_df, input_col_name, filter_df):
        count = FilteringAlg.__filtering(input_df, input_col_name, filter_df,
                                         filter_title='separate_txt-2_pm', tmp_col_name='text2_',
                                         is_full_match=False, is_replace_na=False)
        FilteringAlg.__paste_tmp_cols(input_df, count, tmp_col_except_num='text2_', set_col_name='text2')

        FilteringAlg.__rm_debris(input_df, input_col_name, ['', '.', '..'])

        FilteringAlg.__cat_columns(input_df, 'text1', 'text2', 'text', drop_old=True)

    @staticmethod
    def __extract_unit_2(input_df, input_col_name, filter_df):
        count = FilteringAlg.__filtering(input_df, input_col_name, filter_df,
                                         filter_title='ext_unit-2_pm', tmp_col_name='unit-2_',
                                         is_full_match=False, is_replace_na=False)
        FilteringAlg.__paste_tmp_cols(input_df, count, tmp_col_except_num='unit-2_', set_col_name='unit2')
        FilteringAlg.__cat_columns(input_df, 'unit1', 'unit2', 'unit_final', drop_old=True)

    @staticmethod
    def __pre_processing_num_extract(input_df, input_col_name, filter_df):
        # pm
        for filter_index, filter_value in filter_df['preprocessing_num_ext'].iteritems():
            patt = re.compile(rf'{filter_value}')
            for data_index in input_df.index:
                input_data = input_df.at[data_index, input_col_name]
                extracted_str = FilteringAlg.__str_extract(patt, input_data)
                if extracted_str is not np.nan:
                    FilteringAlg.__patt_replace(input_df, input_col_name, data_index, input_data, patt, '')

    @staticmethod
    def __extract_number(input_df, input_col_name, filter_df):
        count = FilteringAlg.__filtering(input_df, input_col_name, filter_df,
                                         filter_title='ext_num_pm', tmp_col_name='num_',
                                         is_full_match=False, is_replace_na=False)
        FilteringAlg.__paste_tmp_cols(input_df, count, tmp_col_except_num='num_',
                                      set_col_name='num', is_numeric=True)

    @staticmethod
    def __extract_cate_3_additionally(input_df, input_col_name, filter_df):
        count = FilteringAlg.__filtering(input_df, input_col_name, filter_df,
                                         filter_title='ext_cat3-additionally_fm', tmp_col_name='cat3-add_fm_',
                                         is_full_match=True, is_replace_na=True)
        FilteringAlg.__paste_tmp_cols(input_df, count, tmp_col_except_num='cat3-add_fm_',
                                      set_col_name='category_grade_add')
        FilteringAlg.__cat_columns(input_df, 'category_grade', 'category_grade_add',
                                   'category_grade_final', drop_old=True)

    @staticmethod
    def __rm_remain_useless_data(input_df, input_col_name, filter_df):
        # pm
        for filter_index, filter_value in filter_df['rm_remain_useless_pm'].iteritems():
            patt = re.compile(rf'{filter_value}')
            for data_index in input_df.index:
                input_data = input_df.at[data_index, input_col_name]
                extracted_str = FilteringAlg.__str_extract(patt, input_data)
                if extracted_str is not np.nan:
                    input_df.loc[data_index, input_col_name] = ""
        # fm
        for filter_index, filter_value in filter_df['rm_remain_useless_fm'].iteritems():
            patt = re.compile(rf'{filter_value}')
            for data_index in input_df.index:
                input_data = input_df.at[data_index, input_col_name]
                extracted_str = FilteringAlg.__str_extract(patt, input_data)
                if extracted_str == input_data:  # full match
                    input_df.loc[data_index, input_col_name] = np.nan
        FilteringAlg.__rm_debris(input_df, input_col_name, ['', '.', '..'])







