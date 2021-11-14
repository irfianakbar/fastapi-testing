import logging
import pandas as pd
from ast import literal_eval

from .dttot import dttot_prepro, wmd_prepro, UK_prepro, UN_prepro, OPEC_prepro, get_similarity
import warnings
warnings.filterwarnings("ignore")


def get_data_dttot():
    file_path = "../data/20210429140917.xlsx"
    df = dttot_prepro(file_path)
    return df

def get_data_wmd():
    df1 = pd.read_excel('../data/20181023091737.xlsx')
    df2 = pd.read_excel('../data/20181023091801.xlsx')
    df = wmd_prepro(df1, df2)
    return df

def get_data_un():
    df_UN = pd.read_excel('../data/UN_list.xlsx')
    df = UN_prepro(df_UN)
    return df

def get_data_uk():
    df_UK = pd.read_excel('../data/UK_list.xlsx')
    df = UK_prepro(df_UK)
    return df

def get_data_opec():
    df_OPEC = pd.read_excel('../data/OPEC_list.xlsx')
    df = OPEC_prepro(df_OPEC)
    return df

def data_cleaning(df):
    df['Tanggal Lahir'] = df['Tanggal Lahir'].str.split(";").str[0]
    dict_date = {"Jan" : "/01/", "Feb" : "/02/", "Mar" : "/03/", "Apr" : "/04/", "May" : "/05/", "Jun" : "/06/",
                "Jul" : "/07/", "Aug" : "/08/", "Sep" : "/09/", "Oct" : "/10/", "Nov" : "/11/", "Des" : "/12/",
                "Januari" : "/01/", "Februari" : "/02/", "Maret" : "/03/", "April" : "/04/", "Mei" : "/05/",
                "Juni" : "/06/", "July" : "/07/", "Agustus" : "/08/", "September" : "/09/", "Oktober" : "/10/",
                "November" : "/11/", "Desember" : "/12/"}

    dic_value = {r"\b{}\b".format(k): v for k, v in dict_date.items()}
    df["Tanggal Lahir"] = df["Tanggal Lahir"].replace(dic_value, regex=True)
    df["Tanggal Lahir"] = df["Tanggal Lahir"].str.replace(r"[a-zA-Z*()]",'')
    df["Tanggal Lahir"] = df["Tanggal Lahir"].str.replace(" ", "")
    df["Tanggal Lahir"] = pd.to_datetime(df["Tanggal Lahir"], format='%d/%m/%y', errors='ignore')
    df["Tanggal Lahir"] = df["Tanggal Lahir"].fillna("No Data")
    df['NIK'] = df['NIK'].str.strip()

    return df

def get_4_char_name(df):
    func = lambda x: ''.join([i[:4] for b in x for i in b.strip().split(' ')])
    df["4_char"] = df["nama_list"].apply(func)
    return df

def all_get_list_value(s):
    return [s]

def all_convert_to_list(df):
    # filter non list values and make it as a new DF
    df_filter = df[df["nama_list"] == "No Data"]
    # single string of each row to list
    df_filter["nama_list"] =  df_filter["Nama"].apply(all_get_list_value)
    # change string to list in the nama_list columns
    df_result = df_filter.combine_first(df)
    return df_result

def get_all_data():
    # DTTOT data processing
    df_dttot = get_data_dttot()
    df_wmd = get_data_wmd()
    df = pd.concat([df_dttot, df_wmd], ignore_index=True)
    # UK and UN data processing
    df_UK = get_data_uk()
    df_UN = get_data_un()
    df2 = pd.concat([df_UK, df_UN], axis=0, ignore_index=True)
    # OPEC data processing
    df_OPEC = get_data_opec()
    # concat all data
    df_all = pd.concat([df, df2], axis=0, ignore_index=True)
    df_all = pd.concat([df_all, df_OPEC], axis=0, ignore_index=True)
    # cleaning all Data
    df_all = df_all.fillna("no data")
    df_all = all_convert_to_list(df_all)
    df_all = data_cleaning(df_all)
    df_all = get_4_char_name(df_all)

    df_all['Tempat Lahir'] = df_all['Tempat Lahir'].str.lower()

    return df_all
