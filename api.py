import json
import logging
from typing import Optional
import pandas as pd
import uvicorn
from fastapi import BackgroundTasks, FastAPI

from service.dttot import dttot_prepro, wmd_prepro, UK_prepro, UN_prepro, OPEC_prepro, get_similarity
import warnings
warnings.filterwarnings("ignore")

app = FastAPI()

def get_data_dttot():
    file_path = "./data/20210429140917.xlsx"
    df = dttot_prepro(file_path)
    return df

def get_data_wmd():
    df1 = pd.read_excel('./data/20181023091737.xlsx')
    df2 = pd.read_excel('./data/20181023091801.xlsx')
    df = wmd_prepro(df1, df2)
    return df

def get_data_un():
    df_UN = pd.read_excel('./data/UN_list.xlsx')
    df = UN_prepro(df_UN)
    return df

def get_data_uk():
    df_UK = pd.read_excel('./data/UK_list.xlsx')
    df = UK_prepro(df_UK)
    return df

def get_data_opec():
    df_OPEC = pd.read_excel('./data/OPEC_list.xlsx')
    df = OPEC_prepro(df_OPEC)
    return df

def get_all_data():
    df_dttot = get_data_dttot()
    df_wmd = get_data_wmd()
    df = pd.concat([df_dttot, df_wmd], ignore_index=True)

    df_UK = get_data_uk()
    df_UN = get_data_un()
    df2 = pd.concat([df_UK, df_UN], axis=0, ignore_index=True)

    df_OPEC = get_data_opec()
    df_all = pd.concat([df, df2], axis=0, ignore_index=True)
    df_all = pd.concat([df_all, df_OPEC], axis=0, ignore_index=True)
    df_all = df_all.fillna("no data")
    return df_all

def get_constraint():
    file_path = "../data/Constraint_PPATK.xlsx"
    df = pd.read_excel(file_path)
    return df

def nama_cons(df, input_nama):
    df = df["Nama"].contains(input_nama).reset_index(drop=True)
    return df

def nama_similarity(df, input_nama, treshold_value):
    print("before: ", df.shape)
    df = get_similarity(df, input_nama, treshold_value)
    print("after: ", df.shape)
    return df

def NIK_similarity(df, col, NIK_input):
    df = df[df[col].str.contains(NIK_input)].reset_index(drop=True)
    return df

def DOB_similarity(df, col, dob_input):
    df = df[df[col] == dob_input].reset_index(drop=True)
    return df

def POB_similarity(df, col, pob_input):
    try:
        df = df[df[col].str.contains(pob_input)].reset_index(drop=True)
    except:
        df = df[df[col].str.contains(pob_input).fillna(False)].reset_index(drop=True)

    return df

def to_json(df):
    return df.to_json(orient='records')

def treatment_constraint(nama_status, nik_status, dob_status, pob_status):
    df = get_constraint()
    dict_value = {"nama" :nama_status,
                "nik" : nik_status,
                "dob" : dob_status,
                "pob" : pob_status}
    result = df.loc[(df[list(dict_value)] == pd.Series(dict_value)).all(axis=1)]
    result_recommendation =  list(set(result["recommendation"]))[0]

    return result_recommendation


@app.get('/PPATK/')
async def dttot(Nama: Optional[str]=None, NIK: Optional[str]=None, DOB: Optional[str]=None, POB: Optional[str]=None, Alamat: Optional[str]=None):
    df = get_all_data()
    appended_data = []
    Similarity_Percentage = 0.8

    nama_status = "not match"
    nik_status = "not match"
    dob_status = "not match"
    pob_status = "not match"
    alamat_status = "not match"

    dict_filter = {}

    if Nama is not None:
        df_nama = nama_similarity(df, Nama, Similarity_Percentage)
        if df_nama.shape[0] > 0 :
            print("__________________df_nama.shape[0] > 0__________________")
            df = df_nama.copy()
            dict_filter["Nama"] = Nama
            nama_status = "match"
        appended_data.append(df_nama)

    if NIK is not None:
        df_nik = NIK_similarity(df, 'NIK', NIK)
        appended_data.append(df_nik)
        if df_nik.shape[0] > 0 :
            df = df_nik.copy()
            dict_filter["NIK"] = NIK
            if len(NIK) <= 7:
                nik_status = "not match"
            else:
                nik_status = "match"

    if DOB is not None:
        df_DOB = DOB_similarity(df, 'Tanggal Lahir', DOB)
        appended_data.append(df_DOB)
        if df_DOB.shape[0] > 0 :
            df = df_DOB.copy()
            print("__________________df_DOB.shape[0] > 0__________________")
            dict_filter["Tgl Lahir"] = DOB
            dob_status = "match"

    if POB is not None:
        df_POB = POB_similarity(df, 'Tempat Lahir', POB)
        appended_data.append(df_POB)
        if df_POB.shape[0] > 0 :
            df = df_POB.copy()
            print("__________________df_POB.shape[0] > 0__________________")
            dict_filter["Tpt Lahir"] = POB
            pob_status = "match"

    # if Alamat is not None:
    #     df_alamat = alamat_similarity(df, 'Alamat', Alamat)
        # df = df_nama.copy()
        # appended_data.append(df_alamat)

    # df = pd.concat(appended_data).reset_index(drop=True)
    print("____________________________________")
    print("shape: ", df.shape)
    print("dictionary: ", dict_filter)

    try:
        # df = df.loc[(df[list(dict_filter)] == pd.Series(dict_filter)).all(axis=1)]
        df = df[df.isin(dict_filter)]
        print("**************")
    except:
        df = df.iloc[[0]]
        print("**************zzzzzzzzz")
    print("shape: ", df.shape)
    # df = df.loc[:0]
    print("____________________________________")

    outp = to_json(df)
    simalarity_percentage = None

    if nama_status == "match":
        simalarity_percentage = df["similarity"][0]
    reccomendation = treatment_constraint(nama_status, nik_status, dob_status, pob_status)

    respond_out = {
        "Reccomendation" : reccomendation,
        "Nama" : nama_status,
        "Similarity_Score" : simalarity_percentage,
        "NIK" : nik_status,
        "DOB" : dob_status,
        "POB" : pob_status,
        "Alamat" : alamat_status,
        "Note" : outp
    }
    return respond_out


# if __name__ == "__main__":
#     uvicorn.run("api:app", host="0.0.0.0", port=8090, log_level="info", reload=True)

# to run python api.py
# go here http://127.0.0.1:8090/docs
