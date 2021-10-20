import json
import logging
from typing import Optional
import pandas as pd
import uvicorn
from fastapi import BackgroundTasks, FastAPI

from service.dttot import dttot_prepro, get_similarity, wmd_prepro
import warnings
warnings.filterwarnings("ignore")



app = FastAPI()

def get_data_dttot():
    file_path = "../data/20210429140917.xlsx"
    df = dttot_prepro(file_path)
    return df

def get_data_wmd():
    df1 = pd.read_excel('../data/20181023091737.xlsx')
    df2 = pd.read_excel('../data/20181023091801.xlsx')
    df = wmd_prepro(df1, df2)
    return df


def nama_cons(df, input_nama):
    df = df["Nama"].contains(input_nama).reset_index(drop=True)
    return df


def nama_similarity(df, input_nama):
    df = get_similarity(df, input_nama)
    return df


def NIK_similarity(df, col, NIK_input):
    df = df[df[col].str.contains(NIK_input)].reset_index(drop=True)
    return df


def paspor_similarity(df, col, paspor_input):
    try:
        df = df[df[col].str.contains(paspor_input)].reset_index(drop=True)
    except:
        df = df[df[col].str.contains(paspor_input).fillna(False)].reset_index(drop=True)

    return df


def to_json(df):
    return df.to_json(orient='records')


@app.get('/DTTOT_contains/')
async def name_contains(Nama: str):
    df = get_data_dttot()
    df = nama_cons(df, Nama)

    outp = to_json(df)
    return outp


# @app.get('/DTTOT/')
# async def dttot(Nama: Optional[str]=None, NIK: Optional[str]=None, Paspor: Optional[str]=None):
#     df = get_data_dttot()
#
#     if Nama is not None:
#         df = nama_similarity(df, Nama)
#     if NIK is not None:
#         df = NIK_similarity(df, 'NIK', NIK)
#         print(df.shape)
#     if Paspor is not None:
#         df = paspor_similarity(df, 'paspor', Paspor)
#
#     outp = to_json(df)
#     return outp

@app.get('/DTTOT/')
async def dttot(Nama: Optional[str]=None, NIK: Optional[str]=None, Paspor: Optional[str]=None):
    df = get_data_dttot()
    appended_data = []

    if Nama is not None:
        df_nama = nama_similarity(df, Nama)
        appended_data.append(df_nama)
    if NIK is not None:
        df_nik = NIK_similarity(df, 'NIK', NIK)
        appended_data.append(df_nik)
    if Paspor is not None:
        df_paspor = paspor_similarity(df, 'paspor', Paspor)
        appended_data.append(df_paspor)

    df = pd.concat(appended_data).reset_index(drop=True)
    df = df.loc[:0]

    nama_status = None
    nik_status = None
    paspor_status = None

    if Nama is not None:
        if df_nama.shape[0] > 0:
            nama_status = "match"
    if NIK is not None:
        if df["NIK"].str.contains(NIK).any():
            print("masuk")
            nik_status = "match"
        else:
            print("tidak")
    if Paspor is not None:
        if df["paspor"].str.contains(Paspor).any():
            print("masuk")
            paspor_status = "match"
        else:
            print("tidak")

    outp = to_json(df)

    respond_out = {
        "Nama" : nama_status,
        "NIK" : nik_status,
        "Paspor" : paspor_status,
        "Note" : outp
    }
    return respond_out


@app.get('/WMD/')
async def wmd(Nama: Optional[str]=None, NIK: Optional[str]=None, Paspor: Optional[str]=None):
    df = get_data_wmd()
    appended_data = []

    if Nama is not None:
        df_nama = nama_similarity(df, Nama)
        appended_data.append(df_nama)
    if NIK is not None:
        df_nik = NIK_similarity(df, 'Nomor Identitas', NIK)
        appended_data.append(df_nik)
    if Paspor is not None:
        df_paspor = paspor_similarity(df, 'Nomor Paspor', Paspor)
        appended_data.append(df_paspor)

    df = pd.concat(appended_data).reset_index(drop=True)
    df = df.loc[:0]

    nama_status = None
    nik_status = None
    paspor_status = None

    if Nama is not None:
        if df_nama.shape[0] > 0:
            nama_status = "match"
    if NIK is not None:
        if df["Nomor Identitas"].str.contains(NIK).any():
            nik_status = "match"
    if Paspor is not None:
        if df["Nomor Paspor"].str.contains(Paspor).any():
            paspor_status = "match"

    outp = to_json(df)

    respond_out = {
        "Nama" : nama_status,
        "NIK" : nik_status,
        "Paspor" : paspor_status,
        "Note" : outp
    }
    return respond_out


# if __name__ == "__main__":
#     uvicorn.run("api:app", host="0.0.0.0", port=8090, log_level="info", reload=True)

# to run python api.py
# go here http://127.0.0.1:8090/docs
