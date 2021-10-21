from math import floor, ceil
from datetime import datetime

import numpy as np
import pandas as pd
import os
import re


def extract_NIK(s):
    try:
        result = re.search('NIK(.*) ', s)
        return result.group(1)
    except:
        return np.nan

def extract_paspor(s):
    try:
        result = re.search('paspor(.*) ', s)
        return result.group(1)
    except:
        return np.nan

def jaro_distance(s1, s2):
    # lower case all the character
    s1 = s1.lower()
    s2 = s2.lower()

    # If the s are equal
    if (s1 == s2):
        return 1.0

    # Length of two s
    len1 = len(s1)
    len2 = len(s2)

    # Maximum distance upto which matching
    # is allowed
    max_dist = floor(max(len1, len2) / 2) - 1

    # Count of matches
    match = 0

    # Hash for matches
    hash_s1 = [0] * len(s1)
    hash_s2 = [0] * len(s2)

    # Traverse through the first
    for i in range(len1):

        # Check if there is any matches
        for j in range(max(0, i - max_dist),
                       min(len2, i + max_dist + 1)):

            # If there is a match
            if (s1[i] == s2[j] and hash_s2[j] == 0):
                hash_s1[i] = 1
                hash_s2[j] = 1
                match += 1
                break

    # If there is no match
    if (match == 0):
        return 0.0

    # Number of transpositions
    t = 0
    point = 0

    # Count number of occurrences
    # where two characters match but
    # there is a third matched character
    # in between the indices
    for i in range(len1):
        if (hash_s1[i]):

            # Find the next matched character
            # in second
            while (hash_s2[point] == 0):
                point += 1

            if (s1[i] != s2[point]):
                point += 1
                t += 1
    t = t//2

    # Return the Jaro Similarity
    return (match/ len1 + match / len2 +
            (match - t + 1) / match)/ 3.0

def jaro_distance_max(row, input_nama):
    percentage = round(np.max([jaro_distance(input_nama, s2) for s2 in row]), 2)
    return percentage

def dttot_prepro(path):
    # read file
    print("read_excel file..")
    df_dttot = pd.read_excel(path)

    # cleaning data
    print("cleaning data..")
    df_dttot["Deskripsi"] = df_dttot["Deskripsi"].fillna("no data")

    pass_name = ["Paspor", "Paspor", "PASPOR", "passport", "Passport"]
    nik_name = ["NIK", "nik", "nomor identifikasi nasional", "Nomor Identifikasi Nasional", "Nomor identifikasi nasional"]

    paspor_str = '|'.join(pass_name)
    nik_str = '|'.join(nik_name)

    df_dttot['Deskripsi'] = df_dttot['Deskripsi'].str.replace(paspor_str, 'paspor')
    df_dttot['Deskripsi'] = df_dttot['Deskripsi'].str.replace(nik_str, 'NIK')
    df_dttot['Deskripsi'] = df_dttot['Deskripsi'].str.replace(":", "")
    df_dttot['Deskripsi'] = df_dttot['Deskripsi'].str.replace(";", " ")
    df_dttot['Deskripsi'] = df_dttot['Deskripsi'].str.replace(".", "")
    df_dttot['Deskripsi'] = df_dttot['Deskripsi'].str.replace(",", " ")

    df_dttot['NIK'] = df_dttot['Deskripsi'].apply(extract_NIK)
    df_dttot['paspor'] = df_dttot['Deskripsi'].apply(extract_paspor)

    df_dttot["nama_list"] = df_dttot['Nama'].str.split("ALIAS")
    # fillnan
    df_dttot = df_dttot.fillna("no data")

    return df_dttot

def get_similarity(df, input_name, treshold_value):

    now = datetime.now()
    current_time = now.strftime("%yy_%m_%d_%H_%M_%S")

    print("calculate distance")
    df['similarity'] = df.apply(lambda x: jaro_distance_max(x['nama_list'], input_name), axis=1)
    # save to local as excel file
    # df.to_excel("./temp/df_dttot_similarity_{}.xlsx".format(current_time), index=False)
    # filter
    df = df[df['similarity'] >= treshold_value].reset_index(drop=True)

    return df


def wmd_prepro(df1, df2):
    df1 = df1.fillna("no data")
    cols = ["Nama", "Alias 1", "Alias 2", "Alias 3", "Alias 4", "Alias 5", "Alias 6", "Alias 7", "Alias 8", "Alias 9", "Alias 10", "Alias 11"]
    df1['Alias'] = df1[cols].values.tolist()
    df1.drop(cols[1:], axis=1, inplace=True)
    df1 = df1.iloc[1:].reset_index(drop=True)

    df2 = df2.fillna("no data")
    cols = ["Nama", "Alias 1", "Alias 2", "Alias 3", "Alias 4", "Alias 5"]
    df2['Alias'] = df2[cols].values.tolist()
    df2.drop(cols[1:], axis=1, inplace=True)
    df2 = df2.iloc[1:].reset_index(drop=True)

    df_mwd = pd.concat([df1, df2], ignore_index=True)
    # df_mwd["nama_list"] = df_mwd['Alias'].str.split(" ")
    df_mwd["nama_list"] = df_mwd['Alias']

    df_mwd = df_mwd.fillna("no data")
    return df_mwd

def UN_prepro(df):
    df = df.fillna("No Data")
    cols = ["FIRST_NAME", "SECOND_NAME", "THIRD_NAME"]
    df['nama_list']= df[cols].values.tolist()
    return df

def UK_prepro(df):
    df["Name 6"] = df["Name 6"].str.replace('"', "")
    df = df.fillna("No Data")
    cols = ["Name 6", "Name 1", "Name 2", "Name 3", "Name 4", "Name 5"]
    df['nama_list']= df[cols].values.tolist()
    return df
