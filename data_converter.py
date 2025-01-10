# -*- coding: utf-8 -*-
"""
@author: SamJ_IITM
"""

import pandas as pd
import os
import warnings
import numpy as np
from datetime import datetime

warnings.filterwarnings("ignore")

def convert_date_format(date_str):
    formats = ['%d-%m-%Y', '%Y-%m-%d']
    for fmt in formats:
        try:
            date_obj = datetime.strptime(date_str, fmt).date()
            formatted_date = date_obj.strftime('%Y%m%d')
            return formatted_date
        except ValueError:
            continue 

    return date_str

def format_client_details():
    client_details = pd.read_excel('Client Details.xlsx', header = 2)
    client_details = client_details.rename(columns={'Unnamed: 3': 'ACCOUNT', 'Unnamed: 1': 'ARB CODE'})
    client_details['BSE CASH'] = client_details['BSE CASH'].str.replace('-', '', regex=False)
    client_details['BSE FNO'] = client_details['BSE FNO'].str.replace('-', '', regex=False)
    client_details['NSE CASH'] = client_details['NSE CASH'].str.replace('-', '', regex=False)
    client_details['NSE FNO'] = client_details['NSE FNO'].str.replace('-', '', regex=False)
    
    columns_to_strip = ['ACCOUNT', 'BSE CASH', 'BSE FNO', 'NSE CASH', 'NSE FNO']
    client_details[columns_to_strip] = client_details[columns_to_strip].apply(lambda x: x.str.strip())
    
    clients_set = set(client_details['ACCOUNT'])
    clients_set.remove(np.nan)
    
    return client_details, clients_set

def convert_code(value):
    try:
        return str(int(float(value)))
    except ValueError:
        return value
    
def process_BSEEQ_file(path, client_details, clients_set):
    df = pd.read_csv(path, header = None, dtype={'Pric':float})#,names=cols,d_types={})
    df.columns = df.iloc[0]
    df = df[1:].reset_index(drop=True)
    df = df[df['ClntId'].isin(clients_set)].reset_index(drop=True)
    
    df['INSTRUMENT TYPE'] = 'EQ'
    print('BSE EQUITY Trade File Date:', df['TradDt'][1])
    df['UPDATED TRADE DATE'] = [convert_date_format(df['TradDt'][i]) for i in range(len(df))]
    df['Expiry'] = ''
    df['Strike Rate'] = ''
    df['CALL PUT'] = ''
    df['TERMINAL NUMBER'] = ''
    df['BSE SUB GROUP'] = ''
    df['ACTIVE PASSIVE'] = ''
    df['CUSTORDIAN CODE'] = ''
    
    new_df = pd.DataFrame({
        'EXCHANGE' : df['Xchg'],
        'TRADE DATE' : df['UPDATED TRADE DATE'],
        'GROUP' : df['SctySrs'],
        'CODE' : '',
        'SCRIP CODE/SYMBOL' : df['FinInstrmId'],
        "SCRIP NAME" : df['FinInstrmNm'],
        'INSTRUMENT TYPE' : df['INSTRUMENT TYPE'],
        'EXPIRY DATE': df['Expiry'],
        'STRIKE RATE' : df['Strike Rate'],
        'CALL PUT' : df['CALL PUT'],
        'B/S' : df['BuySellInd'],
        "QTY" : df['TradQty'],
        'MARKET RATE' : df['Pric'],
        'TRADE NUMBER' : df['UnqTradIdr'],
        'ORDER NUMBER' : df['OrdrRef'],
        'TRADE TIME' : df['TradDtTm'],
        'ORDER TIME' : df['OrdrDtTm'],
        'LOCATION ID' : df['CtclId'],
        'Terminal Number' : df['TERMINAL NUMBER'],
        'BSE SUB GROUP' : df['BSE SUB GROUP'],
        'ACTIVE PASSIVE' : df['ACTIVE PASSIVE'],
        'CUSTORDIAN CODE' : df['CUSTORDIAN CODE'],
        'BROKER CODE' : df['ClntId']
        })
    
    new_df = new_df.astype(str)

    new_df['TRADE TIME'] = new_df['TRADE TIME'].str[11:]
    new_df['ORDER TIME'] = new_df['ORDER TIME'].str[11:]
    
    for i in range(len(new_df)):
        for j in range(len(client_details)):
            if str(new_df['BROKER CODE'][i]).strip() == str(client_details['ACCOUNT'][j]).strip():
                if str(new_df['LOCATION ID'][i]).strip() == str(client_details['BSE CASH'][j]):
                    new_df['CODE'][i] = str(client_details['ARB CODE'][j]).strip()
                    break
                else:
                    new_df['CODE'][i] = str(str(client_details['ACCOUNT'][j])+'_NR').strip()

    new_df['CODE'] = new_df['CODE'].apply(convert_code)
    new_df = new_df.applymap(lambda x: x.strip() if isinstance(x, str) else x)
    new_df['MARKET RATE'] = new_df['MARKET RATE'].astype(float)
    new_df['MARKET RATE'] = new_df['MARKET RATE'].apply(lambda x: f"{x:.2f}")
    new_df.to_excel('BSE EQUITY.xlsx', index = False)
    print('BSE EQUITY completed')
    

def process_BSEFO_file(path, client_details, clients_set): 
    df = pd.read_csv(path, header = None) #,dtype={20:str, 17:str, 33:str, 34:str})
    df.columns = df.iloc[0]
    df = df[1:].reset_index(drop=True)
    df = df[df['ClntId'].isin(clients_set)].reset_index(drop=True)
    df['EXCHANGE'] = 'FOB'
    
    print('BSE FO Trade File Date:', df['TradDt'][1])
    df['TRADE DATE'] = [convert_date_format(df['TradDt'][i]) for i in range(len(df))]
    df['EXPIRY DATE'] = [convert_date_format(df['XpryDt'][i]) for i in range(len(df))]
    df['TERMINAL NUMBER'] = ''
    df['BSE SUB GROUP'] = ''
    df['ACTIVE PASSIVE'] = ''
    df['CUSTORDIAN CODE'] = ''
    df['GROUP'] = ''

    
    new_df = pd.DataFrame({
        'EXCHANGE' : df['EXCHANGE'],
        'TRADE DATE' : df['TRADE DATE'],
        'GROUP' : df['GROUP'],
        'CODE' : '',
        'SCRIP CODE/SYMBOL' : df['TckrSymb'],
        "SCRIP NAME" : df['TckrSymb'],
        'INSTRUMENT TYPE' : df['FinInstrmTp'],
        'EXPIRY DATE' : df['EXPIRY DATE'],
        'STRIKE RATE' : df['StrkPric'],
        'CALL PUT' : df['OptnTp'],
        'B/S' : df['BuySellInd'],
        "QTY" : df['TradQty'],
        'MARKET RATE' : df['Pric'],
        'TRADE NUMBER' : df['UnqTradIdr'],
        'ORDER NUMBER' : df['OrdrRef'],
        'TRADE TIME' : df['TradDtTm'],
        'ORDER TIME' : df['OrdrDtTm'],
        'LOCATION ID' : df['CtclId'],
        'TERMINAL NUMBER' : df['TERMINAL NUMBER'],
        'BSE SUB GROUP' : df['BSE SUB GROUP'],
        'ACTIVE PASSIVE' : df['ACTIVE PASSIVE'],
        'CUSTORDIAN CODE' : df['CUSTORDIAN CODE'],
        'BROKER CODE' : df['ClntId']
        })
    
    new_df = new_df.astype(str)
    
    new_df['TRADE TIME'] = new_df['TRADE TIME'].str[11:]
    new_df['ORDER TIME'] = new_df['ORDER TIME'].str[11:]
    
    new_df['INSTRUMENT TYPE'] = new_df['INSTRUMENT TYPE'].replace({
    'IDF': 'FUTIDX',
    'IDO': 'OPTIDX',
    'STO': 'OPTSTK',
    'STF': 'FUTSTK'
    })
    
    new_df['SCRIP CODE/SYMBOL'] = new_df['SCRIP CODE/SYMBOL'].replace({
    'SENSEX': 'BSX',
    'BANKEX': 'BKX'
    })
    
    for i in range(len(new_df)):
        for j in range(len(client_details)):
            if str(new_df['BROKER CODE'][i]).strip() == str(client_details['ACCOUNT'][j]).strip():
                if str(new_df['LOCATION ID'][i]).strip() == str(client_details['BSE FNO'][j]):
                    new_df['CODE'][i] = str(int(client_details['ARB CODE'][j])).strip()
                    break
                else:
                    new_df['CODE'][i] = str(str(client_details['ACCOUNT'][j])+'_NR').strip()
    
    new_df['STRIKE RATE'] = new_df['STRIKE RATE'].apply(convert_code)
    new_df = new_df.applymap(lambda x: x.strip() if isinstance(x, str) else x)
    new_df = new_df.apply(lambda x: pd.Series(x.dropna().values))
    new_df = new_df.fillna(' ')
    new_df.to_excel('BSE FO.xlsx', index = False)
    print('BSE FO completed')
  
    
def process_NSEEQ_file(path, client_details, clients_set):
    df = pd.read_csv(path, header = None)
    df.columns = df.iloc[0]
    df = df[1:].reset_index(drop=True)
    df = df[df['ClntId'].isin(clients_set)].reset_index(drop=True)
    
    print('NSE EQUITY Trade File Date:', df['TradDt'][1])
    
    df['TRADE DATE'] = [convert_date_format(df['TradDt'][i]) for i in range(len(df))]
    df['TERMINAL NUMBER'] = ''
    df['BSE SUB GROUP'] = ''
    df['ACTIVE PASSIVE'] = ''
    df['CUSTORDIAN CODE'] = ''
    df['GROUP'] = ''
    df['Expiry'] = ''
    df['Strike Rate'] = ''
    df['CALL PUT'] = ''
            
    new_df = pd.DataFrame({
        'EXCHANGE' : df['Xchg'],
        'TRADE DATE' : df['TRADE DATE'],
        'GROUP' : df['GROUP'],
        'CODE' : '',
        'SCRIP CODE/SYMBOL' : df['TckrSymb'],
        "SCRIP NAME" : df['FinInstrmNm'],
        'INSTRUMENT TYPE' : df['SctySrs'],
        'EXPIRY DATE' : df['Expiry'],
        'STRIKE RATE' : df['Strike Rate'],
        'CALL PUT' : df['CALL PUT'],
        'B/S' : df['BuySellInd'],
        "QTY" : df['TradQty'],
        'MARKET RATE' : df['Pric'],
        'TRADE NUMBER' : df['UnqTradIdr'],
        'ORDER NUMBER' : df['OrdrRef'],
        'TRADE TIME' : df['TradDtTm'],
        'ORDER TIME' : df['OrdrDtTm'],
        'LOCATION ID' : df['CtclId'],
        'Terminal Number' : df['TERMINAL NUMBER'],
        'BSE SUB GROUP' : df['BSE SUB GROUP'],
        'ACTIVE PASSIVE' : df['ACTIVE PASSIVE'],
        'CUSTORDIAN CODE' : df['CUSTORDIAN CODE'],
        'BROKER CODE' : df['ClntId']
        })
    
    new_df = new_df.astype(str)
    
    new_df['TRADE TIME'] = new_df['TRADE TIME'].str[11:]
    new_df['ORDER TIME'] = new_df['ORDER TIME'].str[11:]
    
    for i in range(len(new_df)):
        for j in range(len(client_details)):
            if str(new_df['BROKER CODE'][i]).strip() == str(client_details['ACCOUNT'][j]).strip():
                if str(new_df['LOCATION ID'][i]).strip() == str(client_details['NSE CASH'][j]):
                    new_df['CODE'][i] = str(int(client_details['ARB CODE'][j])).strip()
                    break
                else:
                    new_df['CODE'][i] = str(str(client_details['ACCOUNT'][j])+'_NR').strip()
    

    new_df = new_df.applymap(lambda x: x.strip() if isinstance(x, str) else x)
    new_df.to_excel('NSE EQUITY.xlsx', index = False)
    print('NSE EQUITY completed')


def process_NSEFO_file(path, client_details, clients_set):
    df = pd.read_csv(path, header = None)
    df.columns = df.iloc[0]
    df = df[1:].reset_index(drop=True)
    df = df[df['ClntId'].isin(clients_set)].reset_index(drop=True)
    df['EXCHANGE'] = 'FON'
    df['TRADE DATE'] = [convert_date_format(df['TradDt'][i]) for i in range(len(df))]
    df['Expiry Date'] = [convert_date_format(df['XpryDt'][i].strip()) for i in range(len(df))]
    print('NSE FO Trade File Date:', df['TradDt'][1])
    df['TERMINAL NUMBER'] = ''
    df['BSE SUB GROUP'] = ''
    df['ACTIVE PASSIVE'] = ''
    df['CUSTORDIAN CODE'] = ''
    df['GROUP'] = ''
    
    new_df = pd.DataFrame({
        'EXCHANGE' : df['EXCHANGE'],
        'TRADE DATE' : df['TRADE DATE'],
        'GROUP' : df['GROUP'],
        'CODE' : '',
        'SCRIP CODE/SYMBOL' : df['TckrSymb'],
        "SCRIP NAME" : df['TckrSymb'],
        'INSTRUMENT TYPE' : df['FinInstrmTp'],
        'EXPIRY DATE' : df['Expiry Date'],
        'STRIKE RATE' : df['StrkPric'],
        'CALL PUT' : df['OptnTp'],
        'B/S' : df['BuySellInd'],
        "QTY" : df['TradQty'],
        'MARKET RATE' : df['Pric'],
        'TRADE NUMBER' : df['UnqTradIdr'],
        'ORDER NUMBER' : df['OrdrRef'],
        'TRADE TIME' : df['TradDtTm'],
        'ORDER TIME' : df['OrdrDtTm'],
        'LOCATION ID' : df['CtclId'],
        'TERMINAL NUMBER' : df['TERMINAL NUMBER'],
        'BSE SUB GROUP' : df['BSE SUB GROUP'],
        'ACTIVE PASSIVE' : df['ACTIVE PASSIVE'],
        'CUSTORDIAN CODE' : df['CUSTORDIAN CODE'],
        'BROKER CODE' : df['ClntId']
        })
    
    new_df = new_df.astype(str)
    
    new_df['TRADE TIME'] = new_df['TRADE TIME'].str[11:]
    new_df['ORDER TIME'] = new_df['ORDER TIME'].str[11:]
    
    new_df = new_df.applymap(lambda x: x.strip() if isinstance(x, str) else x)
    
    new_df['INSTRUMENT TYPE'] = new_df['INSTRUMENT TYPE'].replace({
    'IDF': 'FUTIDX',
    'IDO': 'OPTIDX',
    'STO': 'OPTSTK',
    'STF': 'FUTSTK'
    })
    
    for i in range(len(new_df)):
        for j in range(len(client_details)):
            if str(new_df['BROKER CODE'][i]).strip() == str(client_details['ACCOUNT'][j]).strip():
                if str(new_df['LOCATION ID'][i]).strip() == str(client_details['NSE FNO'][j]).strip():
                    new_df['CODE'][i] = str(int(client_details['ARB CODE'][j])).strip()
                    break
                else:
                    new_df['CODE'][i] = str(str(client_details['ACCOUNT'][j])+'_NR').strip()
    
    new_df['STRIKE RATE'] = new_df['STRIKE RATE'].apply(convert_code)
    new_df = new_df.applymap(lambda x: x.strip() if isinstance(x, str) else x)
    new_df.to_excel('NSE FO.xlsx', index = False)
    
    print('NSE FO completed')

    
if __name__ == "__main__":
    segments = ['BSEEQ', 'BSEFO', 'NSEEQ','NSEFO']
    client_details, clients_set = format_client_details()
    
    for segment in segments:
        directory = f'C:\\Users\\Administrator\\{segment}'
        for filename in os.listdir(directory):
            file_path = os.path.join(directory, filename)
            if segment == 'BSEEQ':
                process_BSEEQ_file(file_path, client_details, clients_set) 
            elif segment == 'BSEFO':
                process_BSEFO_file(file_path, client_details, clients_set)
            elif segment == 'NSEEQ':
                process_NSEEQ_file(file_path, client_details, clients_set)
            else:
                process_NSEFO_file(file_path, client_details, clients_set)
