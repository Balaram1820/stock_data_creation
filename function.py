import pandas as pd
           
def preprocess_data(data):
    data = data[data.Open != data.Close]
    data = data.reset_index()
    return data

def calculate_heiken_ashi(data):
    data.drop(['Volume'], axis=1, inplace=True)
    data = data[data.Open != data.Close]
    data = data.reset_index()
    data['Heiken_Close'] = (data.Open+data.Close+data.High+data.Low)/4
    data['Heiken_Open'] = data['Open']
    for i in range(1, len(data)):
        data['Heiken_Open'][i] = (
            data.Heiken_Open[i-1]+data.Heiken_Close[i-1])/2

    data['Heiken_High'] = data[[
        'High', 'Heiken_Open', 'Heiken_Close']].max(axis=1)
    data['Heiken_Low'] = data[[
        'Low', 'Heiken_Open', 'Heiken_Close']].min(axis=1)
    data.dropna(inplace=True)
    return data

def calculate_modified_heiken_ashi(data):
    # Calculate MhClose
    data['MhClose'] = round(
        (data['Open'] + data['High'] + data['Low'] + 2 * data['Close']) / 5)
    # Calculate MhOpen
    data['MhOpen'] = data['Open']
    for i in range(1, len(data)):
        data.at[i, 'MhOpen'] = round(
            (data.at[i-1, 'MhOpen'] + data.at[i-1, 'MhClose']) / 2)
    # Calculate MhHigh
    data['MhHigh'] = ''
    for i in range(4, len(data)):
        data.at[i, 'MhHigh'] = round(data.iloc[i-4:i, :]['MhOpen'].mean())
    # Calculate MhLow
    data['MhLow'] = ''
    for i in range(4, len(data)):
        data.at[i, 'MhLow'] = round(data.iloc[i-4:i, :]['MhClose'].mean())
    # Round all values to the nearest integer
    data = data.round()
    return data

def calculate_EBR(data):
    ebr = (4 * data['MhOpen'].shift(5) - data['Low'].shift(5)) / 3
    ebr = ebr.round(0).fillna(0).astype(int)
    data['EBR'] = ebr
    return data

def calculate_EBL(data):
    ebl = (4 * data['MhOpen'].shift(5) - data['High'].shift(5)) / 3
    ebl = ebl.round(0).fillna(0).astype(int)
    data['EBL'] = ebl
    return data

def calculate_BTRG(data):
    data['BTRG'] = 1.00168 * data['EBR'].shift(-5)
    data['BTRG'][0:5] = 0
    data['BTRG'] = data['BTRG'].round(0).fillna(0).astype(int)
    return data

def calculate_STRG(data):
    data['STRG'] = 0.99382 * data['EBL'].shift(-5)
    data['STRG'][0:5] = 0
    data['STRG'] = data['STRG'].round(0).fillna(0).astype(int)
    return data

def calculate_historical_prices(data: pd.DataFrame) -> pd.DataFrame:
    # Calculate hpopen
    data['hpOpen'] = (data['Close']).shift(5)
    data['hpOpen'] = data['hpOpen'].fillna(0).astype(int)

    # Calculate hphigh
    data['hpHigh'] = 1.00618*(data['Close']).shift(5)
    data['hpHigh'] = data['hpHigh'].fillna(0).astype(int)

    # Calculate hplow
    data['hpLow'] = 0.99382*(data['Close']).shift(5)
    data['hpLow'] = data['hpLow'].fillna(0).astype(int)

    # Calculate hpclose
    data['hpClose'] = 1.001618*(data['Close']).shift(5)
    data['hpClose'] = data['hpClose'].fillna(0).astype(int)
    return data


def calculate_buy(data: pd.DataFrame) -> pd.DataFrame:
    data['Buy'] = 0
    for i in range(5, len(data)):
        if data.at[i, 'High'] > (1.00618 * data.at[i, 'EBR']):
            data.at[i, 'Buy'] = 1.00618 * data.at[i, 'EBR']
        elif data.at[i, 'Low'] < (0.99382 * data.at[i, 'EBR']):
            data.at[i, 'Buy'] = 0.99382 * data.at[i, 'EBR']
        else:
            data.at[i, 'Buy'] = -1.00618 * data.at[i, 'EBR']

    data['Buy'] = data['Buy'].fillna(0).astype(int)
    return data


def calculate_stop_loss(data: pd.DataFrame) -> pd.DataFrame:
    data['StopLoss'] = 0
    for i in range(5, len(data)):
        if data.at[i, 'Low'] > (0.998382 * data.at[i, 'EBL']):
            data.at[i, 'StopLoss'] = 0.998382 * data.at[i, 'EBL']
        elif data.at[i, 'High'] < (1.001618 * data.at[i, 'EBL']):
            data.at[i, 'StopLoss'] = 1.001618 * data.at[i, 'EBL']
        else:
            data.at[i, 'StopLoss'] = -0.998382 * data.at[i, 'EBL']

    data['StopLoss'] = data['StopLoss'].fillna(0).astype(int)
    return data


def calculate_sell(data: pd.DataFrame) -> pd.DataFrame:
    data['Sell'] = 0
    for i in range(5, len(data)):
        if data.at[i, 'Low'] > (0.99382 * data.at[i, 'EBL']):
            data.at[i, 'Sell'] = 0.99382 * data.at[i, 'EBL']
        elif data.at[i, 'Low'] > (0.99382 * data.at[i, 'EBL']):
            data.at[i, 'Sell'] = 0.99382 * data.at[i, 'EBL']
        else:
            data.at[i, 'Sell'] = -0.99382 * data.at[i, 'EBL']

    data['Sell'] = data['Sell'].fillna(0).astype(int)
    return data


def sell_stop_loss(data):
    data['StopLoss1'] = 0
    for i in range(5, len(data)):
        if data.at[i, 'High'] > (1.001618 * data.at[i, 'EBR']):
            data.at[i, 'StopLoss1'] = 1.001618 * data.at[i, 'EBR']
        elif data.at[i, 'Low'] < (0.998382 * data.at[i, 'EBR']):
            data.at[i, 'StopLoss1'] = -1.001618 * data.at[i, 'EBR']
        else:
            data.at[i, 'StopLoss1'] = 0
    data['StopLoss1'] = data['StopLoss1'].fillna(0).astype(int)
    return data



def calculate_targets(data):
    data['lgTarget'] = data.apply(
        lambda x: 1.0618*min(data['High'][x.name-5:x.name]) if x.name >= 5 else 0, axis=1)
    data['lgTarget'] = data['lgTarget'].fillna(0).astype(int)

    data['sgTarget'] = data.apply(
        lambda x: 0.9382*max(data['High'][x.name-5:x.name]) if x.name >= 5 else 0, axis=1)
    data['sgTarget'] = data['sgTarget'].fillna(0).astype(int)
    
    return data







