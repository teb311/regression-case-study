import pandas as pd
from zipfile import ZipFile

UNKNOWN_VALUE = 'Unknown'
INPUT_NONE_VALUES = ['None or Unspecified', 'Unspecified', '1000', 'nan']
KNOWN_BAD_VALUES = ['#NAME?']
NUMERICAL_CATEGORICAL_COLUMNS = [
    'ModelID', 'datasource', 'YearMade', 'auctioneerID',
]


def create_str_cols(df):
    copy = df.copy()
    for col_name in NUMERICAL_CATEGORICAL_COLUMNS:
        copy[col_name + '_categorical'] = copy[col_name].astype(str)

    return copy


def merge_all_none_types(df):
    copy = df.copy()
    for col_name in df:
        series = copy[col_name]
        if series.dtype in ['string', 'object']:
            new_series = series.apply(none_mapper)
            copy[col_name] = new_series

    return copy


def none_mapper(value):
    if pd.isnull(value):
        return UNKNOWN_VALUE
    elif value == '':
        return UNKNOWN_VALUE
    elif value in INPUT_NONE_VALUES:
        return UNKNOWN_VALUE
    elif value in KNOWN_BAD_VALUES:
        return UNKNOWN_VALUE

    return value


def combine_10_inch_tire_size(value):
    if value == '10 inch':
        return '10"'

    return value


def create_and_norm_categorical(df):
    with_numeric_cat = create_str_cols(df)
    clean_data = merge_all_none_types(with_numeric_cat)
    clean_data['Tire_Size'] = clean_data['Tire_Size'].apply(combine_10_inch_tire_size)
    return clean_data


zf = ZipFile('data/Train.zip')
df = pd.read_csv('data/Train.csv')

year = df['YearMade']
year = year[year != 1000]
price_v_year = df[['SalePrice', 'YearMade']]


cleaned = create_and_norm_categorical(df)
