import pandas as pd
import re
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


def enclosure_ac_map(value):
    if 'AC' in value:
        return 'AC'
    elif value == 'OROPS':
        return 'OROPS'
    else:
        return 'Other'


def year_buckets_cat_map(val):
    if val == 1000:
        return UNKNOWN_VALUE
    elif val < 1940:
        return "Pre-dustbowl"
    elif val < 1960:
        return "Dustbowl"
    elif val < 1980:
        return "Old"
    elif val < 2000:
        return "Modern"
    else:
        return "New"


def create_units_col(desc_value):
    result_name = re.search('[^0-9+]+$', desc_value)  #grab units from the tail..
    if result_name:
        units = result_name.group(0)
        return units
    else:
        return UNKNOWN_VALUE


def create_units_value_col(desc_value):
    result = re.search('([0-9\.]+) to ([0-9\.]+)', desc_value)
    if (result):
        value = (float(result.group(1)) + float(result.group(2))) / 2
        #row[units] = value
        return value
    else:
        #for cases "16.0 + Ft Standard Digging Depth"
        result2 = re.search('([0-9\.]+)\s?\+', desc_value)
        if result2:
            value = result2.group(1)
            return float(value)
        else:
            return 0


def create_and_norm_categorical(df):
    with_numeric_cat = create_str_cols(df)
    clean_data = merge_all_none_types(with_numeric_cat)
    clean_data['Tire_Size'] = clean_data['Tire_Size'].apply(combine_10_inch_tire_size)
    clean_data['Modernity'] = clean_data['YearMade'].apply(year_buckets_cat_map)
    clean_data['Enclosure_Reduced'] = clean_data['Enclosure'].apply(enclosure_ac_map)
    clean_data['_units'] = clean_data['fiProductClassDesc'].apply(create_units_col)
    clean_data['_measurement'] = clean_data['fiProductClassDesc'].apply(create_units_value_col)

    return clean_data

zf = ZipFile('data/Train.zip')
df = pd.read_csv('data/Train.csv')

year = df['YearMade']
year = year[year != 1000]
price_v_year = df[['SalePrice', 'YearMade']]


cleaned = create_and_norm_categorical(df)
