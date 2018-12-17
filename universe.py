import numpy as np, pandas as pd, pymongo, math
from collections import defaultdict
from functools import partial
import datetime
from dateutil import relativedelta
import math


def format_month_string(year, month):
    return str(year) + (str(month) if month > 9 
            else "0" + str(month))

def get_client():
    return pymongo.MongoClient('localhost', 27017, maxPoolSize=100)


def get_stock_list(db):
    return db.list_collection_names()


def get_data_multiple(db, stocks, query_fields, func, special):
    data_all = defaultdict(list)
    
    rfields = {}
    for qf in query_fields:
        rfields[qf] = 1
    rfields["Date"] = 1
    rfields["_id"] = 0

    last = 0

    for i, stock in enumerate(stocks):
        d = list(db[stock].find({}, rfields))
        for f in query_fields:
            d = list(filter(lambda x: f in x, d))
        
        
        d = list(map(lambda x: (format_month_string(x["Date"].year, \
                x["Date"].month), func(x, special)), d))        
        for x in d:
            if x[0] in data_all:
                data_all[x[0]].append(x[1])
            else:
                data_all[x[0]]=[x[1]]
        
        if i == math.ceil(len(stocks)/5 * last):
            print(100*i/len(stocks),"% complete.")
            last += 1
    
    return data_all


def get_data(db, stocks, query_field):

    data_all = defaultdict(list)
    
    rfields = {}
    rfields[query_field] = 1
    rfields["Date"] = 1
    rfields["_id"] = 0

    last = 0

    for i, stock in enumerate(stocks):
        d = list(db[stock].find({}, rfields))
        d = list(filter(lambda x: query_field in x, d))
        d = list(map(lambda x: (format_month_string(x["Date"].year, \
                x["Date"].month), x[query_field]), d))        
        for x in d:
            if x[0] in data_all:
                data_all[x[0]].append(x[1])
            else:
                data_all[x[0]]=[x[1]]
        
        if i == math.ceil(len(stocks)/5 * last):
            print(100*i/len(stocks),"% complete.")
            last += 1
    
    return data_all


def enterprise_value_ratio(feature_dict, field):
    try:
        return feature_dict[field] / feature_dict["ENTERPRISE_VALUE"]
    except ZeroDivisionError:
        return 0


# combine the data by bucket determined by merge_dict, keys are original
# mappings, and values are new mappings
def merge(merge_dict, data):
    data_merged = defaultdict(list)

    for key in data:
        try:
            data_merged[merge_dict[key]].extend(data[key])
        except KeyError:
            try:
                data_merged[merge_dict[key]] = data[key]
            except KeyError:
                pass

    return data_merged


# returns a dict with which quarter date bucket to map to for determining 
# membership in the universe, end_year is non-inclusive
def quarterly(start_year, end_year):
    date_ranges = {}

    month = 1
    year = start_year
    while year < end_year:
        date_ranges[format_month_string(year, month)] = \
                format_month_string(year, 1 + 3 * math.floor((month - 1)/3))
        if month == 12:
            month = 1
            year += 1
        month += 1
    return date_ranges

# compute the market cap threshold, aka middle 2 quantiles
def market_cap_quantiles(db, stocks):
    
    quantile_by_date = {}
    data = get_data(db, stocks, "HISTORICAL_MARKET_CAP")
    for key in data:
        quantile_by_date[key] = np.quantile(data[key], [0.25, 0.75])
    print(quantile_by_date)
    return quantile_by_date


# compute the ebitda to enterprise value ratio threshold, top quantile
def ebitda_value(db, stocks):
    data = get_data_multiple(db, stocks, ["ENTERPRISE_VALUE", 
        "EBITDA"], enterprise_value_ratio, "EBITDA")
    
    date_ranges = quarterly(1990, 2018)
    data_merged = merge(date_ranges, data)

    for key in data_merged:
        print(key,":",len(data_merged[key]))

    top_quart_by_date = {}

    for key in data_merged:
        top_quart_by_date[key] = np.quantile(data_merged[key], [0.75])

    print(top_quart_by_date)
    return top_quart_by_date


# compute the leverage to enterprise ratio quantile, above the median
def leverage(db, stocks):
    data = get_data_multiple(db, stocks, ["ENTERPRISE_VALUE", \
            "BS_LT_BORROW"], enterprise_value_ratio, "BS_LT_BORROW")

    
    date_ranges = quarterly(1990, 2018)
    data_merged = merge(date_ranges, data)
    
    for key in data_merged:
        print(key,":",len(data_merged[key]))

    # compute the median
    median_by_date = {}

    for key in data_merged:
        median_by_date[key] = np.quantile(data_merged[key], [0.5])
    
    print(median_by_date)
    return median_by_date


def date_string_key(date_str):
    key = date_str[0:4]
    month = int(date_str[4:])
    if month < 10:
        key += "0" + str(month)
    else:
        key += str(month)

    return


def above(low, val):
    return val >= low


def above_wrap(low):
    return partial(above, low)


def between(low, high, val):
    return (val >= low and val <= high)


def between_wrap(low, high):
    return partial(between, low, high)


def get_matching(db, stocks, query_fields):
    pass

def get_universe(db):

    stocks = get_stock_list(db)
    mdict = market_cap_quantiles(db, stocks)
    edict = ebitda_value(db, stocks)
    ldict = leverage(db, stocks)

    # get all valid dates from all three features
    lset = set(ldict.keys())
    mset = set(mdict.keys())
    eset = set(edict.keys())

    dates = lset.intersection(mset).intersection(eset)
    print(sorted(dates))

    for date in dates:
        month = int(date[-2:])
        year = int(date[0:4])
        
        # date ranges for features
        start = datetime.datetime(year, month, 1)
        end_feature = start + relativedelta.relativedelta(months=1)

        # date ranges for returns
        end_target = start + relativedelta.relativedelta(years = 1)
        print(start, end_target)


if __name__ == "__main__":
    client = get_client()
    db = client["Stocks"]

    stocks = get_stock_list(db)
    ldict = leverage(db, stocks)

    
    # get_universe(db)

