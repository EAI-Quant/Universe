import numpy as np, pandas as pd, pymongo, math
from collections import defaultdict

def get_client():
    return pymongo.MongoClient('localhost', 27017, maxPoolSize=100)


def get_stock_list(db):
    return db.list_collection_names()


def get_data_multiple(db, stocks, query_fields, func, special):
    data_all = defaultdict(list)
    
    rfields = {}
    qfields = {}
    for qf in query_fields:
        rfields[qf] = 1
        qfields[qf] = {"$exists":True}
    rfields["Date"] = 1
    rfields["_id"] = 0

    last = 0

    for i, stock in enumerate(stocks):
        d = list(db[stock].find({}, rfields))
        for f in query_fields:
            d = list(filter(lambda x: f in x, d))
        d = list(map(lambda x: (str(x["Date"].year) \
                + str(x["Date"].month), func(x, special)), d))
        
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
        d = list(map(lambda x: (str(x["Date"].year) \
                + str(x["Date"].month), x[query_field]), d))
        for x in d:
            if x[0] in data_all:
                data_all[x[0]].append(x[1])
            else:
                data_all[x[0]]=[x[1]]
        
        if i == math.ceil(len(stocks)/5 * last):
            print(100*i/len(stocks),"% complete.")
            last += 1
    
    return data_all


def value_ratio(feature_dict, field):
    try:
        return feature_dict[field] / (feature_dict[""] \
            + feature_dict[""])
    except ZeroDivisionError:
        return 0


def market_cap_quantiles(db, stocks):
    
    quantile_by_date = {}
    data = get_data(db, stocks, "")
    for key in data:
        quantile_by_date[key] = np.quantile(data[key], [0.25,0.75])
    print(quantile_by_date)
    return quantile_by_date


def ebitda_value(db, stocks):
    top_quart_by_date = {}
    data = get_data_multiple(db, stocks, [], value_ratio, "")
    for key in data:
        top_quart_by_date[key] = np.quantile(data[key], [0.75])
    print(top_quart_by_date)
    return top_quart_by_date


def leverage(db, stocks):
    median_by_date = {}
    data = get_data_multiple(db, stocks, [""], value_ratio, "")
    for key in data:
        median_by_date[key] = np.quantile(data[key], [0.5])
    print(median_by_date)
    return median_by_date



if __name__ == "__main__":
    client = get_client()
    db = client["Stocks"]

    stocks = get_stock_list(db)
    market_cap_quantiles(db, stocks)
    ebitda_value(db, stocks)
    leverage(db, stocks)


