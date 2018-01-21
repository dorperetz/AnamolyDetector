import pandas as pd
import pymongo
from sklearn.ensemble import IsolationForest
from pymongo import MongoClient
import pprint
import threading

# cleaning
ID_COLUMN_DIST_RATIO_THRESHOLD = 1.0  # threshold for setting a columns as id column using (unique values / all values)
CATEGORICAL_COLUMN_DIST_RATIO_THRESHOLD = 0.1
# threshold for setting a column as categorical using (unique values / all values)
CATEGORICAL_COUNT_THRESHOLD = 5  # threshold use to differ categorical variables that are of numerical type
MISSING_VALUES_REPRESENTATION = 'NaN'  # indicates which types are considered as missing values in pandas DataFrame
DROP_ABOVE_NULL_THRESHOLD = 0.6  # percents [0.0 - 1.0]
ISOLATION_FOREST_N_ESTIMATORS = 100  # [int > 0] isolation forest is used for removing outliers in data
KNN_N_NEIGHBORS = 4  # [int > 0] knn is used to impute missing data

def detect_anomalies_with_isolation_forest(X, contamination=0.1):
    """
    given a pandas DataFrame returns outliers indexes using isolation forest to detect outliers.

    In data mining, anomaly detection (also outlier detection) is the identification of items,
    events or observations which do not conform to an expected pattern or other items in a dataset.

    :param y: [pandas series] target column
    :param X: [pandas DataFrame] raw features
    :param contamination:  the proportion of outliers in the data set
    :return: outliers indexes
    """
    assert (isinstance(X, pd.DataFrame)) and (not X.empty), 'X should be a valid pandas DataFrame'
    df = X.copy()
    clf = IsolationForest(max_samples=len(df.index), n_estimators=ISOLATION_FOREST_N_ESTIMATORS,
                          contamination=contamination)

    columns = df.columns.tolist()
    columns = [c for c in columns if c not in ["_id"]]

    sanitizedColums = df[columns]
    clf.fit(sanitizedColums)
    Y = clf.predict(sanitizedColums)
    outliers = []
    for i, is_inliner in enumerate(Y):
        if is_inliner == -1:
            outliers.append(i)
    return df.index[outliers]

def checkForUpdates(stats) :
    cursor = stats.find({"id":{'$gte':0,'$lt': 3}})
    df = pd.DataFrame(list(cursor))

    detect_anomalies_with_isolation_forest(df)

def updateAnamolisInDb(anamolys,collection) :
    collection.find({})



client = MongoClient('mongodb://dockmon:bRX-SGD-DZQ-26o@ds111078.mlab.com:11078/dockmon')
db = client['dockmon']
collection = db.stats

threading.Timer(3.0,checkForUpdates(collection)).start()