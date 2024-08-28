import pandas as pd
import pickle
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import train_test_split
from sklearn.compose import make_column_transformer
from sklearn.preprocessing import OneHotEncoder
from sklearn.pipeline import Pipeline

def select_data_training(engine):
    with open("data/train.sql", "r") as file:
        sql_query = file.read()
    df = pd.read_sql_query(sql_query, engine)
    return df

def train_model(df):
    X = df.drop("total_sales", axis=1)
    y = df["total_sales"]
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.01, random_state=195)
    cat_cols = ["store_id"]
    num_cols = ["year", "month", "day", "weekday"]
    one_hot_enc = make_column_transformer(
        (OneHotEncoder(handle_unknown="ignore", drop="first"), cat_cols),
        remainder="passthrough"
    )
    pipeline = Pipeline([
        ('one_hot_enc', one_hot_enc),
        ('model', RandomForestRegressor(n_estimators=100, random_state=195))
    ])
    pipeline.fit(X_train, y_train)
    with open("models/pipeline.pkl", "wb") as f:
        pickle.dump(pipeline, f)
    return pipeline
