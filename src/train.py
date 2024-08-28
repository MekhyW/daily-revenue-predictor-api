import os
import pandas as pd
import pickle
from sqlalchemy import create_engine
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import train_test_split
from sklearn.compose import make_column_transformer
from sklearn.preprocessing import OneHotEncoder
from sklearn.pipeline import Pipeline
from dotenv import load_dotenv

load_dotenv(".env")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_DATABASE = os.getenv("DB_DATABASE")
connection_string = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_DATABASE}"
engine = create_engine(connection_string)

with open("data/train.sql", "r") as file:
    sql_query = file.read()
df = pd.read_sql_query(sql_query, engine)

X = df.drop("total_sales", axis=1)
y = df["total_sales"]
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=195)

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
