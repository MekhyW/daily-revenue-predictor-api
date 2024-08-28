import pandas as pd
import sys
import model
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

load_dotenv("../.env")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_DATABASE = os.getenv("DB_DATABASE")
connection_string = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_DATABASE}"
engine = create_engine(connection_string)

with engine.connect() as connection:
    with open("data/predict.sql", "r") as file:
        sql_script = file.read()
        connection.execute(sql_script)

model_path = sys.argv[1]
data_path = sys.argv[2]
pipeline = model.load_model(model_path)
df = pd.read_parquet(data_path)

df["total_sales"] = pipeline.predict(df)

with engine.connect() as connection:
    for _, row in df.iterrows():
        connection.execute(
            """
            INSERT INTO sales_analytics.scoring_ml_felipec13 (store_id, date_sale, total_sales)
            VALUES (%s, %s, %s)
            """,
            (row['store_id'], row['date_sale'], row['total_sales'])
        )

print(f"Predictions saved in the sales_analytics.scoring_ml_felipec13 table!")
