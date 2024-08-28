import pandas as pd
import model

def init_predict_table(engine):
    with engine.connect() as connection:
        with open("data/predict.sql", "r") as file:
            sql_script = file.read()
            connection.execute(sql_script)
    print("Table sales_analytics.scoring_ml_felipec13 created!")

def predict(engine, data_path, pipeline):
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
