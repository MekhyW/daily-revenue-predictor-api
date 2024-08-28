from fastapi import FastAPI, HTTPException, Depends
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from pydantic import BaseModel
import sqlite3
import predict
import train
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

class Revenue(BaseModel):
    store_id: int
    year: int
    month: int
    day: int
    weekday: int

def init_db_view():
    with open("data/create_view_abt_train_felipec13.sql", "r") as file:
        sql_query = file.read()
    with engine.connect() as connection:
        connection.execute(sql_query)
    print("View abt_train_felipec13 created!")

app = FastAPI()
bearer = HTTPBearer()
init_db_view()

def get_username_for_token(token: str) -> str:
    """
    Validate the token by checking it against the SQLite database.
    """
    conn = sqlite3.connect('tokens.db')
    cursor = conn.cursor()
    cursor.execute('SELECT username FROM tokens WHERE token = ?', (token,))
    result = cursor.fetchone()
    conn.close()
    if result:
        return result[0]
    return ""

async def validate_token(credentials: HTTPAuthorizationCredentials = Depends(bearer)):
    token = credentials.credentials
    username = get_username_for_token(token)
    if not username:
        raise HTTPException(status_code=401, detail="Invalid token")
    return {"username": username}

@app.get("/")
async def root():
    """
    Route to check that API is alive!
    """
    return "Model API is alive!"

@app.post("/train")
async def train(user: dict = Depends(validate_token)):
    """
    Route to train the model!
    """
    train_data = train.select_data_training(engine)
    pipeline = train.train_model(train_data)
    return {"message": "Model trained successfully!"}

@app.post("/predict")
async def predict(revenue: Revenue, user: dict = Depends(validate_token)):
    """
    Route to make predictions!
    """
    predict.init_predict_table(engine)
    predict.predict(engine, revenue.dict())
    return {"message": "Predictions made successfully!"}