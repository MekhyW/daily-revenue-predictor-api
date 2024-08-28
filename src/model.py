import pickle
import boto3
import os

def setup_s3(key, secret):
    s3 = boto3.client(
        "s3",
        aws_access_key_id=key,
        aws_secret_access_key=secret
    )
    return s3

def save_model(s3, pipeline, bucket_name, key, filename="pipeline.pkl"):
    with open(filename, "wb") as f:
        pickle.dump(pipeline, f)
    s3.upload_file(filename, bucket_name, key)
    os.remove(filename)

def load_model(s3, bucket_name, key):
    obj = s3.get_object(Bucket=bucket_name, Key=key)
    with open("pipeline.pkl", "wb") as f:
        f.write(obj["Body"].read())
    with open("pipeline.pkl", "rb") as f:
        pipeline = pickle.load(f)
    os.remove("pipeline.pkl")
    return pipeline