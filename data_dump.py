import pymongo
import pandas as pd
import json

#provide the mongodb local host url to connect oython to mongodb

client = pymongo.MongoClient("mongodb+srv://admin:admin@cluster0.qqzjykg.mongodb.net/")


DATA_FILE_PATH = "./aps_failure_training_set1.csv"
DATABASE_NAME = "aps"
COLLECTION_NAME = "sensor"

if __name__=="__main__":
    df = pd.read_csv(DATA_FILE_PATH,index_col=0)
    print(f"Rows and columns: {df.shape}")
    
    df.reset_index(drop=True,inplace=True)
    
    json_record = list(json.loads(df.T.to_json()).values())
    print(json_record[0])
    
    #insert converted json record to mongodb
    client[DATABASE_NAME][COLLECTION_NAME].insert_many(json_record)