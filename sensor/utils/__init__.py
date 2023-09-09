import pandas as pd
from sensor.logger import logging
from sensor.exception import CustomException
from sensor.config import mongo_client
import os,sys

def get_collection_as_dataframe(database_name:str,colloection_name:str)->pd.DataFrame:
    """_summary_
    This function return collection as dataframe
    ============================================
    Args:
        database_name (str): database name
        colloection_name (str): collection name
    ============================================
    Returns:
        pd.DataFrame: returns Pandas dataframe of a collection
    """
    try:
        logging.info(f"Reading the dataframe from database : {database_name} and collection :{colloection_name}")
        df = pd.DataFrame(list(mongo_client[database_name][colloection_name].find()))
        logging.info(f"Found Colums : {df.columns}")
        if "_id" in df.columns:
            logging.info(f"Dropping the columns: _id ")
            df = df.drop("_id",axis=1)
        logging.info(f"Rows and Columns in df: {df.shape}")
        return df
    except Exception as e:
        raise CustomException(e,sys)
        