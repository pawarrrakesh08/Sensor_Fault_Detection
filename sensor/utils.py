from sensor.exception import CustomException
from sensor.config import mongo_client
import sys,os
import yaml

def collection_as_dataframe(database_name:str,collection_name:str)->pd.DataFrame:
    """
    Description: This funtion return collection as dataframe
    """
    return df
    
    
def write_yaml_file(file_path,data:dict):
    try:
        file_dir = os.path.dirname(file_path)
        os.makedirs(file_dir,exist_ok=True)
        with open(file_path,"w") as file_writer:
            yaml.dump(data,file_writer)
    except Exception as e:
        raise CustomException(e,sys)
    
def convert_columns_float(df:pd.DataFrame,exclude_columns:list)->pd.DataFrame:
    try:
        for column in df.columns:
            if column not in exclude_columns:
                df[column] = df[column].astype('float')
        return df
    except Exception as e:
        raise CustomException(e,sys)
    