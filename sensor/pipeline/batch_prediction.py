from sensor.exception import CustomException
from sensor.logger import logging
from sensor.predictor import ModelResolver
import pandas as pd
from sensor.utils import load_object
import os,sys
from datetime import datetime

PREDICTION_DIR="prediction"
import numpy as np

def start_batch_prediction(input_file_path):
    
    try:
        os.makedirs(PREDICTION_DIR,exist_ok=True)
        logging.info(f"Creating Model Resolver object")
        model_resolver = ModelResolver(model_registry="saved_models")
        logging.info(f"Reading file :{input_file_path}")
        df = pd.read_csv(input_file_path)
        df.replace({'na':np.NAA},inplace=True)
        
        #validation
        logging.info(f"loading transformer to transform dataset")
        transformer = load_object(file_path=model_resolver.get_latest_transformer_path)
        
        input_feature_names = list(transformer.feature_names_in_)
        input_arr = transformer.transform(df[input_feature_names])
        
        
        logging.info("Loading the model to make prediction")
        model = load_object(file_path=model_resolver.get_latest_model_path())
        prediction = model.predict(input_arr)
        
        logging.info("Target encoder to convert predicted column into categorical")
        target_encoder = load_object(file_path=model_resolver.get_latest_encoder_path())
        
        cat_prediction = target_encoder.inverse_transform(prediction)
        
        df['Prediction'] = prediction
        df['cat_pred'] = cat_prediction
        
        prediction_file_name = os.path.basename(input_file_path).replace(".csv",f"{datetime.now().strftime('%m_%d_%Y_%H_%M_%S')}.csv")
        prediction_file_path = os.path.join(PREDICTION_DIR,prediction_file_name)
        df.to_csv(prediction_file_path,index=False,header=True)
        
        return prediction_file_path
    except Exception as e:
        raise CustomException(e,sys)