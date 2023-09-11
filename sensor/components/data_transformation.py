from sensor.entity import artifact_entity,config_entity
from sensor.exception import CustomException
from sensor.logger import logging
from typing import Optional
import os,sys
from sklearn.pipeline import Pipeline
import pandas as pd
from sensor import utils
import numpy as np
from sklearn.preprocessing import LabelEncoder
from imblearn.combine import SMOTETomek
from sklearn.impute import SimpleImputer
from sklearn.preprocessing import RobustScaler
from sensor.config import TARGET_COLUMN


class DataTransofrmation:
    
    def __init__(self,data_transformation_config:config_entity.DataTransformationConfig,
                 data_ingestion_artifcat:artifact_entity.DataIngestionArtifact):
        try:
            logging.info(f"{'<<'*20} Data Transformation {'<<'*20}")
            self.data_transformation_config=data_transformation_config
            self.data_ingestion_artifact=data_ingestion_artifcat
            
        except Exception as e:
            raise CustomException(e,sys)

@classmethod
def get_data_transformer_object(cls)->Pipeline:
    try:
        simple_imputer = SimpleImputer(strategy='costant',fill_value=0)
        robust_scaler = RobustScaler()
        pipeline = Pipeline(steps=[
         ('Imputer',simple_imputer),
         ('RobustScaler',robust_scaler)   
        ])
        
        return pipeline
    except Exception as e:
        raise CustomException(e,sys)
    
def initaiate_data_transformation(self,)-> artifact_entity.DataTransformationArtifact:
    try:
        
        logging.info("Reading the test and train file") 
        train_df = pd.read_csv(self.data_ingestion_aritifact.train_file_path)
        test_df = pd.read_csv(self.data_ingestion_artifact.test_file_path)
        
        logging.info("selecting input feature from train and test dataframes")
        input_feature_train_df = train_df.drop(TARGET_COLUMN,axis=1)
        input_feature_test_df = test_df.drop(TARGET_COLUMN,axis=1)
        
        logging.info("Selecting target feature for train and test dataframe")
        target_feature_train_df = train_df[TARGET_COLUMN]
        target_feature_test_df = test_df[TARGET_COLUMN]

        label_encoder = LabelEncoder()
        label_encoder.fit(target_feature_train_df)
        
        logging.info("Transformation on target column")
        target_feature_train_arr = label_encoder.transform(target_feature_train_df)
        target_feature_test_arr = label_encoder.transform(target_feature_test_df)
        
        transformatiovn_pipeline = DataTransofrmation.get_data_transformer_object()
        transformatiovn_pipeline.fit(input_feature_train_df)
        
        logging.info("transforming input features")
        input_feature_train_arr = transformatiovn_pipeline.transform(input_feature_train_df)
        input_feature_test_arr = transformatiovn_pipeline.transform(input_feature_test_df)
        
        
        smt = SMOTETomek(sampling_strategy="minority")
        
        logging.info(f"Before resampling in training set input :{input_feature_train_arr.shape} Target :{target_feture_train_arr.shape}")
        input_feature_train_arr,target_feture_train_arr = smt.fit_resample(input_feature_train_arr,target_feture_train_arr)
        logging.info(f"After resampling in training set Input :{input_feature_train_arr.shape} Target: {target_feture_train_arr.shape}")
        
        logging.info(f"Before resampling in testing set input : {input_feature_test_arr.shape} Target : {target_feature_test_arr.shape}")
        input_feature_test_arr,target_feature_test_arr = smt.fit_resample(input_feature_test_arr,target_feature_test_arr)        
        logging.info(f"After resampling in testing set input : {input_feature_test_arr.shape} Target : {target_feature_test_arr.shape}")        
        
        
        #Target encoder
        train_arr = np.c_[input_feature_train_arr,target_feature_train_arr]
        test_arr = np.c_[input_feature_test_arr,target_feature_test_arr]
        
        
        #save numpy array
        utils.save_numpy_array_data(file_path=self.data_transformation_config.transformed_train_path,array=train_arr)
        utils.save_numpy_array_data(file_path=self.data_transformation_config.transformed_test_path,array=test_arr)
        
        
        utils.save_object(file_path=self.data_transformation_config.transform_object_path,obj=transformatiovn_pipeline)
        utils.save_object(file_path=self.data_transformation_config.target_encoder_path,obj=label_encoder)
        
        data_transformation_aritifact = artifact_entity.DataTransformationArtifact(
            transform_object_path=self.data_transformation_config.transform_object_path,
            transformed_train_path=self.data_transformation_config.transformed_train_path,
            transformed_test_path=self.data_transformation_config.transformed_test_path,
            target_encoder_path=self.data_transformtion_config.target_encoder_path
        )
        
        logging.info(f"Data transformation object :{data_transformation_aritifact}")
        
        return data_transformation_aritifact
    
    except Exception as e:
        raise CustomException(e,sys)  