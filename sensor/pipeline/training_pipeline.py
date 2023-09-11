from sensor.logger import logging
from sensor.exception import CustomException
from sensor.utils import get_collection_as_dataframe
import sys,os
from sensor.entity import config_entity
from sensor.components.data_ingestion import DataIngestion
from sensor.components.data_validation import DataValidation
from sensor.components.data_transformation import DataTransofrmation
from sensor.components.model_trainer import ModelTrainer
from sensor.components.model_evaluation import ModelEvaluation
from sensor.components.model_pusher import ModelPusher

def start_training_pipeline():
    try:
        training_pipeline_config = config_entity.TrainingPipelineConfig()
        
        #data Ingestion
        data_ingestion_config = config_entity.DataIngestionConfig(training_pipeline_config=training_pipeline_config)
        print(data_ingestion_config.to_dict())
        data_ingestion = DataIngestion(data_ingestion_config=data_ingestion_config)
        data_ingestion_artifact = data_ingestion.initiate_data_ingestion()
        
        #data validation
        data_validation_config = config_entity.DataValidationConfig(training_pipeline_config=training_pipeline_config)
        data_validation = DataValidation(data_validation_config=data_validation_config,
                                         data_ingestion_artifact=data_ingestion_artifact)
        data_validation_artifact = data_validation.initiate_data_validation()
        
        #data Transformation
        data_transfomration_config = config_entity.DataTransformationConfig(training_pipeline_config=training_pipeline_config)
        data_transformation = DataTransofrmation(data_transformation_config=data_transfomration_config,
                                                 data_ingestion_artifact=data_ingestion_artifact)
        data_transfomration_artifact = data_transformation.initiate_data_transformation()
        
        #model trainer
        model_trainer_config = config_entity.ModelTrainerConfig(training_pipeline_config=training_pipeline_config)
        model_trainer = ModelTrainer(model_trainfer_config=model_trainer_config,
                                     data_transformation_artifact=data_transfomration_artifact)
        model_trainer_artifact = model_trainer.initiate_model_trainer()
        
        #model evaluation
        model_eval_config = config_entity.ModelEvalutaionConfig(training_pipeline_config=training_pipeline_config)
        model_eval = ModelEvaluation(model_eval_config=model_eval_config,
                                     data_ingestion_artifact=data_ingestion_artifact,
                                     data_transformation_artifact=data_transfomration_artifact,
                                     model_trainer_artifact=model_trainer_artifact
                                     )
        model_eval_artifact = model_eval.initiate_model_evaluation()
        
        
        #model_pusher
        model_pusher_config = config_entity.ModelPusherConfig(training_pipeline_config)
        model_pusher = ModelPusher(model_pusher_config=model_pusher_config,
                                   data_transfomration_artifact=data_transfomration_artifact,
                                   model_trainer_artifact=model_trainer_artifact
                                   )
        model_pusher_artifact = model_pusher.initiate_model_pusher()
        
    except Exception as e:
        raise CustomException(e,sys)
         
        
          