from sensor.predictor import ModelResolver
from sensor.entity.config_entity import ModelPusherConfig
from sensor.exception import CustomException
import os,sys
from sensor.utils import load_object,save_object
from sensor.logger import logging
from sensor.entity.artifact_entity import DataTransformationArtifact,ModelTrainerArtifact,ModelPusherArtifact

class ModelPusher:
    
    def __init__(self,model_pusher_config:ModelPusherConfig,
                 data_transformation_artifact:DataTransformationArtifact,
                 model_trainer_artifact:ModelTrainerArtifact
                 ):
        try:
            logging.info(f"{'>>'*20} Data Transformation {'<<'*20}")
            self.model_pusher_config=model_pusher_config
            self.data_transformation_artifact=data_transformation_artifact
            self.model_trainer_artifact=model_trainer_artifact
            self.model_resolver = ModelResolver(model_registry=self.model_pusher_config.saved_model_dir)
        except Exception as e:
            raise CustomException(e,sys)
        
    
    def initiate_model_pusher(self,)->ModelPusherArtifact:
        try:
            #Load object
            logging.info("Loading transformer model and target encoder")
            transfomer = load_object(file_path=self.data_transformation_artifact.transform_object_path)
            model = load_object(file_path=self.model_trainer_artifact.model_path)
            target_encoder = load_object(file_path=self.data_transformation_artifact.target_encoder_path)
            
            #model pusher dir
            logging.info("Saving model in saved model dir")
            transfomer_path = self.model_resolver.get_latest_save_transformer_path()
            model_path = self.model_resolver.get_latest_save_model_path()
            target_encoder_path = self.model_resolver.get_latest_save_target_encoder_path()
            
            save_object(file_path=transfomer_path,obj=transfomer)
            save_object(file_path=model_path,obj=model)
            save_object(file_path=target_encoder_path,obj=target_encoder)
            
            models_pusher_artifact = ModelPusherArtifact(
                pusher_model_dir=model_pusher_config.pusher_model_dir,
                saved_model_dir=self.model_pusher_config.saved_model_dir)
            logging.info(f"Model Pusher artifact: {model_pusher_artifact}")                    
            return models_pusher_artifact
        except Exception as e:
            raise CustomException(e,sys)
    