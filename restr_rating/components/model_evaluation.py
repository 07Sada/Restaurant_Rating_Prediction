from restr_rating.predictor import ModelResolver
from restr_rating.entity import config_entity, artifact_entity
from restr_rating.exception import RatingException
from restr_rating.logger import logging 
from restr_rating.utils import load_object
from sklearn.metrics import r2_score
import pandas as pd 
import numpy as np 
import sys, os 
from restr_rating import utils

class ModelEvaluation:
    
    def __init__(self, 
                model_eval_config:config_entity.ModelEvaluationConfig,
                data_ingestion_artifact:artifact_entity.DataTransformationArtifact,
                data_transformation_artifact:artifact_entity.DataTransformationArtifact,
                model_trainer_artifact:artifact_entity.ModelTrainerArtifact):
        try:
            logging.info(f"{'>'*30} Model Evaluation Initiated {'<'*30}")
            self.model_eval_config = model_eval_config
            self.data_ingestion_artifact = data_ingestion_artifact
            self.data_transformation_artifact = data_transformation_artifact
            self.model_trainer_artifact = model_trainer_artifact 
            self.model_resolver = ModelResolver()
        except Exception as e:
            raise RatingException(e, sys)

    def initiate_model_evaluation(self)->artifact_entity.ModelEvaluationArtifact:
        try:
            # if the model folder has model then we will compare
            # which model is best trained or the the model from saved model folder 

            logging.info(f"If saved model folder has model then we will compare which model is best trained or the model from saved model folder")
                
            latest_dir_path = self.model_resolver.get_latest_dir_path()
            logging.info(f"latest dir: {latest_dir_path}")
            if latest_dir_path == None:
                model_eval_artifact = artifact_entity.ModelEvaluationArtifact(is_model_accepted=True, improved_accuracy=None)
                logging.info(f"Model Evaluation artifact: {model_eval_artifact}")
                # return model_eval_artifact

            # finding the location of transformer model 
            logging.info(f"Finding location of model")
            model_path = self.model_resolver.get_latest_model_path()
            logging.info(f"model path {model_path}")
            logging.inf(f"Previous trained objects of transformer, model")
            # previous trained object 
            model = load_object(file_path = model_path)

            logging.info(f"Currenlty trained model objects")
            # currently trained model objects
            current_model = load_object(file_path=self.model_trainer_artifact.model_path)
            
            # importing and loading the dataset
            test_arr = utils.load_numpy_array(file_path=self.data_transformation_artifact.transformed_test_path)
            x_test, y_test = test_arr[:,1:], test_arr[:,0]

            # accuracy using the previous trained model 
            y_pred = model.predict(x_test)
            previous_model_r2_score = r2_score(y_true=y_test, y_pred=y_pred)
            logging.info(f"R2 score for previous trained model is: {previous_model_r2_score}")

            # r2 score using the current trained model 
            y_pred = current_model.predict(x_test)
            current_model_r2_score = r2_score(y_true=y_test, y_pred=y_pred)
            logging.info(f"R2 score for previous current model is: {current_model_r2_score}")

            if current_model_r2_score <= previous_model_r2_score:
                logging.info(f" Currently trained model is not better than previous model")
                raise Exception(f"Currently trained model is not better than previous model")

            model_eval_artifact = artifact_entity.ModelEvaluationArtifact(is_model_accepted=True, 
                            improved_accuracy=current_model_r2_score - previous_model_r2_score)
            logging.info(f"Model eval artifact created")
            return model_eval_artifact
        
        except Exception as e:
            raise RatingException(e, sys)