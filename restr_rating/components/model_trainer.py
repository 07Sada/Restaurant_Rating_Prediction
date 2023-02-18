from restr_rating.entity import config_entity, artifact_entity
from restr_rating.logger import logging
from restr_rating.exception import RatingException
from typing import Optional
from restr_rating import utils
from sklearn.metrics import r2_score
from sklearn.ensemble import ExtraTreesRegressor
import os, sys

class ModelTrainer:
 
    def __init__(self,model_trainer_config:config_entity.ModelTrainerConfig,
                        data_transformation_artifact:artifact_entity.DataTransformationArtifact):

        try:
            logging.info(f"{'>'*30} Model Training Initiated {'<'*30}")
            self.model_trainer_config = model_trainer_config
            self.data_transformation_artifact = data_transformation_artifact
        
        except Exception as e:
            raise RatingException(e, sys)
        

    def train_model(self, x, y):
        try:
            ETree=ExtraTreesRegressor(n_estimators = 100)
            ETree.fit(x, y)
            return ETree
        except Exception as e:
            raise RatingException(e, sys)


    def initiate_model_trainer(self)->artifact_entity.ModelTrainerArtifact:
        try:
            logging.info(f"Loading train and test array")
            train_arr = utils.load_numpy_array(file_path=self.data_transformation_artifact.transformed_train_path)
            test_arr = utils.load_numpy_array(file_path=self.data_transformation_artifact.transformed_test_path)

            logging.info(f"Splitting input and target features from both train and test arr")
            x_train , y_train = train_arr[:,1:], train_arr[:,0]
            x_test, y_test = test_arr[:,1:], test_arr[:,0]

            logging.info(f"Train the model")
            model = self.train_model(x=x_train, y=y_train)

            logging.info(f"Calculating R2 score")
            y_hat_train = model.predict(x_train)
            r2_train_score = r2_score(y_true=y_train, y_pred=y_hat_train)
            logging.info(f"r2 score for the train dattase:[{r2_train_score}]")

            y_hat_test = model.predict(x_test)
            r2_test_score = r2_score(y_true=y_test, y_pred=y_hat_test)
            logging.info(f"r2 score for the test dattase:[{r2_test_score}]")

            # check the overfitting or underfitting or expected score
            logging.info(f"Checking if our model is underfitting or not")
            if r2_test_score < self.model_trainer_config.expected_score:
                raise Exception(f"Model is not good as it is not able to give\
                    expected accuracy: {self.model_trainer_config.expected_score}, model actual score:{r2_test_score}")
            
            logging.info(f"Checking if our model is overfitting or not")
            diff = abs(r2_train_score - r2_test_score)

            if diff > self.model_trainer_config.overfitting_threshold:
                raise Exception(f"Train and test score diff: {diff} is more than overfitting threshold: {self.model_trainer_config.overfitting_threshold}")
        
            #save the trained model
            logging.info(f"Saving the model object")
            utils.save_object(file_path=self.model_trainer_config.model_path, obj=model)

            # prepare the artifact:
            logging.info(f"Preparing the model trainer artifact")
            model_trainer_artifact = artifact_entity.ModelTrainerArtifact(
                model_path=self.model_trainer_config.model_path, 
                r2_train_score = r2_train_score, 
                r2_test_score = r2_test_score)
            
            return model_trainer_artifact
        
        except Exception as e:
            raise RatingException(e, sys)