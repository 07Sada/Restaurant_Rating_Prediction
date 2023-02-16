# Importing necessary libraries
from dataclasses import dataclass
from datetime import datetime 
import os, sys 
from restr_rating.exception import RatingException
from restr_rating.logger import logging

# Defining constants for filenames
FILE_NAME ='ratings.csv'
TRAIN_FILE_NAME = 'train.csv'
TEST_FILE_NAME = 'test.csv'
MODEL_FILE_NAME = 'model.pkl'
TRANSFORMER_OBJECT_FILE_NAME = 'transformer.pkl'
TARGET_ENCODER_OBJECT_FILE_NAME = 'target_encoder.pkl'


# Class to store the configuration for the training pipeline
class TrainingPipelineConfig:
    def __init__(self):
        try: 
            # Defining the artifact directory, with a unique name based on the current time
            self.artifact_dir = os.path.join(os.path.join(os.getcwd(),'artifact', f"{datetime.now().strftime('%m_%d_%Y__%I_%M_%S')}"))
        except Exception as e:
            # Raising an exception in case of any error
            raise RatingException(e, sys)

# Class to store the configuration for the data ingestion process
class DataIngestionConfig:
    def __init__(self, training_pipeline_config:TrainingPipelineConfig):
        try:
            # Defining constants for the database and collection names
            self.database_name = 'restr_ratings'
            self.collection_name = 'ratings'

            # Defining the directories for data ingestion
            # creating data_ingestion directory
            self.data_ingestion_dir =os.path.join(training_pipeline_config.artifact_dir,'data_ingestion')

            # creating feature_store file path 
            self.feature_store_dir = os.path.join(self.data_ingestion_dir,'feature_store', FILE_NAME)

            # creating train_file_path 
            self.train_file_name = os.path.join(self.data_ingestion_dir,'dataset',TRAIN_FILE_NAME)

            # creating test_file path
            self.test_file_name = os.path.join(self.data_ingestion_dir,'dataset',TEST_FILE_NAME)

            # Defining the test size as a constant
            self.test_size:float=0.2
        
        except Exception as e:
            # Raising an exception in case of any error
            raise RatingException(e, sys)

    # Method to convert the object into a dictionary
    def to_dict(self)->dict:
        try:
            return self.__dict__
        
        except Exception as e:
            # Raising an exception in case of any error
            raise RatingException(e, sys)

class DataValidationConfig:
    def __init__(self, training_pipeline_config:TrainingPipelineConfig):
        self.data_validation_dir = os.path.join(training_pipeline_config.artifact_dir,"data_validation")
        self.report_file_path = os.path.join(self.data_validation_dir,'report.yaml')
        self.missing_threshold:float =0.2
        self.base_file_path = os.path.join("/config/workspace/zomato_cleaned.csv")
        
class DataTransformationConfig:
    def __init__(self, training_pipeline_config:TrainingPipelineConfig):
        self.data_transformation_dir = os.path.join(training_pipeline_config.artifact_dir, "data_transformation")
        self.transformed_train_path = os.path.join(self.data_transformation_dir,"transformed",TRAIN_FILE_NAME.replace("csv", "npz"))
        self.transformed_test_path = os.path.join(self.data_transformation_dir,"transformed",TEST_FILE_NAME.replace("csv", "npz"))
    