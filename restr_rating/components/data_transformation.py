from dataclasses import dataclass 
from restr_rating.logger import logging
from restr_rating.exception import RatingException 
from restr_rating.entity import config_entity, artifact_entity
from typing import Optional 
import pandas as pd 
import numpy as np 
import os, sys 
from sklearn.pipeline import Pipeline 
from sklearn.preprocessing import LabelEncoder 
from restr_rating import utils
from restr_rating.config import TARGET_COLUMN, ENCODE_EXCLUDE_COLUMN

class DataTransformation:
    
    def __init__(self, data_transformation_config:config_entity.DataTransformationConfig,
                        data_ingestion_artifact:artifact_entity.DataIngestionArtifact):
        
        try:
            logging.info(f"{'>'*30} Initiated Data Transformation {'<'*30}")
            self.data_transformation_config = data_transformation_config
            self.data_ingestion_artifact = data_ingestion_artifact
        except Exception as e:
            raise RatingException(e, sys)

            
    @classmethod
    def get_data_transformer_object(cls)->Pipeline:
        try: 
            pipeline = Pipeline([('encoder', LabelEncoder())])
            return pipeline 
        except Exception as e:
            raise RatingException(e, sys)
                

    def initiate_data_transformation(self)->artifact_entity.DataTransformationArtifact:
        try: 
            # reading the training and testing files
            logging.info("Reading train and test files in data_transformation.py")
            test_df = pd.concat(pd.read_csv(self.data_ingestion_artifact.test_file_path, chunksize = 5000))
            # test_df = pd.read_csv(self.data_ingestion_artifact.test_file_path, chunksize = 5000)
            train_df = pd.concat(pd.read_csv(self.data_ingestion_artifact.train_file_path, chunksize = 5000))
            logging.info(f"train and test file read")
            
            # selecting input feature for encoding
            en_train_df = train_df.drop(ENCODE_EXCLUDE_COLUMN, axis=1)
            en_test_df = test_df.drop(ENCODE_EXCLUDE_COLUMN, axis=1)

            label_encoding = LabelEncoder()
            label_encoding.fit(en_train_df)
            label_encoding.fit(en_test_df)
            logging.info(f"Label Encoding done for train and test dataframe")

            # # transforming training and testing dataset
            # logging.info(f"Transforming train and test dataframes")
            # transformed_train_path = self.Encode(train_df)
            # transformed_test_path = self.Encode(test_df)
            # logging.info(f"Encoding Completed")

            # saving into numpy array
            logging.info(f"Saving the transformed dataframe into numpy array")
            utils.save_numpy_array_data(file_path=self.data_transformation_config.transformed_train_path, df=transformed_train_path)
            utils.save_numpy_array_data(file_path=self.data_transformation_config.transformed_test_path, df=transformed_test_path)

            # creating the artifacts
            data_transformation_artifact = artifact_entity.DataTransformationArtifact(
                                            transformed_train_path= self.data_transformation_config.transformed_train_path, 
                                            transformed_test_path = self.data_transformation_config.transformed_test_path)
            
            logging.info(f"Data Transformation Done")
            return data_transformation_artifact
        
        except Exception as e:
            raise RatingException(e, sys)
