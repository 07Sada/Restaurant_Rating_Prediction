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

    def Encode(df:pd.DataFrame):
    # Initialize the LabelEncoder object
        le = LabelEncoder()
        
        # Iterate through columns in the DataFrame
        for column in df.columns[~df.columns.isin(ENCODE_EXCLUDE_COLUMN)]:
            # Fit and transform the categorical column using LabelEncoder
            df[column] = le.fit_transform(df[column])
        # Return the encoded DataFrame
        return df

    def initiate_data_transformation(self)->artifact_entity.DataTransformationArtifact:
        try: 
            # reading the training and testing files
            logging.info("Reading train and test files in data_transformation.py")
            train_df = pd.read_csv(self.data_ingestion_artifact.train_file_path)
            test_df = pd.read_csv(self.data_ingestion_artifact.test_file_path)

            # transforming training and testing dataset
            logging.info(f"Transforming train and test dataframes")
            transformed_train_path = self.Encode(train_df)
            transformed_test_path = self.Encode(test_df)

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
