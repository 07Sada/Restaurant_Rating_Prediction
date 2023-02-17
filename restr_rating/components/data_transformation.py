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

    def Encode (self, df:pd.DataFrame):
    # Initialize the LabelEncoder object
        le = LabelEncoder()

        # Iterate through columns in the DataFrame
        for column in df.columns[~df.columns.isin(ENCODE_EXCLUDE_COLUMN)]:
            # Fit and transform the categorical column using LabelEncoder
            le.fit(df[column])
            df[column] = le.transform(df[column])
        # Return the encoded DataFrame
        return df

                

    def initiate_data_transformation(self)->artifact_entity.DataTransformationArtifact:
        try: 
            # reading the training and testing files
            logging.info("Reading train and test files in data_transformation.py")
            test_df = pd.concat(pd.read_csv(self.data_ingestion_artifact.test_file_path, chunksize = 5000))
            train_df = pd.concat(pd.read_csv(self.data_ingestion_artifact.train_file_path, chunksize = 5000))
            logging.info(f"train and test file read")
            
            # selecting input feature for encoding
            en_train_df = train_df.drop(ENCODE_EXCLUDE_COLUMN, axis=1)
            en_test_df = test_df.drop(ENCODE_EXCLUDE_COLUMN, axis=1)
            
            train_encode = self.Encode(df=en_train_df)
            test_encode = self.Encode(df=en_test_df)

            train_df = pd.concat([train_df[ENCODE_EXCLUDE_COLUMN], train_encode], axis=1)
            test_df = pd.concat([test_df[ENCODE_EXCLUDE_COLUMN], test_encode], axis=1)

            # saving into numpy array
            logging.info(f"Saving the transformed dataframe into numpy array")
            logging.info(f"file path: {self.data_transformation_config.transformed_train_path}")
            utils.save_numpy_array_data(file_path=self.data_transformation_config.transformed_train_path, df=train_df)
            utils.save_numpy_array_data(file_path=self.data_transformation_config.transformed_test_path, df=test_df)

            # creating the artifacts
            data_transformation_artifact = artifact_entity.DataTransformationArtifact(
                                            transformed_train_path= self.data_transformation_config.transformed_train_path, 
                                            transformed_test_path = self.data_transformation_config.transformed_test_path,
                                            )
            
            logging.info(f"Data Transformation Done")
            return data_transformation_artifact
        
        except Exception as e:
            raise RatingException(e, sys)
