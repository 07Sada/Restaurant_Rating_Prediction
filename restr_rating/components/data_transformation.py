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


    def encode_categorical_variables(self, base: pd.DataFrame, train_df: pd.DataFrame, test_df: pd.DataFrame) -> (pd.DataFrame, pd.DataFrame):
    # fit .factorize() method on base_df
        unique_values = {}
        for column in base.columns:
            unique_values[column] = base[column].unique()

        # transform categorical variables in train_df and test_df using unique values
        for column in train_df.columns:
            if train_df[column].dtype == 'object':
                train_df[column] = pd.Categorical(train_df[column], categories=unique_values[column]).codes
            if test_df[column].dtype == 'object':
                test_df[column] = pd.Categorical(test_df[column], categories=unique_values[column]).codes

        return train_df, test_df

                

    def initiate_data_transformation(self)->artifact_entity.DataTransformationArtifact:
        try: 
            # reading the training and testing files
            logging.info("Reading train and test files in data_transformation.py")
            base_df = pd.concat(pd.read_csv('/config/workspace/zomato_cleaned.csv', chunksize = 5000)).drop(['address', 'reviews_list'], axis=1)
            test_df = pd.concat(pd.read_csv(self.data_ingestion_artifact.test_file_path, chunksize = 5000)).drop(['address', 'reviews_list'], axis=1)
            train_df = pd.concat(pd.read_csv(self.data_ingestion_artifact.train_file_path, chunksize = 5000)).drop(['address', 'reviews_list'], axis=1)
            logging.info(f"train and test file read")
            
            # selecting input feature for encoding
            en_train_df = train_df.drop(ENCODE_EXCLUDE_COLUMN, axis=1)
            en_test_df = test_df.drop(ENCODE_EXCLUDE_COLUMN, axis=1)
            
            train_encode, test_encode = self.encode_categorical_variables(base=base_df, train_df=en_train_df, test_df=en_test_df)
             

            train_df_encoded = pd.concat([train_df[ENCODE_EXCLUDE_COLUMN], train_encode], axis=1)
            test_df_encoded = pd.concat([test_df[ENCODE_EXCLUDE_COLUMN], test_encode], axis=1)

            # saving into numpy array
            logging.info(f"Saving the transformed dataframe into numpy array")
            logging.info(f"file path: {self.data_transformation_config.transformed_train_path}")
            utils.save_numpy_array_data(file_path=self.data_transformation_config.transformed_train_path, df=train_df_encoded)
            utils.save_numpy_array_data(file_path=self.data_transformation_config.transformed_test_path, df=test_df_encoded)

            # creating the artifacts
            data_transformation_artifact = artifact_entity.DataTransformationArtifact(
                                            transformed_train_path= self.data_transformation_config.transformed_train_path, 
                                            transformed_test_path = self.data_transformation_config.transformed_test_path,
                                            )
            
            logging.info(f"Data Transformation Done")
            return data_transformation_artifact
        
        except Exception as e:
            raise RatingException(e, sys)
