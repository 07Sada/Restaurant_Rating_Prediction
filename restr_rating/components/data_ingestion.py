from restr_rating.logger import logging
from restr_rating.exception import RatingException
import pandas as pd 
import numpy as np 
import sys, os 
from sklearn.model_selection import train_test_split
from restr_rating.entity import config_entity
from restr_rating.entity import artifact_entity
from restr_rating import utils


class DataIngestion:

    def __init__(self, data_ingestion_config:config_entity.DataIngestionConfig):
        try:
            logging.info(f"{'>'*30} Data Ingestion Initiated {'<'*30}")
            self.data_ingestion_config=data_ingestion_config
        except Exception as e:
            raise RatingException(e, sys)


    def initiate_data_ingestion(self)->artifact_entity.DataIngestionArtifact:
        try:
            logging.info("Exporting collection as dataframe")
            
            df:pd.DataFrame = utils.get_collection_as_dataframe(database_name=self.data_ingestion_config.database_name, 
                                                                collection_name=self.data_ingestion_config.collection_name)
            # saving data to feature store 
            # creating feature store directory
            logging.info(f"Creating feature store folder if not exists")
            feature_store_dir = os.path.dirname(self.data_ingestion_config.feature_store_dir)
            os.makedirs(feature_store_dir, exist_ok=True)

            # saving the data to feature store
            logging.info(f'Saving the data to feature store folder')
            df.to_csv(path_or_buf=self.data_ingestion_config.feature_store_dir, index=False, header=True)

            logging.info(f"Splitting the dataset into train and test set")
            # splitting the dataset
            train_df, test_df = train_test_split(df, test_size=self.data_ingestion_config.test_size, random_state=42)
            logging.info("Creating the dataset Directory if not available")
            dataset_dir = os.path.dirname(self.data_ingestion_config.test_file_name)
            os.makedirs(dataset_dir, exist_ok=True)

            logging.info(f"Saving train_df and test_df into dataset folder")
            train_df.to_csv(path_or_buf=self.data_ingestion_config.train_file_name, index=False, header =True)
            test_df.to_csv(path_or_buf=self.data_ingestion_config.test_file_name, index=False, header = True)

            # preparing the artifact
            data_ingestion_artifact = artifact_entity.DataIngestionArtifact(
                feature_store_file_path=self.data_ingestion_config.feature_store_dir,
                train_file_path = self.data_ingestion_config.train_file_name, 
                test_file_path = self.data_ingestion_config.test_file_name
                )

            logging.info(f"data_ingestion_artifact: {data_ingestion_artifact}")
            return data_ingestion_artifact
        
        except Exception as e:
            raise RatingException(e, sys)
    
            