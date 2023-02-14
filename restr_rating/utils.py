import os, sys 
from restr_rating.logger import logging
from restr_rating.exception import RatingException
import pandas as pd 
import numpy as np 
from restr_rating.config import mongo_client
import yaml
import dill


def get_collection_as_dataframe(database_name:str, collection_name:str)->pd.DataFrame:
    """
    Description: This function return collection as dataframe
    =========================================================
    database_name : database name 
    collection_name : collection name
    ========================================================
    return Pandas dataframe of a collection
    """
    try:
        logging.info(f"Reading data from database :[{database_name}] and collection : [{collection_name}]")

        # Creating a Pandas dataframe from the MongoDB data
        df:pd.DataFrame = pd.DataFrame(list(mongo_client[database_name][collection_name].find()))
        logging.info(f"Data Loaded Successfully to pandas dataframe, size of the data: [{df.shape}]")

        # Removing the "_id" column from the dataframe
        if "_id" in df.columns:
            logging.info(f"Removing the '_id' column from database")
            df.drop("_id", axis=1, inplace=True)
        
        return df 

    except Exception as e: 
        raise RatingException(e, sys)

def write_yaml_file(file_path:str, data:dict):
    try: 
        # get the directory path from the file path 
        file_dir = os.path.dirname(file_path)
        # create directory if not exists
        os.makedirs(name=file_dir, exist_ok=True)
        # open file in write mode
        with open(file_path,'w') as file_writer:
            yaml.dump(data, file_writer)
    except Exception as e:
        raise RatingException(e, sys)