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
        df:pd.DataFrame = pd.DataFrame(list(mongo_client[database_name][collection_name].find({}, {"_id": 0, "Unnamed: 0": 0})))
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

def filter_and_clean(df:pd.DataFrame):
    try:
        df = df.loc[df.rate != 'NEW']
        df = df.loc[df.rate != '-'].reset_index(drop=True)
        df['rate'] = df['rate'].replace(r'[^\S]|/5', '', regex=True).replace(r'-|NEW|nan',np.nan, regex=True).astype(float)
        return df
    except Exception as e:
        raise RatingException(e, sys)

def clean_cost_column(df:pd.DataFrame):
    try:
        df['cost'] = df['cost'].str.replace(',', '').astype('int32')
        return df['cost']
    except Exception as e:
        raise RatingException(e, sys)

def save_numpy_array_data(file_path:str, df:pd.DataFrame):
    """
    Save Pandas DataFrame data as a NumPy array to file
    file_path: str location of file to save
    df: pd.DataFrame data to save
    """
    try:
        dir_path = os.path.dirname(file_path)
        os.makedirs(dir_path, exist_ok=True)
        array = df.to_numpy()  # Convert DataFrame to NumPy array
        with open(file_path, "wb") as file_obj:
            np.save(file_obj, array)
    except Exception as e:
        raise RatingException(e, sys)

def load_numpy_array(file_path:str)->np.array:
    """
    load numpy array data from file 
    file_path: str location of file to load
    return np.array data loaded
    """
    try:
        with open(file_path,"rb") as file_obj:
            return np.load(file_obj, allow_pickle=True)
    except Exception as e:
        raise RatingException(e, sys)

def save_object(file_path:str, obj:object)->None:
    try:
        logging.info(f"Entered the save object method of utils")
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        with open(file_path, "wb") as file_obj:
            dill.dump(obj, file_obj)
        logging.info(f"Exited the save_object method of utils")
    except Exception as e:
        raise RatingException(e, sys)


def save_encoding_to_dill(unique_values: dict, encoded_base: pd.DataFrame, file_path: str):
    try:
        logging.info(f"Entered the save object method of utils")
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        with open(file_path, 'wb') as file:
            dill.dump({'unique_values': unique_values, 'encoded_base': encoded_base}, file)
    except Exception as e:
        raise RatingException(e, sys)

def load_object(file_path: str, ) -> object:
    try:
        if not os.path.exists(file_path):
            raise Exception(f"The file: {file_path} is not exists")
        with open(file_path, "rb") as file_obj:
            return dill.load(file_obj)
    except Exception as e:
        raise RatingException (e, sys)