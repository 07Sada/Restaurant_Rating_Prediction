import os 
from restr_rating.logger import logging
from restr_rating.exception import RatingException
from restr_rating.entity.config_entity import TRANSFORMER_OBJECT_FILE_NAME, MODEL_FILE_NAME
from glob import glob
from typing import Optional
import sys 

class ModelResolver:
    def __init__(self, model_registry:str ='saved_models',
                transformer_dir_name = 'transformer',
                model_dir_name ='model'):

        self.model_registry = model_registry
        os.makedirs(self.model_registry, exist_ok=True)
        self.transformer_dir_name = transformer_dir_name
        self.model_dir_name = model_dir_name

    def get_latest_dir_path(self) -> Optional[str]:
        try:
            # Get a list of all file and directory names in the model registry folder
            dir_names = os.listdir(self.model_registry)

            # If there are no directories in the folder, return None
            if len(dir_names) == 0:
                return None
            
            # Convert the directory names to integers, assuming they are all valid
            dir_names = list(map(int, dir_names))

            # Find the largest integer in the list, which corresponds to the latest directory created
            latest_dir_name = max(dir_names)

            # Return the path to the latest directory
            return os.path.join(self.model_registry, f"{latest_dir_name}")
        except Exception as e:
                raise RatingException(e, sys)
    
    def get_latest_model_path(self):
        try:
            # Get the path to the latest directory
            latest_dir = self.get_latest_dir_path()

            # If no directories were found, raise an exception
            if latest_dir is None:
                raise Exception(f"Model is not available")

            # Construct the path to the model file
            return os.path.join(latest_dir, self.model_dir_name, self.MODEL_FILE_NAME)
        
        # If an exception occurs, raise it
        except Exception as e:
            raise e 

    def get_latest_transformer_path(self):
        try:
            latest_dir = self.get_latest_dir_path()
            if latest_dir is None:
                raise Exception(f"Transformer is not available")
            return os.path.join(latest_dir, self.transformer_dir_name, TRANSFORMER_OBJECT_FILE_NAME)
        except Exception as e:
            raise e 
    
    def get_latest_save_dir_path(self)->str:
        try:
            latest_dir = self.get_latest_dir_path()
            if latest_dir ==None:
                return os.path.join(self.model_registry,f"{0}")
            latest_dir_num = int(os.path.basename(self.get_latest_dir_path()))
            return os.path.join(self.model_registry,f"{latest_dir_num+1}")
        except Exception as e:
            raise e
            
    def get_latest_save_model_path(self):
        try:
            latest_dir = self.get_latest_save_dir_path()
            return os.path.join(latest_dir,self.model_dir_name,MODEL_FILE_NAME)
        except Exception as e:
            raise e

    def get_latest_save_transformer_path(self):
        try:
            latest_dir = self.get_latest_save_dir_path()
            return os.path.join(latest_dir,self.transformer_dir_name,TRANSFORMER_OBJECT_FILE_NAME)
        except Exception as e:
            raise e