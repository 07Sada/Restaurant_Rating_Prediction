from restr_rating.logger import logging
from restr_rating.exception import RatingException
from datetime import datetime 
import os, sys 
import pandas as pd 
import numpy as np 
from restr_rating import utils
from restr_rating.entity import config_entity
from restr_rating.entity import artifact_entity
import yaml
from typing import Optional,List
from restr_rating.config import EXTRA_COLUMNS
from scipy.stats import ks_2samp
extra_columns=EXTRA_COLUMNS
class DataValidation:
    def __init__(self,
                data_validation_config:config_entity.DataValidationConfig,
                data_ingestion_artifact:artifact_entity.DataIngestionArtifact):

        try:
            logging.info(f"{'>'*30} Data Validation Initiated {'<'*30}")
            self.data_validation_config = data_validation_config
            self.data_ingestion_artifact = data_ingestion_artifact
            self.validation_error =dict() # this line of code creates an empty dictonary

        except Exception as e:
            raise RatingException(e, sys)

    def drop_missing_values(self, df:pd.DataFrame, report_key_name:str)->Optional[pd.DataFrame]:
        """
        This function will drop columns which contains missing value more than specified threshold
        
        df : pandas dataframe
        threshold : percentage criteria to drop columns
        ===========================================================================================
        return pandas dataframe if atleast a single column is available after missing column drop else None
        """
        try:
            threshold = self.data_validation_config.missing_threshold
            null_report = df.isna().sum()/df.shape[0]
            # selecting columns which contains the null 
            logging.info(f"Selecting the column name which contain null above the threshold of: {threshold}")
            drop_column_name = null_report[null_report>threshold].index

            logging.info(f"Columns to drop: {list(drop_column_name)}")
            self.validation_error[report_key_name]=list(drop_column_name)

            df.drop(list(drop_column_name), axis=1, inplace=True)
            logging.info(f"columns are droped")
            
            # return none if no column left
            if len(df.columns) == 0:
                return None
            return df 
        
        except Exception as e:
            raise RatingException(e, sys)

    def remove_extra_columns(self, df:pd.DataFrame, extra_column_name:List)->pd.DataFrame:

        try:
            logging.info(f"column available in dataset: {df.columns}")
            for i in extra_column_name:
                if i in df.columns:
                    df.drop(i,axis=1,inplace=True)
            return df
        
        except Exception as e:
            raise RatingException(e, sys)
        
    def remove_duplicate_rows(self, df:pd.DataFrame, report_key_name:str)->pd.DataFrame:
        try:
            logging.info(f"Calculating the duplicate rows in df")
            total_duplicate_rows = df.duplicated().sum()
            self.validation_error[report_key_name]=total_duplicate_rows

            logging.info(f"Dropping duplicate rows")
            df.drop_duplicates(inplace=True)
            
            return df 
        
        except Exception as e:
            raise RatingException(e, sys)

    def removig_rows_with_missing_values(self, df:pd.DataFrame,report_key_name:str)->pd.DataFrame:
        try:
            logging.info(f'Calculating the rows with missing columns')
            missing_dict = (df.isna().sum()/df.shape[0]).to_dict()
            filtered_dict = {k: v for k, v in missing_dict.items() if v > 0}

            logging.info(f"dropping rows with null values")
            df.dropna(how='any', inplace=True)

            self.validation_error[report_key_name]=filtered_dict
            return df
        
        except Exception as e:
            raise RatingException(e, sys)

    def is_required_column_exist(self, base_df:pd.DataFrame, current_df:pd.DataFrame, report_key_name:str)->bool:

        try:
            # get the column name in base and current dataframe 
            base_columns = base_df.columns
            current_columns = current_df.columns
            
            # initializing an empty list to store missing columns 
            missing_columns = []

            # iterate through the column in database columns
            for base_column in base_columns:
                if base_column not in current_columns:
                    logging.info(f"Column {base_column} in not available in current dataset")
                    missing_columns.append(base_column)
            
            # check if there are any missing columns 
            if len(missing_columns) > 0:
                # add the missing columns to validation_error dictonary
                self.validation_error[report_key_name]=missing_columns
                # return False to indicate the missing columns 
                return False
            # return True to indicate there are no missing columns
            return True
        except Exception as e:
            raise RatingException(e, sys)

    def data_drift(self, base_df:pd.DataFrame, current_df:pd.DataFrame, report_key_name:str):

        try:
            # initializing an empty dictonary to store the data drift 
            drift_report = dict()

            base_columns = base_df.columns
            current_columns = current_df.columns

            for base_column in base_columns:

                # get the data for the corresponding column in which current dataframe
                base_data, current_data = base_df[base_column], current_df[base_column]

                # Null hypothesis is that both columns are drawn from same distribution 
                same_distribution = ks_2samp(data1=base_data, data2=current_data)

                logging.info(f"Checking null hypothesis")
                # checking if null hypothesis is rejected or accepted 
                if same_distribution.pvalue > 0.05:
                    # we are accepting the null hypothesis
                    drift_report[base_column]={
                        "pvalue":float(same_distribution.pvalue),
                        "same_distribution":True
                    }
                
                else:
                    drift_report[base_column]={
                        "pvalue":float(same_distribution.pvalue),
                        "Same_distribution":False
                    }
            # add the drift report to validation error dictonary 
            self.validation_error[report_key_name]=drift_report
        
        except Exception as e: 
            raise RatingException(e, sys)

    def initiate_data_validation(self)->artifact_entity.DataValidationArtifact:
        try:
            logging.info(f"Reading the dataframe")
            base_df = pd.read_csv(self.data_validation_config.base_file_path)

            logging.info(f"Reading the train dataframe")
            train_df = pd.read_csv(self.data_ingestion_artifact.train_file_path)

            logging.info(f"Reading the test dataframe")
            test_df = pd.read_csv(self.data_ingestion_artifact.test_file_path)

            logging.info(f"Dropping columns having null values more than threshold")
            logging.info(f"Droping null values columns from train dataframe")
            train_df = self.drop_missing_values(df=train_df, report_key_name="missing_values_within_train_dataset")
            logging.info(f"Droping null values columns from test dataframe")
            test_df = self.drop_missing_values(df=test_df, report_key_name="missing_values_within_test_dataset")

            logging.info(f"removing extra columns from train dataframe")
            train_df = self.remove_extra_columns(df=train_df, extra_column_name=EXTRA_COLUMNS)
            logging.info(f"removing extra columns from test dataframe")
            test_df = self.remove_extra_columns(df=test_df, extra_column_name=EXTRA_COLUMNS)

            logging.info(f"removing duplicate rows from train dataframe")
            train_df = self.remove_duplicate_rows(df=train_df, report_key_name="duplicate_rows_within_train_dataframe")
            logging.info(f"removing duplicate rows from test dataframe")
            test_df = self.remove_duplicate_rows(df=test_df, report_key_name="duplicate_rows_within_test_dataframe")

            logging.info(f"removing rows with null values in train dataframe")
            train_df = self.removig_rows_with_missing_values(df=train_df, report_key_name="rows_with_missing_values_within_train_dataframe")
            logging.info(f"removing rows with null values in test dataframe")
            test_df = self.removig_rows_with_missing_values(df=test_df, report_key_name="rows_with_missing_values_within_test_dataframe")

            logging.info(f"Checking if all required columns are present in train df")
            train_df_column_stats = self.is_required_column_exist(base_df=base_df, current_df=train_df, report_key_name="missing_columns_within_train_dataset")
            logging.info(f"Checking if all required columns are present in test df")
            test_df_column_stats = self.is_required_column_exist(base_df=base_df, current_df=test_df, report_key_name="missing_columns_within_test_dataset")
            
            # writing yaml report 
            logging.info(f"Writing report in yaml format")
            utils.write_yaml_file(file_path=self.data_validation_config.report_file_path, data=self.validation_error)

            data_validation_artifact = artifact_entity.DataValidationArtifact(report_file_path=self.data_validation_config.report_file_path)

            return data_validation_artifact
            logging.info(f"Validation report created")

        except Exception as e:
            raise RatingException(e, sys)