import pymongo
import pandas as pd 
import json 

# Import the mongo_client object from the restr_rating.config module
from restr_rating.config import mongo_client

# Define the path to the data file
DATA_FILE_PATH = "/config/workspace/zomato_cleaned.csv"

# Define the name of the database and collection to use
DATABASE_NAME = 'restr_ratings'
COLLECTION_NAME = 'ratings'

if __name__=="__main__":
    # Read the data file using pandas
    df = pd.read_csv(DATA_FILE_PATH)
    
    # Print the shape of the data to verify that it has been loaded correctly
    print(f"Shape of the data: {df.shape} ")

    # Reset the index of the dataframe
    df.reset_index(drop=True, inplace=True)

    # Convert the dataframe into a list of dictionaries in JSON format
    json_record = list(json.loads(df.T.to_json()).values())

    # Insert the list of dictionaries into MongoDB
    mongo_client[DATABASE_NAME][COLLECTION_NAME].insert_many(json_record)
    
    # Print a message indicating that the data has been successfully dumped to the database
    print("Records Successfully Dumbed to DB")
