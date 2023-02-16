import pymongo
import pandas as pd 
import numpy as np 
import json 
from dataclasses import dataclass 
import os 
from dotenv import load_dotenv

# load environment variable from a .env file
load_dotenv()

@dataclass
class EnvironmentVariable:
    mongodb_url:str = os.getenv("MONGO_DB_URL")

env_var = EnvironmentVariable()

mongo_client = pymongo.MongoClient(env_var.mongodb_url)

EXTRA_COLUMNS = ['url','phone','dish_liked']

TARGET_COLUMN ="rate"

ENCODE_EXCLUDE_COLUMN = ['rate', 'cost', 'votes']
