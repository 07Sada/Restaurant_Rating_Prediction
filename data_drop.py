import pymongo

# Connect to MongoDB
client = pymongo.MongoClient("mongodb+srv://Sadashiv:s.a.d.a.SHIV@sada.ufebrno.mongodb.net/?retryWrites=true&w=majority")

# Specify the database to delete
db_name = "FitBit_Database"

# Use the drop_database method to delete the database
client.drop_database(db_name)

# Confirm that the database has been deleted
print(client.list_database_names())