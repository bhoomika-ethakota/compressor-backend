
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi

uri = "mongodb+srv://bhoomikaethakota:Bhoomika123@bhoomika.exypscf.mongodb.net/?retryWrites=true&w=majority&appName=bhoomika"

# Create a new client and connect to the server
client = MongoClient(uri, server_api=ServerApi('1'))

print(client)