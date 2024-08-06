from pypdf import PdfWriter
import boto3
import json
from io import BytesIO
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
import datetime

uri = "mongodb+srv://bhoomikaethakota:Bhoomika123@bhoomika.exypscf.mongodb.net/?retryWrites=true&w=majority&appName=bhoomika"

   # Create a new client and connect to the server
mongo_client = MongoClient(uri, server_api=ServerApi('1'))


def lambda_handler(event, context):
    # reader = pypdf.PdfReader()
    
    # saving the datetime using datetime in built function
    started = datetime.datetime.now()
    # getting the bucketname and the key(filename) and then getting the file from the bucket using getobject 
    bucketname = event["Records"][0]["s3"]["bucket"]["name"]
    key = event["Records"][0]["s3"]["object"]["key"]
    size = event["Records"][0]["s3"]["object"]["size"] // 1024
    s3_client = boto3.client('s3')
    file = s3_client.get_object(Key=key, Bucket = bucketname)
    # DATA PART
    # creating a database named bhoomikaethakota and a collection named pdfs and inserting records in it using insert one method in 
    # which each record has name of the file and the completed field which is boolean(we dont have the file yet)
    db = mongo_client.get_database("bhoomikaethakota")
    collection = db.get_collection("Pdfs")
    collection.insert_one({"name": key, "completed": False, "initialSize": size})

    # FILE PART
    # converting the file into byte file and creating a writer object to store the byte file

    bytefile = BytesIO(file["Body"].read())
    writer = PdfWriter(clone_from= bytefile)

    # for each page we are compressing using the pypdf library functions
    for page in writer.pages:
        for img in page.images:
            img.replace(img.image, quality=50)
        page.compress_content_streams() 

   
    
    # writing the new compressed file 
    compresses_file = BytesIO()
    writer.write(compresses_file)
    compresses_file = compresses_file.getvalue()
    # getting end time
    ended = datetime.datetime.now()
    # calculateing time taken to compress
    elapsed = ended-started

    # uploading pdf to s3 
    s3_client.put_object(Key= key, Bucket= "compressed-outputs",Body=compresses_file)
    data = s3_client.head_object(Key= key, Bucket= "compressed-outputs")
    final_size = data["ContentLength"]
    # for downlaod url to get preassigned url for that compressed file using get object and the parameters 
    try:
        downloadURL = s3_client.generate_presigned_url('get_object', Params = {
            'Bucket': 'compressed-outputs',
            'Key' : key
        })
    except :
        print("URL generation failed")
    # DATA PART
    # updating the compressed file url , time, completed field to the name we have created earlier 
    collection.update_one({"name": key}, {'$set': {"url": downloadURL, "elapsed": elapsed.total_seconds(), "completed": True, "finalSize": int(final_size) // 1024}})
    return {
        "statusCode" :200,
        "body" : json.dumps({"message": "compressed"})
    }








