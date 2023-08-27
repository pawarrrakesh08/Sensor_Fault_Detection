import pymongo

# Provide the mongodb localhost url to connect to mongodb
client  = pymongo.MongoClient("mongodb+srv://admin:admin@cluster0.qqzjykg.mongodb.net/")

#DataBase Name
dataBase  = client['Practice']

# Collection name
collection = dataBase['Products']

#sample_data

d = {
    'company_name':'Practice',
    'product':'Affordable AI',
    'courseoffered':'Maching Learning with Deployment'
}

#Insert above records in the collection
rec = collection.insert_one(d)


#Verify all the records at once in the record with all the fields
all_record = collection.find()

#Print all the records
for idx,record in enumerate(all_record):
    print(f"{idx}:{record}")