



MYSQL_CONFIG = {
    'host': 'ich-db.edu.itcareerhub.de',
    'user': 'ich1',
    'password': 'password',
    'database': 'sakila',
}


from pymongo import MongoClient

MONGO_CONFIG = {
    'uri': 'mongodb://ich_editor:verystrongpassword@mongo.itcareerhub.de:27017/?readPreference=primary&ssl=false&authMechanism=DEFAULT&authSource=ich_edit',
    'db_name': 'ich_edit',
    'collection_name': 'final_project_250425-ptm_georgiy_nedelko'
}

client = MongoClient(MONGO_CONFIG['uri'])
db = client[MONGO_CONFIG['db_name']]
collection = db[MONGO_CONFIG['collection_name']]