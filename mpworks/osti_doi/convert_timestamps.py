from dateutil.parser import parse
from osti_record import OstiMongoAdapter
db_yaml = 'materials_db_prod.yaml'
matad = OstiMongoAdapter.from_config(db_yaml=db_yaml)
for doc in matad.doicoll.find():
  if doc['_id'] == 'mp-12661': continue
  new_datetime = parse(doc['created_at'])
  matad.doicoll.update(
    {'_id': doc['_id']}, {'$set': {'created_at': new_datetime}}
  )
