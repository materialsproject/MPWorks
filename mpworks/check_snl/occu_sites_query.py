import csv
from mpworks.snl_utils.snl_mongo import SNLMongoAdapter
sma = SNLMongoAdapter.auto_load()
with open('mpworks/check_snl/results/zero_occu_sites.csv', 'wb') as f:
    writer = csv.writer(f)
    writer.writerow([
        'snl_id', 'num_zero_occu_sites', 'icsd_id', 'is_valid'
    ])
    for doc in sma.snl.aggregate([
        #{ '$match': { 'about._icsd.icsd_id': { '$exists': True } } },
        { '$unwind': '$sites' },
        { '$unwind': '$sites.species' },
        { '$project': {
            'snl_id': 1, 'sites.species.occu': 1, '_id': 0, 
            'about._icsd.icsd_id': 1, 'is_valid': 1
        } },
        { '$match': { 'sites.species.occu': 0.0 } },
        { '$group': {
            '_id': '$snl_id',
            'num_zero_occu_sites': { '$sum': 1 },
            'icsd_ids': { '$addToSet': '$about._icsd.icsd_id' },
            'is_valid': { '$addToSet': '$is_valid' },
        } },
    ], cursor={}):
        icsd_id = doc['icsd_ids'][0] if len(doc['icsd_ids']) > 0 else ''
        row = [
            doc['_id'], doc['num_zero_occu_sites'], icsd_id, doc['is_valid'][0]
        ]
        writer.writerow(row)

