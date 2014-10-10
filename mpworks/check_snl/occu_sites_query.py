from mpworks.snl_utils.snl_mongo import SNLMongoAdapter
sma = SNLMongoAdapter.auto_load()
for doc in sma.snl.aggregate([
    { '$match': { 'about._icsd.icsd_id': { '$exists': True } } },
    { '$unwind': '$sites' },
    { '$unwind': '$sites.species' },
    { '$project': {
        'snl_id': 1, 'sites.species.occu': 1, '_id': 0, 
        'about._icsd.icsd_id': 1,
    } },
    { '$match': { 'sites.species.occu': 0.0 } },
    { '$group': {
        '_id': '$snl_id',
        'num_zero_occu_sites': { '$sum': 1 },
        'icsd_ids': { '$addToSet': '$about._icsd.icsd_id' }
    } }
], cursor={}):
    print doc

