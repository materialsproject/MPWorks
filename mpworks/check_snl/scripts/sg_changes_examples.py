import os
import plotly.plotly as py
from pandas import DataFrame
from mpworks.snl_utils.snl_mongo import SNLMongoAdapter

sma = SNLMongoAdapter.auto_load()
sma2 = SNLMongoAdapter.from_file(
    os.path.join(os.environ['DB_LOC'], 'materials_db.yaml')
)


def _get_snlgroup_id(snl_id):
    return sma.snlgroups.find_one(
        {'all_snl_ids': int(snl_id)},
        {'snlgroup_id': 1, '_id': 0}
    )['snlgroup_id']

def _get_mp_id(snlgroup_id):
    mat = sma2.database.materials.find_one(
        {'snlgroup_id_final': snlgroup_id},
        {'_id': 0, 'task_id': 1}
    )
    if mat is not None:
        return mat['task_id']
    return 'not found'

def _get_mp_link(mp_id):
    if mp_id == 'not found': return mp_id
    url = 'link:$$https://materialsproject.org/materials/'
    url += mp_id
    url += '$$[%s]' % mp_id
    return url

fig = py.get_figure('tschaume',11)
df = DataFrame.from_dict(fig['data'][1]).filter(['x','y','text'])
grouped_x = df.groupby('x')
print '|==============================='
print '| old SG | close to bisectrix | far from bisectrix'
for n,g in grouped_x:
    if g.shape[0] < 2: continue # at least two entries at same old SG
    grouped_y = g.groupby('y')
    if len(grouped_y.groups) < 2: continue # at least two different entries
    g['diff'] = g['x'] - g['y']
    gs = g.sort('diff') # first entry: closest to bisectrix, last entry: farthest
    first, last = gs.iloc[0], gs.iloc[-1]
    ratios = [
        float(abs(first['diff']))/float(first['x']),
        float(abs(last['diff']))/float(last['x']) 
    ]
    if ratios[0] > 0.2 or ratios[1] < 0.8: continue
    snlgroup_ids = _get_snlgroup_id(first['text']), _get_snlgroup_id(last['text'])
    mp_ids = _get_mp_id(snlgroup_ids[0]), _get_mp_id(snlgroup_ids[1])
    print '| %d | %d (%d) -> %d -> %s | %d (%d) -> %d -> %s' % (
        first['x'],
        first['text'], first['y'], snlgroup_ids[0], _get_mp_link(mp_ids[0]),
        last['text'], last['y'], snlgroup_ids[1], _get_mp_link(mp_ids[1])
    )
print '|==============================='
