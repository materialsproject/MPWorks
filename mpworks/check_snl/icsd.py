import os, csv
from builders.init_plotly import py
from mpworks.snl_utils.snl_mongo import SNLMongoAdapter
from pymatgen.analysis.structure_matcher import StructureMatcher, ElementComparator
from mpworks.snl_utils.mpsnl import MPStructureNL
sma = SNLMongoAdapter.auto_load()
matcher = StructureMatcher(
    ltol=0.2, stol=0.3, angle_tol=5, primitive_cell=True, scale=True,
    attempt_supercell=False, comparator=ElementComparator()
)

if py is not None:
    fig_id = 94
    fig = py.get_figure("https://plot.ly/~plotly.materialsproject/94")
    rows, snl_ids = [], set() 
    for category, text in zip(fig['data'][2]['y'], fig['data'][2]['text']):
        for line in text.split('<br>'):
            before_colon, after_colon = line.split(':')
            snlgroup1, snlgroup2 = map(int, before_colon[1:-1].split(','))
            snls, icsd = after_colon.split('->')
            snl1, snl2 = map(int, snls[2:-2].split(','))
            snl_ids.add(snl1)
            snl_ids.add(snl2)
            rows.append([snlgroup1, snlgroup2, snl1, snl2, int(icsd)])
    num_snl_ids = len(snl_ids)
    print '#snl_ids:', num_snl_ids
    structures = {}
    for num,mpsnl_dict in enumerate(sma.snl.find({'snl_id': {'$in': list(snl_ids)}})):
        mpsnl = MPStructureNL.from_dict(mpsnl_dict)
        structures[mpsnl.snl_id] = mpsnl.structure
        if not num % 100: print '%d/%d' % (num, num_snl_ids)
    with open('mpworks/check_snl/results/shared_icsds.csv', 'wb') as f:
        writer = csv.writer(f)
        writer.writerow([
            'snlgroup_id 1', 'snlgroup_id 2',
            'snl_id 1', 'snl_id 2', 'shared icsd_id', 'match?'
        ])
        for row in rows:
            match = matcher.fit(structures[row[2]], structures[row[3]])
            row.append(match)
            writer.writerow(row)
