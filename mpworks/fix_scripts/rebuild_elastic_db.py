__author__ = 'Joseph Montoya'
__copyright__ = 'Copyright 2016, The Materials Project'
__version__ = '0.1'
__maintainer__ = 'Joseph Montoya'
__email__ = 'montoyjh@lbl.gov'
__date__ = 'March 8, 2016'

import json
import os
import sys
from pymongo import MongoClient
from pymatgen.matproj.rest import MPRester
from pymatgen.core import Structure
from pymatgen.analysis.structure_matcher import StructureMatcher, ElementComparator
from multiprocessing import Pool
import numpy as np
from tabulate import tabulate
from pymatgen.symmetry.analyzer import SpacegroupAnalyzer
import argparse

mpr = MPRester()

db_path = os.path.join(os.environ['DB_LOC'], 'tasks_db.json')
with open(db_path) as f:
    db_creds = json.load(f)

conn = MongoClient(host = db_creds['host'], port = db_creds['port'], connect=False)
db = conn[db_creds['database']]
db.authenticate(db_creds['admin_user'], 
                password = db_creds['admin_password'])

tasks = db[db_creds['collection']]
elasticity = db['elasticity']
global wc_elasticity
wc_elasticity = db['wc_elasticity']

path_to_wc_json = 'ec_web_03092016.json'
with open(path_to_wc_json) as f:
    wc_data = json.load(f)

def find_structures(structure, sg, ltol=0.1, stol=0.1, angle_tol = 3):
    """
    Needed custom method to find structures from MPRester, mostly a copy
    of materials_django.rest.views.find_structure with mutable arguments
    to StructureMatcher
    """
    m = StructureMatcher(ltol=ltol, stol=stol, angle_tol = angle_tol, scale=True,
                         primitive_cell=True, attempt_supercell=False, 
                         comparator=ElementComparator())
    crit = {"reduced_cell_formula": structure.composition.to_reduced_dict,
            "spacegroup.number": sg}
    matches = [r['task_id'] for r in mpr.query(crit, properties=['structure', 'task_id'])
               if m.fit(structure, r['structure'])]
    if len(matches) > 0:
        rmses = [m.get_rms_dist(structure, mpr.get_structures(s)[0]) for s in matches]
        matches.sort(key = dict(zip(matches,rmses)).get)
    else:
        rmses = []
    return matches, rmses


def verify(entry, ltol = 0.2, stol = 0.3, angle_tol = 5, verbose = False):
    """
    Attempts to verify that a given material_id in an elasticity document
    corresponds to the correct structure in the online materials collection
    """
    try:
        this = mpr.query({'task_id':entry}, {'structure':1, 'elasticity':1})
        web_struct = this[0]['structure']

        wc_db_entries = wc_elasticity.find({"material_id":entry, 
                                            "homogeneous_poisson":this[0]['elasticity']['homogeneous_poisson']}).sort([('kpoint_density',-1)])
        doc = wc_db_entries[0]
        db_struct = Structure.from_dict(doc['snl'])
        m = StructureMatcher(ltol=ltol, stol=stol, angle_tol = angle_tol, scale=True,
                             primitive_cell=True, attempt_supercell=False, 
                             comparator=ElementComparator())
        struct_match = m.fit(web_struct, db_struct)
        # tol = 1e-7
        # elast_match = (doc['homogeneous_poisson'] - this[0]['elasticity']['homogeneous_poisson'] < tol)

        if not m.fit(web_struct, db_struct):
            sga = SpacegroupAnalyzer(web_struct)
            web_conv = sga.get_conventional_standard_structure()
            if m.fit(web_conv, db_struct):
                elasticity.update_one({'_id':doc['_id']}, {'$set':doc}, upsert = True)
                return True
            print 'Unverified: {} does not structure match'.format(entry)
            return False
        else:
            elasticity.update_one({'_id':doc['_id']}, {'$set':doc}, upsert = True)
            return True
    except Exception as e:
        print 'Unverified: {} returns error {}'.format(entry, e)
        return False


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('-unverified', action='store_true',
                        help = 'flag to limit materials to previously unverified materials')
    args = parser.parse_args()
    if os.path.isfile('unverified_elastic_ids.json') and args.unverified:
        with open('unverified_elastic_ids.json') as f:
            matlist = json.load(f)
    else:
        matlist = wc_data.keys()
    # sys.exit(1)
    # unverified = []
    # TODO: verification framework
    if not verify(wc_data.keys()[0]):
        sys.exit(0)
    p = Pool(16)
    result = p.map(verify, matlist)
    unverified = np.array(matlist)[np.where(np.logical_not(np.array(result)))]
    print 'Unverified total, {}:{}'.format(len(unverified), ', '.join(unverified))
    with open('unverified_elastic_ids.json','w') as f:
        json.dump(unverified.tolist(), f)
