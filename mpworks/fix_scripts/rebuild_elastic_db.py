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

def verify(entry, ltol = 0.5, stol = 0.5, angle_tol = 5, verbose = False):
    """
    Attempts to verify that a given material_id in an elasticity document
    corresponds to the correct structure in the online materials collection
    """
    try:
        wc_db_entry = wc_elasticity.find_one({"material_id":entry}, {'snl':1, 'spacegroup':1})
        struct = Structure.from_dict(wc_db_entry['snl'])
        spacegroup = wc_db_entry['spacegroup']['number']
        s_list, rmses = find_structures(struct, spacegroup, ltol=ltol,
                                        stol=stol, angle_tol=angle_tol)
        '''
        if len(s_list) > 1:
            verify(entry, ltol = 0.9*ltol, stol = 0.25*stol, angle_tol=0.9*angle_tol)
        '''
        if len(s_list) == 0:
            verify(entry, ltol = 1.5*ltol, stol = 2.5*stol,
                   angle_tol = 2.5*angle_tol)
        if verbose:
            print "Matching {}".format(entry)
            print rmses[0][0]
            rms1, rms2 = zip(*rmses)
            print tabulate({"matches":s_list, "RMS_dist":rms1}, headers = 'keys')
        if s_list[0] == entry:
            print "Verified {}".format(entry)
            return True
        else:
            print "Unverified {}".format(entry)
            return False
    except Exception as e:
        print "Unverified {}".format(entry)
        return False

reassigned_ids = {"mp-12552":"mp-2593",
                  "whoknows":"ifthemoonsaballoon"}

if __name__ == "__main__": 
    # sys.exit(1)
    # unverified = []
    # TODO: verification framework
    # verify(wc_data.keys()[0])
    '''
    p = Pool(16)
    result = p.map(verify, wc_data.keys())
    unverified = np.array(wc_data.keys())[np.where(np.logical_not(np.array(result)))]
    print 'Unverified total, {}:{}'.format(len(unverified), ', '.join(unverified))
    with open('unverified_elastic_ids.json','w') as f:
        json.dump(unverified.tolist(), f)
    '''
    for mpid in wc_data.keys():
        doc = wc_elasticity.find({'material_id' : mpid, 'G_Voigt_Reuss_Hill': wc_data[mpid]['G_VRH']}).sort

        
