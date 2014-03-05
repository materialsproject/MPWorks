import json
import os
from pymongo import MongoClient
from fireworks.core.launchpad import LaunchPad
from mpworks.snl_utils.snl_mongo import SNLMongoAdapter
from mpworks.submission.submission_mongo import SubmissionMongoAdapter
from pymatgen import MPRester
from pymatgen.matproj.snl import StructureNL

__author__ = 'Anubhav Jain'
__copyright__ = 'Copyright 2013, The Materials Project'
__version__ = '0.1'
__maintainer__ = 'Anubhav Jain'
__email__ = 'ajain@lbl.gov'
__date__ = 'May 06, 2013'

def clear_env():
    sma = SubmissionMongoAdapter.auto_load()

    lp = LaunchPad.auto_load()

    snl = SNLMongoAdapter.auto_load()

    db_dir = os.environ['DB_LOC']
    db_path = os.path.join(db_dir, 'tasks_db.json')
    with open(db_path) as f:
        db_creds = json.load(f)

    sma._reset()
    lp.reset('', require_password=False)
    snl._reset()

    conn = MongoClient(db_creds['host'], db_creds['port'])
    db = conn[db_creds['database']]
    db.authenticate(db_creds['admin_user'], db_creds['admin_password'])
    db.tasks.remove()
    db.counter.remove()
    db['dos_fs.chunks'].remove()
    db['dos_fs.files'].remove()
    db['band_structure_fs.files'].remove()
    db['band_structure_fs.files'].remove()


def submit_tests(names=None):
    sma = SubmissionMongoAdapter.auto_load()

    # note: TiO2 is duplicated twice purposely, duplicate check should catch this
    compounds = {"Si": 149, "Al": 134, "ZnO": 2133, "FeO": 18905,
                 "LiCoO2": 601860, "LiFePO4": 585433, "GaAs": 2534, "Ge": 32, "PbTe": 19717,
                 "YbO": 1216, "SiC": 567551, "Fe3C": 510623, "SiO2": 547211, "Na2O": 2352,
                 "InSb (unstable)": 10148, "Sb2O5": 1705, "N2O5": 554368, "BaTiO3": 5020,
                 "Rb2O": 1394, "TiO2": 554278, "TiO2 (2)": 554278, 'BaNbTePO8': 560794,
                 "AgCl": 22922, "AgCl (2)": 570858, "SiO2 (2)": 555211, "Mg2SiO4": 2895, "CO2": 20066,
                 "PbSO4": 22298, "SrTiO3": 5532, "FeAl": 2658, "AlFeCo2": 10884, "NaCoO2": 554427,
                 "ReO3": 547271, "LaH2": 24153, "SiH3I": 28538, "LiBH4": 30209, "H8S5N2": 28143,
                 "LiOH": 23856, "LiO2": 546422, "SrO2": 2697, "Mn": 35, "Hg4Pt": 2312,
                 "PdF4": 13868, "Gd2WO6": 651333, 'MnO2': 19395, 'VO2': 504800}

    mpr = MPRester()

    for name, sid in compounds.iteritems():
        if not names or name in names:
            sid = mpr.get_materials_id_from_task_id("mp-{}".format(sid))
            s = mpr.get_structure_by_material_id(sid, final=False)

            snl = StructureNL(s, 'Anubhav Jain <anubhavster@gmail.com>')

            parameters = {'priority': 10} if name == 'Si' else None
            sma.submit_snl(snl, 'anubhavster@gmail.com', parameters=parameters)


def clear_and_submit(clear=False, names=None):
    if clear:
        clear_env()
    submit_tests(names=names)