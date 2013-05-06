import json
import os
from pymongo import MongoClient
from fireworks.core.fw_config import FWConfig
from fireworks.core.launchpad import LaunchPad
from mpworks.snl_utils.snl_mongo import SNLMongoAdapter
from mpworks.submissions.submissions_mongo import SubmissionMongoAdapter
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
    if 'testing' not in str(sma.db):
        raise ValueError('{} is not a testing database'.format(sma.db))

    l_dir = FWConfig().CONFIG_FILE_DIR
    l_file = os.path.join(l_dir, 'my_launchpad.yaml')
    lp = LaunchPad.from_file(l_file)
    if 'testing' not in str(lp.db):
        raise ValueError('{} is not a testing database'.format(sma.db))

    snl = SNLMongoAdapter.auto_load()
    if 'testing' not in str(snl.db):
        raise ValueError('{} is not a testing database'.format(sma.db))

    db_dir = os.environ['DB_LOC']
    db_path = os.path.join(db_dir, 'tasks_db.json')
    with open(db_path) as f:
        db_creds = json.load(f)
        if 'testing' not in db_creds['database']:
            raise ValueError('{} is not a testing database'.format(db_creds['database']))


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


def submit_tests():
    sma = SubmissionMongoAdapter.auto_load()
    if 'testing' not in str(sma.db):
        raise ValueError('{} is not a testing database'.format(sma.db))

    compounds={"Si": 149, "Al": 134}

    sids = compounds.values()
    mpr = MPRester(api_key="flebb3pU1yfExlOc", host="www.materialsproject.org")

    for sid in sids:
        s = mpr.get_structure_by_material_id(sid)

        snl = StructureNL(s, 'Anubhav Jain <anubhavster@gmail.com>')
        sma.submit_snl(snl, 'anubhavster@gmail.com', parameters=None)


def clear_and_submit(clear=False):
    if clear:
        clear_env()
    submit_tests()