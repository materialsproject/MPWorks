import os
from fireworks.core.launchpad import LaunchPad

__author__ = 'Anubhav Jain'
__copyright__ = 'Copyright 2013, The Materials Project'
__version__ = '0.1'
__maintainer__ = 'Anubhav Jain'
__email__ = 'ajain@lbl.gov'
__date__ = 'May 14, 2013'

if __name__ == '__main__':

    module_dir = os.path.dirname(os.path.abspath(__file__))
    lp_f = os.path.join(module_dir, 'my_launchpad.yaml')
    snl_f = os.path.join(module_dir, 'snl.yaml')

    with open(lp_f) as f:
        lp = LaunchPad.from_file(lp_f)
        lp.reset(None, require_password=False)




    """
    mc = MongoClient(y['host'], y['port'])
    db = mc[y['db']]

    db.authenticate(y['username'], y['password'])

    snldb = SNLMongoAdapter.from_file(snl_f)

    for icsd_dict in db.icsd_2012_crystals.find(sort=[("icsd_id", ASCENDING)], timeout=False):
        try:
            snl = icsd_dict_to_snl(icsd_dict)
            if snl:
                snldb.add_snl(snl)
        except:
            traceback.print_exc()
            print 'ERROR - icsd id:', icsd_dict['icsd_id']

    print 'DONE'
    """