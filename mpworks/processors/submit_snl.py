from mpworks.snl_utils.mpsnl import MPStructureNL
from mpworks.snl_utils.snl_mongo import SNLMongoAdapter
from mpworks.submission.submission_mongo import SubmissionMongoAdapter
from mpworks.workflows.wf_utils import NO_POTCARS
from pymatgen.matproj.snl import StructureNL

__author__ = 'Anubhav Jain'
__copyright__ = 'Copyright 2013, The Materials Project'
__version__ = '0.1'
__maintainer__ = 'Anubhav Jain'
__email__ = 'ajain@lbl.gov'
__date__ = 'May 14, 2013'


def submit_all_snl(min=None, max=None):
    constraints = {'is_ordered': True, 'is_valid': True, 'nsites': {'$lte': 200}, 'canonical_snl.about.projects': {'$ne': 'CederDahn Challenge'}}
    constraints['elements'] = {'$nin': NO_POTCARS}

    if min and max:
        constraints['snlgroup_id'] = {'$gte': min, '$lte': max}
    elif min or max:
        raise ValueError('Must specify both min AND max if you specify one')

    snldb = SNLMongoAdapter.auto_load()
    sma = SubmissionMongoAdapter.auto_load()

    for result in snldb.snlgroups.find(constraints, {'canonical_snl': 1, 'snlgroup_id': 1}):
        snl = MPStructureNL.from_dict(result['canonical_snl'])
        parameters = {'snlgroup_id': result['snlgroup_id']}
        sma.submit_snl(snl, 'Anubhav Jain <ajain@lbl.gov>', parameters=parameters)

if __name__ == '__main__':
    submit_all_snl(0, 0)