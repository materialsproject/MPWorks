from fireworks.features.dupefinder import DupeFinderBase

__author__ = 'Anubhav Jain'
__copyright__ = 'Copyright 2013, The Materials Project'
__version__ = '0.1'
__maintainer__ = 'Anubhav Jain'
__email__ = 'ajain@lbl.gov'
__date__ = 'Mar 22, 2013'

# TODO: add Launchpad indices that make this query efficient

class DupeFinderVasp(DupeFinderBase):
    """
    TODO: add docs
    """

    _fw_name = 'Dupe Finder Vasp'

    def verify(self, spec1, spec2):
        # assert: task_type and snlgroup_id have already been checked through query
        return set(spec1['run_tags']) == set(spec2['run_tags'])

    def query(self, spec):
        return {'spec.task_type': spec['task_type'],
                'spec.snlgroupSG_id': spec['snlgroupSG_id']}