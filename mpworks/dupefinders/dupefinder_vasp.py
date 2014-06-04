from fireworks.features.dupefinder import DupeFinderBase

__author__ = 'Anubhav Jain'
__copyright__ = 'Copyright 2013, The Materials Project'
__version__ = '0.1'
__maintainer__ = 'Anubhav Jain'
__email__ = 'ajain@lbl.gov'
__date__ = 'Mar 22, 2013'


class DupeFinderVasp(DupeFinderBase):
    """
    TODO: add docs
    """

    _fw_name = 'Dupe Finder Vasp'

    def verify(self, spec1, spec2):
        # assert: task_type and snlgroup_id have already been checked through query
        return set(spec1.get('run_tags', [])) == set(spec2.get('run_tags', []))

    def query(self, spec):
        return {'spec.task_type': spec['task_type'],
                'spec.snlgroup_id': spec['snlgroup_id']}


class DupeFinderDB(DupeFinderBase):
    """
    TODO: add docs
    """

    _fw_name = 'Dupe Finder DB'

    def verify(self, spec1, spec2):
        # assert: task_type and prev_vasp_dir have already been checked through query
        return set(spec1.get('run_tags', [])) == set(spec2.get('run_tags', []))

    def query(self, spec):
        if 'prev_task_type' in spec and 'prev_vasp_dir' in spec and '_fizzled_parents' not in spec:
            return {'spec.task_type': spec['task_type'], 'spec.prev_task_type': spec['prev_task_type'], 'spec.prev_vasp_dir': spec['prev_vasp_dir']}
        return {'fw_id': -1}