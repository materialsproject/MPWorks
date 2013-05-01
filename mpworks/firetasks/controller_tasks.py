from fireworks.core.firework import FireTaskBase, FWAction, FireWork, Workflow
from fireworks.utilities.fw_serializers import FWSerializable
from mpworks.dupefinders.dupefinder_vasp import DupeFinderVasp
from mpworks.firetasks.vasp_io_tasks import VaspCopyTask, VaspToDBTask
from mpworks.firetasks.vasp_setup_tasks import SetupStaticRunTask, SetupNonSCFTask
from pymatgen.matproj.snl import StructureNL

__author__ = 'Anubhav Jain'
__copyright__ = 'Copyright 2013, The Materials Project'
__version__ = '0.1'
__maintainer__ = 'Anubhav Jain'
__email__ = 'ajain@lbl.gov'
__date__ = 'May 01, 2013'


class AddEStructureTask(FireTaskBase, FWSerializable):

    _fw_name = "Add Electronic Structure Task"

    def run_task(self, fw_spec):
        from mpworks.workflows.snl_to_wf import _get_metadata, _get_custodian_task
        # TODO: only add the workflow if the gap is > 1.0 eV
        # TODO: add stored data?
        # TODO: switch between GGA and GGA+U tags depending on prev task type

        snl = StructureNL.from_dict(fw_spec['mpsnl'])

        fws = []
        connections = {}

        # run GGA static
        spec = fw_spec  # pass all the items from the current spec to the new one
        spec.update({'task_type': 'GGA static', '_dupefinder': DupeFinderVasp().to_dict()})
        spec.update(_get_metadata(snl))
        fws.append(
            FireWork([VaspCopyTask({'extension': '.relax2'}), SetupStaticRunTask(), _get_custodian_task(spec)], spec, fw_id=-10))

        # insert into DB - GGA static
        spec = {'task_type': 'VASP db insertion', '_allow_fizzled_parents': True}
        spec.update(_get_metadata(snl))
        fws.append(
            FireWork([VaspToDBTask()], spec, fw_id=-9))
        connections[-10] = -9

        # run GGA Uniform
        spec = {'task_type': 'GGA Uniform', '_dupefinder': DupeFinderVasp().to_dict()}
        spec.update(_get_metadata(snl))
        fws.append(FireWork([VaspCopyTask(), SetupNonSCFTask({'mode': 'uniform'}), _get_custodian_task(spec)], spec, fw_id=-8))
        connections[-9] = -8

        # insert into DB - GGA Uniform
        spec = {'task_type': 'VASP db insertion', '_allow_fizzled_parents': True}
        spec.update(_get_metadata(snl))
        fws.append(
            FireWork([VaspToDBTask({'parse_uniform': True})], spec, fw_id=-7))
        connections[-8] = -7

        # run GGA Band structure
        spec = {'task_type': 'GGA band structure', '_dupefinder': DupeFinderVasp().to_dict()}
        spec.update(_get_metadata(snl))
        fws.append(FireWork([VaspCopyTask(), SetupNonSCFTask({'mode': 'line'}), _get_custodian_task(spec)], spec, fw_id=-6))
        connections[-7] = -6

        # insert into DB - GGA Band structure
        spec = {'task_type': 'VASP db insertion', '_allow_fizzled_parents': True}
        spec.update(_get_metadata(snl))
        fws.append(FireWork([VaspToDBTask({})], spec, fw_id=-5))
        connections[-6] = -5

        wf = Workflow(fws, connections)

        return FWAction(additions=wf)
