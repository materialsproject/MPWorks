import time
from fireworks.core.firework import FireTaskBase, FWAction, Firework, Workflow, Tracker
from fireworks.utilities.fw_serializers import FWSerializable
from fireworks.utilities.fw_utilities import get_slug
from mpworks.dupefinders.dupefinder_vasp import DupeFinderVasp, DupeFinderDB
from mpworks.firetasks.custodian_task import get_custodian_task
from mpworks.firetasks.vasp_io_tasks import VaspCopyTask, VaspToDBTask
from mpworks.firetasks.vasp_setup_tasks import SetupStaticRunTask, \
    SetupNonSCFTask
from mpworks.workflows.wf_settings import QA_VASP, QA_DB, QA_VASP_SMALL
from pymatgen import Composition
from pymatgen.matproj.snl import StructureNL

__author__ = 'Anubhav Jain'
__copyright__ = 'Copyright 2013, The Materials Project'
__version__ = '0.1'
__maintainer__ = 'Anubhav Jain'
__email__ = 'ajain@lbl.gov'
__date__ = 'May 01, 2013'


class AddEStructureTask(FireTaskBase, FWSerializable):
    _fw_name = "Add Electronic Structure Task v2"

    def __init__(self, parameters=None):
        """

        :param parameters:
        """
        parameters = parameters if parameters else {}
        self.update(parameters)  # store the parameters explicitly set by the user
        self.gap_cutoff = parameters.get('gap_cutoff', 0.5)  # see e-mail from Geoffroy, 5/1/2013
        self.metal_cutoff = parameters.get('metal_cutoff', 0.05)

    def run_task(self, fw_spec):
        print 'sleeping 10s for Mongo'
        time.sleep(10)
        print 'done sleeping'
        print 'the gap is {}, the cutoff is {}'.format(fw_spec['analysis']['bandgap'], self.gap_cutoff)
        if fw_spec['analysis']['bandgap'] >= self.gap_cutoff:
            static_dens = 90
            uniform_dens = 1000
            line_dens = 20
        else:
            static_dens = 450
            uniform_dens = 1500
            line_dens = 30

        if fw_spec['analysis']['bandgap'] <= self.metal_cutoff:
            user_incar_settings = {"ISMEAR": 1, "SIGMA": 0.2}
        else:
            user_incar_settings = {}

        print 'Adding more runs...'

        type_name = 'GGA+U' if 'GGA+U' in fw_spec['prev_task_type'] else 'GGA'

        snl = fw_spec['mpsnl']
        f = Composition(snl.structure.composition.reduced_formula).alphabetical_formula

        fws = []
        connections = {}

        priority = fw_spec['_priority']
        trackers = [Tracker('FW_job.out'), Tracker('FW_job.error'), Tracker('vasp.out'), Tracker('OUTCAR'), Tracker('OSZICAR')]
        trackers_db = [Tracker('FW_job.out'), Tracker('FW_job.error')]

        # run GGA static
        spec = fw_spec  # pass all the items from the current spec to the new
        spec.update({'task_type': '{} static v2'.format(type_name), '_queueadapter': QA_VASP_SMALL,
                     '_dupefinder': DupeFinderVasp().to_dict(), '_priority': priority, '_trackers': trackers})
        fws.append(
            Firework(
                [VaspCopyTask({'use_CONTCAR': True, 'skip_CHGCAR': True}), SetupStaticRunTask({"kpoints_density": static_dens, 'user_incar_settings': user_incar_settings}),
                 get_custodian_task(spec)], spec, name=get_slug(f+'--'+spec['task_type']), fw_id=-10))

        # insert into DB - GGA static
        spec = {'task_type': 'VASP db insertion', '_queueadapter': QA_DB,
                '_allow_fizzled_parents': True, '_priority': priority*2, "_dupefinder": DupeFinderDB().to_dict(), '_trackers': trackers_db}
        fws.append(
            Firework([VaspToDBTask()], spec, name=get_slug(f+'--'+spec['task_type']), fw_id=-9))
        connections[-10] = -9

        # run GGA Uniform
        spec = {'task_type': '{} Uniform v2'.format(type_name), '_queueadapter': QA_VASP,
                '_dupefinder': DupeFinderVasp().to_dict(), '_priority': priority, '_trackers': trackers}
        fws.append(Firework(
            [VaspCopyTask({'use_CONTCAR': False}), SetupNonSCFTask({'mode': 'uniform', "kpoints_density": uniform_dens}),
             get_custodian_task(spec)], spec, name=get_slug(f+'--'+spec['task_type']), fw_id=-8))
        connections[-9] = -8

        # insert into DB - GGA Uniform
        spec = {'task_type': 'VASP db insertion', '_queueadapter': QA_DB,
                '_allow_fizzled_parents': True, '_priority': priority*2, "_dupefinder": DupeFinderDB().to_dict(), '_trackers': trackers_db}
        fws.append(
            Firework([VaspToDBTask({'parse_uniform': True})], spec, name=get_slug(f+'--'+spec['task_type']),
                     fw_id=-7))
        connections[-8] = -7

        # run GGA Band structure
        spec = {'task_type': '{} band structure v2'.format(type_name), '_queueadapter': QA_VASP,
                '_dupefinder': DupeFinderVasp().to_dict(), '_priority': priority, '_trackers': trackers}
        fws.append(Firework([VaspCopyTask({'use_CONTCAR': False}), SetupNonSCFTask({'mode': 'line', "kpoints_line_density": line_dens}),
                             get_custodian_task(spec)], spec, name=get_slug(f+'--'+spec['task_type']),
                            fw_id=-6))
        connections[-7] = [-6]

        # insert into DB - GGA Band structure
        spec = {'task_type': 'VASP db insertion', '_queueadapter': QA_DB,
                '_allow_fizzled_parents': True, '_priority': priority*2, "_dupefinder": DupeFinderDB().to_dict(), '_trackers': trackers_db}
        fws.append(Firework([VaspToDBTask({})], spec, name=get_slug(f+'--'+spec['task_type']), fw_id=-5))
        connections[-6] = -5


        if fw_spec.get('parameters') and fw_spec['parameters'].get('boltztrap'):
            # run Boltztrap
            from mpworks.firetasks.boltztrap_tasks import BoltztrapRunTask
            spec = {'task_type': '{} Boltztrap'.format(type_name), '_queueadapter': QA_DB,
                    '_dupefinder': DupeFinderDB().to_dict(), '_priority': priority}
            fws.append(Firework(
                [BoltztrapRunTask()], spec, name=get_slug(f+'--'+spec['task_type']), fw_id=-4))
            connections[-7].append(-4)

        wf = Workflow(fws, connections)

        print 'Done adding more runs...'

        return FWAction(additions=wf)


class AddEStructureTask_old(FireTaskBase, FWSerializable):

    """
    WARNING: THIS IS DEPRECATED CODE!!!
    """
    _fw_name = "Add Electronic Structure Task"

    def __init__(self, parameters=None):
        """

        :param parameters:
        """
        parameters = parameters if parameters else {}
        self.update(parameters)  # store the parameters explicitly set by the user
        self.gap_cutoff = parameters.get('gap_cutoff', 0.5)  # see e-mail from Geoffroy, 5/1/2013

    def run_task(self, fw_spec):
        print 'sleeping 10s for Mongo'
        time.sleep(10)
        print 'done sleeping'
        print 'the gap is {}, the cutoff is {}'.format(fw_spec['analysis']['bandgap'], self.gap_cutoff)

        if fw_spec['analysis']['bandgap'] >= self.gap_cutoff:
            print 'Adding more runs...'
            type_name = 'GGA+U' if 'GGA+U' in fw_spec['prev_task_type'] else 'GGA'

            snl = fw_spec['mpsnl']
            f = Composition(snl.structure.composition.reduced_formula).alphabetical_formula

            fws = []
            connections = {}

            priority = fw_spec['_priority']

            # run GGA static
            spec = fw_spec  # pass all the items from the current spec to the new
            #  one
            spec.update({'task_type': '{} static'.format(type_name), '_queueadapter': QA_VASP,
                         '_dupefinder': DupeFinderVasp().to_dict(), '_priority': priority})
            fws.append(
                Firework(
                    [VaspCopyTask({'use_CONTCAR': True}), SetupStaticRunTask(),
                     get_custodian_task(spec)], spec, name=get_slug(f+'--'+spec['task_type']), fw_id=-10))

            # insert into DB - GGA static
            spec = {'task_type': 'VASP db insertion', '_queueadapter': QA_DB,
                    '_allow_fizzled_parents': True, '_priority': priority, "_dupefinder": DupeFinderDB().to_dict()}
            fws.append(
                Firework([VaspToDBTask()], spec, name=get_slug(f+'--'+spec['task_type']), fw_id=-9))
            connections[-10] = -9

            # run GGA Uniform
            spec = {'task_type': '{} Uniform'.format(type_name), '_queueadapter': QA_VASP,
                    '_dupefinder': DupeFinderVasp().to_dict(), '_priority': priority}
            fws.append(Firework(
                [VaspCopyTask({'use_CONTCAR': False}), SetupNonSCFTask({'mode': 'uniform'}),
                 get_custodian_task(spec)], spec, name=get_slug(f+'--'+spec['task_type']), fw_id=-8))
            connections[-9] = -8

            # insert into DB - GGA Uniform
            spec = {'task_type': 'VASP db insertion', '_queueadapter': QA_DB,
                    '_allow_fizzled_parents': True, '_priority': priority, "_dupefinder": DupeFinderDB().to_dict()}
            fws.append(
                Firework([VaspToDBTask({'parse_uniform': True})], spec, name=get_slug(f+'--'+spec['task_type']),
                         fw_id=-7))
            connections[-8] = -7

            # run GGA Band structure
            spec = {'task_type': '{} band structure'.format(type_name), '_queueadapter': QA_VASP,
                    '_dupefinder': DupeFinderVasp().to_dict(), '_priority': priority}
            fws.append(Firework([VaspCopyTask({'use_CONTCAR': False}), SetupNonSCFTask({'mode': 'line'}),
                                 get_custodian_task(spec)], spec, name=get_slug(f+'--'+spec['task_type']),
                                fw_id=-6))
            connections[-7] = -6

            # insert into DB - GGA Band structure
            spec = {'task_type': 'VASP db insertion', '_queueadapter': QA_DB,
                    '_allow_fizzled_parents': True, '_priority': priority, "_dupefinder": DupeFinderDB().to_dict()}
            fws.append(Firework([VaspToDBTask({})], spec, name=get_slug(f+'--'+spec['task_type']), fw_id=-5))
            connections[-6] = -5

            wf = Workflow(fws, connections)

            print 'Done adding more runs...'

            return FWAction(additions=wf)
        return FWAction()




class DummyLegacyTask(FireTaskBase, FWSerializable):
    _fw_name = "Dummy Legacy Task"

    def run_task(self, fw_spec):
        pass
