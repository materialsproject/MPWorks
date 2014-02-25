from fireworks import FireTaskBase
import json
import os
from pymongo import MongoClient
from fireworks.core.firework import FWAction
from fireworks.utilities.fw_serializers import FWSerializable
from monty.os.path import zpath
from mpworks.workflows.wf_utils import get_loc, get_block_part
from pymatgen.electronic_structure.bandstructure import BandStructure
from pymatgen.electronic_structure.boltztrap import BoltztrapRunner, BoltztrapAnalyzer
from pymatgen.io.vaspio.vasp_output import Vasprun
from pymatgen.util.io_utils import clean_json

__author__ = 'Geoffroy Hautier, Anubhav Jain'
__copyright__ = 'Copyright 2014, The Materials Project'
__version__ = '0.1'
__maintainer__ = 'Anubhav Jain'
__email__ = 'ajain@lbl.gov'
__date__ = 'Feb 24, 2014'


class BoltztrapRunTask(FireTaskBase, FWSerializable):
    _fw_name = "Boltztrap Run Task"

    def run_task(self, fw_spec):

        # get the band structure and nelect from files
        """
        prev_dir = get_loc(fw_spec['prev_vasp_dir'])
        vasprun_loc = zpath(os.path.join(prev_dir, 'vasprun.xml'))
        kpoints_loc = zpath(os.path.join(prev_dir, 'KPOINTS'))

        vr = Vasprun(vasprun_loc)
        bs = vr.get_band_structure(kpoints_filename=kpoints_loc)
        """

        # get the band structure and nelect from DB
        block_part = get_block_part(fw_spec['prev_vasp_dir'])

        db_dir = os.environ['DB_LOC']
        assert isinstance(db_dir, object)
        db_path = os.path.join(db_dir, 'tasks_db.json')
        with open(db_path) as f:
            creds = json.load(f)
            connection = MongoClient(creds['host'], creds['port'])
            tdb = connection[creds['db']]
            tdb.authenticate(creds['username'], creds['password'])

            m_task = tdb.tasks.find_one({"dir_name": db_dir})
            nelect = m_task['calculations'][0]['input']['parameters']['NELECT']
            bs_id = m_task['calculations'][0]['band_structure_fs_id']
            bs=BandStructure.from_dict(tdb.electronic_structure.find_one({'_id':bs_id}))

            print nelect
            print 'Band Structure found:', bool(bs)
            # run Boltztrap
            runner = BoltztrapRunner(bs, nelect)
            dir = runner.run(path_dir=os.getcwd())

            # put the data in the database
            bta = BoltztrapAnalyzer.from_files(dir)
            data = bta.to_dict
            data['composition'] = bs.composition.to_dict
            data['composition_reduced'] = bs.composition.reduced_composition.to_dict
            data['elements'] = bs._structure.composition.elements
            data['num_elements'] = len(bs._structure.composition.elements)
            data['snlgroup_id'] = fw_spec['snlgroup_id']
            data['run_tags'] = fw_spec['run_tags']
            data['snl'] = fw_spec['mpsnl']
            data['dir_name_full'] = dir
            data['dir_name'] = get_block_part(dir)
            data['task_id'] = m_task['task_id']
            data['hall'] = {}
            data['hall_doping'] = {}
            tdb.boltztrap.insert(clean_json(data))

        update_spec = {'prev_vasp_dir': fw_spec['prev_vasp_dir'],
                       'boltztrap_dir': os.getcwd(),
                       'prev_task_type': fw_spec['task_type'],
                       'mpsnl': fw_spec['mpsnl'],
                       'snlgroup_id': fw_spec['snlgroup_id'],
                       'run_tags': fw_spec['run_tags']}

        return FWAction(update_spec=update_spec)