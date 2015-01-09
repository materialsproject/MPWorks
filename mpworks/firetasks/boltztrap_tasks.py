from __future__ import division
from collections import defaultdict
import gridfs
import time
from fireworks import FireTaskBase
import json
import os
from pymongo import MongoClient
from fireworks.core.firework import FWAction
from fireworks.utilities.fw_serializers import FWSerializable
from monty.os.path import zpath
from monty.json import jsanitize, MontyEncoder
from mpworks.snl_utils.mpsnl import get_meta_from_structure
from mpworks.workflows.wf_utils import get_loc, get_block_part
from pymatgen import Structure
from pymatgen.electronic_structure.bandstructure import BandStructure
from pymatgen.electronic_structure.boltztrap import BoltztrapRunner, BoltztrapAnalyzer
from pymatgen.io.vaspio.vasp_output import Vasprun

__author__ = 'Geoffroy Hautier, Anubhav Jain'
__copyright__ = 'Copyright 2014, The Materials Project'
__version__ = '0.1'
__maintainer__ = 'Anubhav Jain'
__email__ = 'ajain@lbl.gov'
__date__ = 'Feb 24, 2014'


class BoltztrapRunTask(FireTaskBase, FWSerializable):
    _fw_name = "Boltztrap Run Task"

    def generate_te_doping(self, d):
        types = ['p', 'n']
        target = 'seebeck_doping'  # root key for getting all temps, etc

        pf_dict = defaultdict(lambda: defaultdict(int))
        zt_dict = defaultdict(lambda: defaultdict(int))

        for type in types:
            for t in d[target][type]:  # temperatures
                outside_pf_array = []
                outside_zt_array = []
                for didx, tensor in enumerate(d[target][type][t]):  # doping idx
                    inside_pf_array = []
                    inside_zt_array = []
                    for tidx, val in enumerate(tensor):
                            seebeck = d['seebeck_doping'][type][t][didx][tidx]
                            cond = d['cond_doping'][type][t][didx][tidx]
                            kappa = d['kappa_doping'][type][t][didx][tidx]
                            inside_pf_array.append(seebeck*seebeck*cond)
                            inside_zt_array.append(seebeck*seebeck*cond*t/kappa)
                    outside_pf_array.append(inside_pf_array)
                    outside_zt_array.append(inside_zt_array)

                pf_dict[type][t] = outside_pf_array
                zt_dict[type][t] = outside_zt_array

        return pf_dict, zt_dict

    def get_extreme(self, d, target, maximize=True, types=('p', 'n'), iso_cutoff=0.05):
        """

        :param d: data dictionary
        :param target: root key of target property, e.g. 'seebeck_doping'
        :param maximize: (bool) max if True, min if False
        :param types:
        :param iso_cutoff: percent cutoff for isotropicity
        :return:
        """
        max = None
        max_type = None
        max_temp = None
        max_dope = None
        max_mu = None
        isotropic = None

        for type in types:
            for t in d[target][type]:  # temperatures
                for didx, tensor in enumerate(d[target][type][t]):  # doping idx
                    for tidx, val in enumerate(tensor):
                        if (val > max and maximize) or (val < max and not maximize) or max is None:
                            max = val
                            max_type = type
                            max_temp = t
                            max_dope = d['doping'][type][didx]
                            max_mu = d['mu_doping'][type][didx]
                            st = sorted(tensor)
                            isotropic = all([st[0],st[1],st[2]]) and ((st[1]-st[0])/st[1] <= iso_cutoff) and ((st[2]-st[0])/st[2] <= iso_cutoff) and ((st[2]-st[1])/st[2] <= iso_cutoff)

        return {'value': max, 'type': max_type, 'temperature': max_temp, 'doping': max_dope, 'mu': max_mu, 'isotropic': isotropic}

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
            tdb = connection[creds['database']]
            tdb.authenticate(creds['admin_user'], creds['admin_password'])

            m_task = tdb.tasks.find_one({"dir_name": block_part}, {"calculations": 1, "task_id": 1})
            if not m_task:
                time.sleep(60)  # only thing to think of is wait for DB insertion(?)
                m_task = tdb.tasks.find_one({"dir_name": block_part}, {"calculations": 1, "task_id": 1})

            if not m_task:
                raise ValueError("Could not find task with dir_name: {}".format(block_part))

            nelect = m_task['calculations'][0]['input']['parameters']['NELECT']
            bs_id = m_task['calculations'][0]['band_structure_fs_id']
            print bs_id, type(bs_id)
            fs = gridfs.GridFS(tdb, 'band_structure_fs')
            bs_dict = json.loads(fs.get(bs_id).read())
            bs_dict['structure'] = m_task['calculations'][0]['output']['crystal']
            bs = BandStructure.from_dict(bs_dict)
            print 'Band Structure found:', bool(bs)
            print nelect

            # run Boltztrap
            runner = BoltztrapRunner(bs, nelect)
            dir = runner.run(path_dir=os.getcwd())

            # put the data in the database
            bta = BoltztrapAnalyzer.from_files(dir)
            data = bta.as_dict()
            data.update(get_meta_from_structure(bs._structure))
            data['snlgroup_id'] = fw_spec['snlgroup_id']
            data['run_tags'] = fw_spec['run_tags']
            data['snl'] = fw_spec['mpsnl']
            data['dir_name_full'] = dir
            data['dir_name'] = get_block_part(dir)
            data['task_id'] = m_task['task_id']
            del data['hall']  # remove because it is too large and not useful
            del data['hall_doping']  # remove because it is too large and not useful
            fs = gridfs.GridFS(tdb, "boltztrap_full")
            btid = fs.put(json.dumps(jsanitize(data)))

            # now for the "sanitized" data
            data['boltztrap_full_fs_id'] = btid
            # kill the super-long documents vs. mu
            to_delete = ['cond', 'seebeck', 'kappa', 'carrier_conc']
            for k in to_delete:
                del data[k]

            # add TE properties
            data['PF_doping'], data['ZT_doping'] = self.generate_te_doping(data)

            # add maximums
            data['best_seebeck'] = self.get_extreme(data, 'seebeck_doping', maximize=True)
            data['best_cond'] = self.get_extreme(data, 'cond_doping', maximize=True)
            data['best_kappa'] = self.get_extreme(data, 'kappa_doping', maximize=False)
            data['best_PF'] = self.get_extreme(data, 'PF_doping', maximize=True)
            data['best_ZT'] = self.get_extreme(data, 'ZT_doping', maximize=True)

            tdb.boltztrap.insert(jsanitize(data))

        update_spec = {'prev_vasp_dir': fw_spec['prev_vasp_dir'],
                       'boltztrap_dir': os.getcwd(),
                       'prev_task_type': fw_spec['task_type'],
                       'mpsnl': fw_spec['mpsnl'],
                       'snlgroup_id': fw_spec['snlgroup_id'],
                       'run_tags': fw_spec['run_tags'], 'parameters': fw_spec.get('parameters')}

        return FWAction(update_spec=update_spec)