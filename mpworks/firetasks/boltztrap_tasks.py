from __future__ import division
from collections import defaultdict
import copy
import gridfs
import time
from fireworks import FireTaskBase
import json
import os
from pymongo import MongoClient
from fireworks.core.firework import FWAction
from fireworks.utilities.fw_serializers import FWSerializable
from monty.json import jsanitize, MontyEncoder
from mpworks.snl_utils.mpsnl import get_meta_from_structure
from mpworks.workflows.wf_utils import get_block_part
from mpcollab.thermoelectrics.boltztrap_TE import BoltztrapAnalyzerTE
import numpy as np
from pymatgen.electronic_structure.bandstructure import BandStructure
from pymatgen.electronic_structure.boltztrap import BoltztrapRunner, BoltztrapAnalyzer

__author__ = 'Geoffroy Hautier, Anubhav Jain'
__copyright__ = 'Copyright 2014, The Materials Project'
__version__ = '0.1'
__maintainer__ = 'Anubhav Jain'
__email__ = 'ajain@lbl.gov'
__date__ = 'Feb 24, 2014'


class BoltztrapRunTask(FireTaskBase, FWSerializable):
    _fw_name = "Boltztrap Run Task"
    TAU = 1E-14
    KAPPAL = 1

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

    def get_eigs(d, target, iso_cutoff=0.05):
        eigs_d = copy.deepcopy(d[target])
        for te_type in ('p', 'n'):
                for t in eigs_d[te_type]:  # temperatures
                    for didx, tensor in enumerate(eigs_d[te_type][t]):  # doping idx
                        eigs = np.linalg.eigh(tensor)[0].tolist()
                        st = sorted(eigs)
                        st = [round(e, 2) for e in st]
                        if [st[0],st[1],st[2]] == [0, 0, 0]:
                            isotropic = True
                        else:
                            isotropic = all([st[0],st[1],st[2]]) and (abs((st[1]-st[0])/st[1]) <= iso_cutoff) and (abs((st[2]-st[0]))/st[2] <= iso_cutoff) and (abs((st[2]-st[1])/st[2]) <= iso_cutoff)
                        eigs_d[te_type][t][didx] = {'eigs': eigs, 'isotropic': isotropic}

        return eigs_d


    def get_extreme(d, target, maximize=True, iso_cutoff=0.05):
            """

            :param d: data dictionary
            :param target: root key of target property, e.g. 'seebeck_doping'
            :param maximize: (bool) max if True, min if False
            :param iso_cutoff: percent cutoff for isotropicity
            :return:
            """
            max_val = None
            max_temp = None
            max_dope = None
            max_mu = None
            isotropic = None
            data = {}

            for te_type in ('p', 'n'):
                for t in d[target][te_type]:  # temperatures
                    for didx, evs in enumerate(d[target][te_type][t]):  # doping idx
                        for val in evs['eigs']:
                            if (val > max_val and maximize) or (val < max_val and not maximize) or max_val is None:
                                max_val = val
                                max_temp = float(t)
                                max_dope = d['doping'][te_type][didx]
                                max_mu = d['mu_doping'][te_type][t][didx]

                                isotropic = evs['isotropic']
                data[te_type] = {'value': max_val, 'temperature': max_temp, 'doping': max_dope, 'mu': max_mu, 'isotropic': isotropic}
                max_val = None

            if maximize:
                max_type = 'p' if data['p']['value'] >= data['n']['value'] else 'n'
            else:
                max_type = 'p' if data['p']['value'] <= data['n']['value'] else 'n'

            data['best'] = data[max_type]
            data['best']['type'] = max_type
            return data

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
            fs = gridfs.GridFS(tdb, "boltztrap_full")
            btid = fs.put(json.dumps(jsanitize(data)))

            # now for the "sanitized" data
            te_analyzer = BoltztrapAnalyzerTE.from_BoltztrapAnalyzer(bta)

            ted = te_analyzer.as_dict()
            del ted['seebeck']
            del ted['hall']
            del ted['kappa']
            del ted['cond']

            ted['boltztrap_full_fs_id'] = btid
            ted['snlgroup_id'] = fw_spec['snlgroup_id']
            ted['run_tags'] = fw_spec['run_tags']
            ted['snl'] = fw_spec['mpsnl']
            ted['dir_name_full'] = dir
            ted['dir_name'] = get_block_part(dir)
            ted['task_id'] = m_task['task_id']

            ted['pf_doping'] = te_analyzer.get_power_factor(tau=self.TAU).as_dict()
            ted['zt_doping'] = te_analyzer.get_ZT(kappal=self.KAPPAL, tau=self.TAU).as_dict()

            ted['pf_eigs'] = self.get_eigs(ted, 'pf_doping')
            ted['pf_best'] = self.get_extreme(ted, 'pf_eigs')
            ted['zt_eigs'] = self.get_eigs(ted, 'zt_doping')
            ted['zt_best'] = self.get_extreme(ted, 'zt_eigs')
            ted['seebeck_eigs'] = self.get_eigs(ted, 'seebeck_doping')
            ted['seebeck_best'] = self.get_extreme(ted, 'seebeck_eigs')
            ted['cond_eigs'] = self.get_eigs(ted, 'cond_doping')
            ted['cond_best'] = self.get_extreme(ted, 'cond_eigs')
            ted['kappa_eigs'] = self.get_eigs(ted, 'kappa_doping')
            ted['kappa_best'] = self.get_extreme(ted, 'kappa_eigs', maximize=False)
            ted['hall_eigs'] = self.get_eigs(ted, 'hall_doping')
            ted['hall_best'] = self.get_extreme(ted, 'hall_eigs')

            tdb.boltztrap.insert(jsanitize(ted))

            update_spec = {'prev_vasp_dir': fw_spec['prev_vasp_dir'],
                       'boltztrap_dir': os.getcwd(),
                       'prev_task_type': fw_spec['task_type'],
                       'mpsnl': fw_spec['mpsnl'],
                       'snlgroup_id': fw_spec['snlgroup_id'],
                       'run_tags': fw_spec['run_tags'], 'parameters': fw_spec.get('parameters')}

        return FWAction(update_spec=update_spec)