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
from fireworks.utilities.fw_utilities import get_slug
from monty.json import jsanitize
from mpworks.snl_utils.mpsnl import get_meta_from_structure
from mpworks.workflows.wf_utils import get_block_part
import numpy as np
from pymatgen import Composition
from pymatgen.electronic_structure.bandstructure import BandStructure
from pymatgen.electronic_structure.boltztrap import BoltztrapRunner, BoltztrapAnalyzer
from pymatgen.entries.compatibility import MaterialsProjectCompatibility
from pymatgen.entries.computed_entries import ComputedEntry

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

    def get_eigs(self, d, target, iso_cutoff=0.05):
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


    def get_extreme(self, d, target, maximize=True, iso_cutoff=0.05, max_didx=None):
            """

            :param d: data dictionary
            :param target: root key of target property, e.g. 'seebeck_doping'
            :param maximize: (bool) max if True, min if False
            :param iso_cutoff: percent cutoff for isotropicity
            :param max_didx: max doping idx
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
                        if not max_didx or didx <= max_didx:
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
        # import here to prevent import errors in bigger MPCollab
        from mpcollab.thermoelectrics.boltztrap_TE import BoltztrapAnalyzerTE, BoltzSPB
        # get the band structure and nelect from files
        """
        prev_dir = get_loc(fw_spec['prev_vasp_dir'])
        vasprun_loc = zpath(os.path.join(prev_dir, 'vasprun.xml'))
        kpoints_loc = zpath(os.path.join(prev_dir, 'KPOINTS'))

        vr = Vasprun(vasprun_loc)
        bs = vr.get_band_structure(kpoints_filename=kpoints_loc)
        """
        filename = get_slug(
            'JOB--' + fw_spec['mpsnl'].structure.composition.reduced_formula + '--'
            + fw_spec['task_type'])
        with open(filename, 'w+') as f:
            f.write('')

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

            props = {"calculations": 1, "task_id": 1, "state": 1, "pseudo_potential": 1, "run_type": 1, "is_hubbard": 1, "hubbards": 1, "unit_cell_formula": 1}
            m_task = tdb.tasks.find_one({"dir_name": block_part}, props)
            if not m_task:
                time.sleep(60)  # only thing to think of is wait for DB insertion(?)
                m_task = tdb.tasks.find_one({"dir_name": block_part}, props)

            if not m_task:
                raise ValueError("Could not find task with dir_name: {}".format(block_part))

            if m_task['state'] != 'successful':
                raise ValueError("Cannot run Boltztrap; parent job unsuccessful")

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

            # 8/21/15 - Anubhav removed fs_id (also see line further below, ted['boltztrap_full_fs_id'] ...)
            # 8/21/15 - this is to save space in MongoDB, as well as non-use of full Boltztrap output (vs rerun)
            """
            data = bta.as_dict()
            data.update(get_meta_from_structure(bs._structure))
            data['snlgroup_id'] = fw_spec['snlgroup_id']
            data['run_tags'] = fw_spec['run_tags']
            data['snl'] = fw_spec['mpsnl']
            data['dir_name_full'] = dir
            data['dir_name'] = get_block_part(dir)
            data['task_id'] = m_task['task_id']
            del data['hall']  # remove because it is too large and not useful
            fs = gridfs.GridFS(tdb, "boltztrap_full_fs")
            btid = fs.put(json.dumps(jsanitize(data)))
            """

            # now for the "sanitized" data
            te_analyzer = BoltztrapAnalyzerTE.from_BoltztrapAnalyzer(bta)

            ted = te_analyzer.as_dict()
            del ted['seebeck']
            del ted['hall']
            del ted['kappa']
            del ted['cond']

            # ted['boltztrap_full_fs_id'] = btid
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
            ted['pf_best_dope18'] = self.get_extreme(ted, 'pf_eigs', max_didx=3)
            ted['pf_best_dope19'] = self.get_extreme(ted, 'pf_eigs', max_didx=4)
            ted['zt_eigs'] = self.get_eigs(ted, 'zt_doping')
            ted['zt_best'] = self.get_extreme(ted, 'zt_eigs')
            ted['zt_best_dope18'] = self.get_extreme(ted, 'zt_eigs', max_didx=3)
            ted['zt_best_dope19'] = self.get_extreme(ted, 'zt_eigs', max_didx=4)
            ted['seebeck_eigs'] = self.get_eigs(ted, 'seebeck_doping')
            ted['seebeck_best'] = self.get_extreme(ted, 'seebeck_eigs')
            ted['seebeck_best_dope18'] = self.get_extreme(ted, 'seebeck_eigs', max_didx=3)
            ted['seebeck_best_dope19'] = self.get_extreme(ted, 'seebeck_eigs', max_didx=4)
            ted['cond_eigs'] = self.get_eigs(ted, 'cond_doping')
            ted['cond_best'] = self.get_extreme(ted, 'cond_eigs')
            ted['cond_best_dope18'] = self.get_extreme(ted, 'cond_eigs', max_didx=3)
            ted['cond_best_dope19'] = self.get_extreme(ted, 'cond_eigs', max_didx=4)
            ted['kappa_eigs'] = self.get_eigs(ted, 'kappa_doping')
            ted['kappa_best'] = self.get_extreme(ted, 'kappa_eigs', maximize=False)
            ted['kappa_best_dope18'] = self.get_extreme(ted, 'kappa_eigs', maximize=False, max_didx=3)
            ted['kappa_best_dope19'] = self.get_extreme(ted, 'kappa_eigs', maximize=False, max_didx=4)

            try:
                bzspb = BoltzSPB(te_analyzer)
                maxpf_p = bzspb.get_maximum_power_factor('p', temperature=0, tau=1E-14, ZT=False, kappal=0.5,\
                    otherprops=('get_seebeck_mu_eig', 'get_conductivity_mu_eig', \
                                                    'get_thermal_conductivity_mu_eig', 'get_average_eff_mass_tensor_mu'))

                maxpf_n = bzspb.get_maximum_power_factor('n', temperature=0, tau=1E-14, ZT=False, kappal=0.5,\
                    otherprops=('get_seebeck_mu_eig', 'get_conductivity_mu_eig', \
                                                    'get_thermal_conductivity_mu_eig', 'get_average_eff_mass_tensor_mu'))

                maxzt_p = bzspb.get_maximum_power_factor('p', temperature=0, tau=1E-14, ZT=True, kappal=0.5, otherprops=('get_seebeck_mu_eig', 'get_conductivity_mu_eig', \
                                                    'get_thermal_conductivity_mu_eig', 'get_average_eff_mass_tensor_mu'))

                maxzt_n = bzspb.get_maximum_power_factor('n', temperature=0, tau=1E-14, ZT=True, kappal=0.5, otherprops=('get_seebeck_mu_eig', 'get_conductivity_mu_eig', \
                                                    'get_thermal_conductivity_mu_eig', 'get_average_eff_mass_tensor_mu'))

                ted['zt_best_finemesh'] = {'p': maxzt_p, 'n': maxzt_n}
                ted['pf_best_finemesh'] = {'p': maxpf_p, 'n': maxpf_n}
            except:
                import traceback
                traceback.print_exc()
                print 'COULD NOT GET FINE MESH DATA'

            # add is_compatible
            mpc = MaterialsProjectCompatibility("Advanced")
            try:
                func = m_task["pseudo_potential"]["functional"]
                labels = m_task["pseudo_potential"]["labels"]
                symbols = ["{} {}".format(func, label) for label in labels]
                parameters = {"run_type": m_task["run_type"],
                          "is_hubbard": m_task["is_hubbard"],
                          "hubbards": m_task["hubbards"],
                          "potcar_symbols": symbols}
                entry = ComputedEntry(Composition(m_task["unit_cell_formula"]),
                                      0.0, 0.0, parameters=parameters,
                                      entry_id=m_task["task_id"])

                ted["is_compatible"] = bool(mpc.process_entry(entry))
            except:
                traceback.print_exc()
                print 'ERROR in getting compatibility, task_id: {}'.format(m_task["task_id"])
                ted["is_compatible"] = None

            tdb.boltztrap.insert(jsanitize(ted))

            update_spec = {'prev_vasp_dir': fw_spec['prev_vasp_dir'],
                       'boltztrap_dir': os.getcwd(),
                       'prev_task_type': fw_spec['task_type'],
                       'mpsnl': fw_spec['mpsnl'],
                       'snlgroup_id': fw_spec['snlgroup_id'],
                       'run_tags': fw_spec['run_tags'], 'parameters': fw_spec.get('parameters')}

        return FWAction(update_spec=update_spec)
