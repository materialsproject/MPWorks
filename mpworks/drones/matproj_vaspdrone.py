import json
import os
import math
from pymongo import MongoClient
from matgendb.creator import VaspToDbTaskDrone
from mpworks.drones.signals_vasp import VASPInputsExistSignal, VASPOutputsExistSignal, VASPOutSignal, HitAMemberSignal, SegFaultSignal, VASPStartedCompletedSignal, PositiveEnergySignal, ChargeUnconvergedSignal, WallTimeSignal, DiskSpaceExceededSignal, StopcarExistsSignal
from mpworks.snl_utils.snl_mongo import SNLMongoAdapter
from pymatgen.core.structure import Structure
from pymatgen.matproj.snl import StructureNL
from mpworks.drones.signals_base import SignalDetectorList

__author__ = 'Anubhav Jain'
__copyright__ = 'Copyright 2013, The Materials Project'
__version__ = '0.1'
__maintainer__ = 'Anubhav Jain'
__email__ = 'ajain@lbl.gov'
__date__ = 'Mar 26, 2013'


def is_valid_vasp_dir(mydir):
    # note that the OUTCAR and POSCAR are known to be empty in some
    # situations
    files = ["OUTCAR", "POSCAR", "INCAR", "KPOINTS"]
    for f in files:
        m_file = os.path.join(mydir, f)
        if not (os.path.exists(m_file) and
                os.stat(m_file).st_size > 0):
            return False
    return True

class MatprojVaspDrone(VaspToDbTaskDrone):

    def assimilate(self, path):
        """
        Parses vasp runs. Then insert the result into the db. and return the
        task_id or doc of the insertion.

        Returns:
            If in simulate_mode, the entire doc is returned for debugging
            purposes. Else, only the task_id of the inserted doc is returned.
        """

        # set the task_id in advance, because it's needed by post-process in order to auto-link SNL history
        conn = MongoClient(self.host, self.port)
        db = conn[self.database]
        if self.user:
            db.authenticate(self.user, self.password)
        coll = db[self.collection]
        task_id = db.counter.find_and_modify(query={"_id": "taskid"}, update={"$inc": {"c": 1}})["c"]
        self.additional_fields = self.additional_fields if self.additional_fields else {}
        self.additional_fields.update({'task_id': task_id})

        d = self.get_task_doc(path, self.parse_dos,
                              self.additional_fields)
        tid = self._insert_doc(d)
        return tid, d

    @classmethod
    def post_process(cls, dir_name, d):
        # run the post-process of the superclass
        VaspToDbTaskDrone.post_process(dir_name, d)

        # custom Materials Project post-processing for FireWorks
        with open(os.path.join(dir_name, 'FW.json')) as f:
            fw_dict = json.load(f)
            d['fw_id'] = fw_dict['fw_id']
            d['snl'] = fw_dict['spec']['mpsnl']
            d['snlgroup_id'] = fw_dict['spec']['snlgroup_id']
            d['submission_id'] = fw_dict['spec'].get('submission_id')
            d['run_tags'] = fw_dict['spec'].get('run_tags', [])
            d['vaspinputset_name'] = fw_dict['spec'].get('vaspinputset_name')
            d['task_type'] = fw_dict['spec']['task_type']

            if 'optimize structure' in d['task_type']:
                # create a new SNL based on optimized structure
                new_s = Structure.from_dict(d['output']['crystal'])
                old_snl = StructureNL.from_dict(d['snl'])
                history = old_snl.history
                history.append(
                    {'name':'Materials Project structure optimization',
                     'url':'http://www.materialsproject.org',
                     'description':{'task_type': d['task_type'], 'fw_id': d['fw_id'], 'task_id': d['task_id']}})
                new_snl = StructureNL(new_s, old_snl.authors, old_snl.projects,
                                      old_snl.references, old_snl.remarks,
                                      old_snl.data, history)

                # enter new SNL into SNL db
                # get the SNL mongo adapter
                sma = SNLMongoAdapter.auto_load()

                # add snl
                mpsnl, snlgroup_id = sma.add_snl(new_snl)
                d['snl_final'] = mpsnl.to_dict
                d['snlgroup_id_final'] = snlgroup_id
                d['snlgroup_changed'] = d['snlgroup_id'] != d['snlgroup_id_final']

        # custom processing for detecting errors
        vasp_signals = {}
        critical_errors = ["INPUTS_DONT_EXIST",
                           "OUTPUTS_DONT_EXIST", "INCOHERENT_POTCARS",
                           "VASP_HASNT_STARTED", "VASP_HASNT_COMPLETED",
                           "STOPCAR_EXISTS", "POSITIVE_ENERGY",
                           "CHARGE_UNCONVERGED", "TOO_MANY_ELECTRONIC_STEPS",
                           "NETWORK_QUIESCED", "HARD_KILLED",
                           "HIGH_RESIDUAL_FORCE", "INSANE_ENERGY",
                           "WALLTIME_EXCEEDED", "ATOMS_TOO_CLOSE", "DISK_SPACE_EXCEEDED"]

        MAX_FORCE_THRESHOLD = 0.5  # 500 meV
        INSANE_ENERGY_CUTOFF = -15  # should be sufficiently insane

        # get the last relaxation dir
        # the order is relax2, current dir, then relax1. This is because
        # after completing relax1, the job happens in the current dir. Finally
        # it gets moved to relax2.
        # There are some weird cases where both the current dir and relax2
        # contain data. The relax2 is good, but the current dir is bad.
        # This should not really happen, but trust relax2 in this case.
        last_relax_dir = dir_name
        if is_valid_vasp_dir(os.path.join(dir_name, "relax2")):
            last_relax_dir = os.path.join(dir_name, "relax2")
        elif is_valid_vasp_dir(dir_name):
            pass
        elif is_valid_vasp_dir(os.path.join(dir_name, "relax1")):
            last_relax_dir = os.path.join(dir_name, "relax1")

        vasp_signals['last_relax_dir'] = last_relax_dir
        ## see what error signals are present

        print "getting signals for dir :{}".format(last_relax_dir)

        sl = SignalDetectorList()
        sl.append(VASPInputsExistSignal())
        sl.append(VASPOutputsExistSignal())
        sl.append(VASPOutSignal())
        sl.append(HitAMemberSignal())
        sl.append(SegFaultSignal())
        sl.append(VASPStartedCompletedSignal())
        sl.append(PositiveEnergySignal())
        sl.append(ChargeUnconvergedSignal())

        signals = sl.detect_all(last_relax_dir)

        # try detecting walltime in two directories - dir_name and one level above
        signals = signals.union(WallTimeSignal().detect(dir_name))
        root_dir = os.path.dirname(dir_name)  # one level above dir_name
        signals = signals.union(WallTimeSignal().detect(root_dir))

        # try detecting disk space error in dir_name and root dir
        signals = signals.union(DiskSpaceExceededSignal().detect(dir_name))
        root_dir = os.path.dirname(dir_name)  # one level above dir_name
        signals = signals.union(DiskSpaceExceededSignal().detect(root_dir))

        # try detecting stopcar in many dirs - root, relax1, relax2
        # note that only doing the 'last_relax_dir' does not seem to work
        signals = signals.union(StopcarExistsSignal().detect(dir_name))
        signals = signals.union(StopscarExistsSignal()
                                .detect(os.path.join(dir_name, "relax1")))
        signals = signals.union(StopcarExistsSignal()
                                .detect(os.path.join(dir_name, "relax2")))

        if d['state'] == 'successful':
            # handle the max force and max force error
            max_force = max([math.sqrt(a[0] * a[0] + a[1] * a[1] + a[2] * a[2])
                             for a in d['calculations'][-1]['output']
                             ['ionic_steps'][-1]['forces']])
            d['analysis']['max_force'] = max_force

            if max_force > MAX_FORCE_THRESHOLD:
                signals.add("HIGH_RESIDUAL_FORCE")

            # handle insane energies
            if d['output']['final_energy_per_atom'] <= INSANE_ENERGY_CUTOFF:
                signals.add("INSANE_ENERGY")

        if len(d.get("calculations", [])) > 0:
            ismear = d['calculations'][0]["input"]["incar"]["ISMEAR"]
            if ismear == 1:
                signals.add("ISMEAR_1_ERROR")

            max_steps = d['calculations'][-1]["input"]["incar"]["NSW"]
            total_steps = len(d['calculations'][-1]["output"]["ionic_steps"])
            if total_steps >= max_steps:
                signals.add("TOO_MANY_IONIC_STEPS")

        signals = list(signals)

        critical_signals = [val for val in signals if val in critical_errors]

        vasp_signals['signals'] = signals
        vasp_signals['critical_signals'] = critical_signals

        vasp_signals['num_signals'] = len(signals)
        vasp_signals['num_critical'] = len(critical_signals)

        if len(critical_signals) > 0 and d['state'] == "successful":
            d.update({"state": "rejected"})

        d.update({'vasp_signals': vasp_signals})