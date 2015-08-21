import json
import os
import datetime
import logging

from pymongo import MongoClient
import gridfs

from matgendb.creator import VaspToDbTaskDrone
from mpworks.drones.signals import VASPInputsExistSignal, \
    VASPOutputsExistSignal, VASPOutSignal, HitAMemberSignal, SegFaultSignal, \
    VASPStartedCompletedSignal, WallTimeSignal, DiskSpaceExceededSignal, \
    SignalDetectorList
from mpworks.fix_scripts.legacy.mps_to_snl import mps_dict_to_snl
from mpworks.snl_utils.snl_mongo import SNLMongoAdapter
from mpworks.workflows.wf_utils import get_block_part
from pymatgen.core.structure import Structure
from pymatgen.matproj.snl import StructureNL
from pymatgen.serializers.json_coders import MontyEncoder

__author__ = 'Anubhav Jain'
__copyright__ = 'Copyright 2013, The Materials Project'
__version__ = '0.1'
__maintainer__ = 'Anubhav Jain'
__email__ = 'ajain@lbl.gov'
__date__ = 'Mar 26, 2013'

logger = logging.getLogger(__name__)


def is_valid_vasp_dir(mydir):
    # note that the OUTCAR and POSCAR are known to be empty in some
    # situations
    files = ["OUTCAR", "POSCAR", "INCAR", "KPOINTS"]
    for f in files:
        m_file = os.path.join(mydir, f)
        if not (os.path.exists(m_file) and os.stat(m_file).st_size > 0):
            return False
    return True


class MPVaspDrone_CONVERSION(VaspToDbTaskDrone):
    # AJ: give old_task, not dir_name
    def assimilate(self, old_task):
        """
        Parses vasp runs. Then insert the result into the db. and return the
        task_id or doc of the insertion.

        Returns:
            If in simulate_mode, the entire doc is returned for debugging
            purposes. Else, only the task_id of the inserted doc is returned.
        """

        path = old_task['dir_name']  # AJ: get dir name from task
        d = self.get_task_doc(path, self.parse_dos,
                              self.additional_fields)
        d["dir_name_full"] = d["dir_name"].split(":")[1]
        d["dir_name"] = get_block_part(d["dir_name_full"])
        if not self.simulate:
            # Perform actual insertion into db. Because db connections cannot
            # be pickled, every insertion needs to create a new connection
            # to the db.
            conn = MongoClient(self.host, self.port)
            db = conn[self.database]
            if self.user:
                db.authenticate(self.user, self.password)
            coll = db[self.collection]

            # Insert dos data into gridfs and then remove it from the dict.
            # DOS data tends to be above the 4Mb limit for mongo docs. A ref
            # to the dos file is in the dos_fs_id.
            result = coll.find_one({"dir_name": d["dir_name"]})
            if result is None or self.update_duplicates:
                if self.parse_dos and "calculations" in d:
                    for calc in d["calculations"]:
                        if "dos" in calc:
                            dos = json.dumps(calc["dos"], cls=MontyEncoder)
                            fs = gridfs.GridFS(db, "dos_fs")
                            dosid = fs.put(dos)
                            calc["dos_fs_id"] = dosid
                            del calc["dos"]

                d["last_updated"] = datetime.datetime.today()
                if result is None:
                    d["task_id"] = "mp-{}".format(old_task['task_id'])  # AJ: old task_id is new
                    logger.info("Inserting {} with taskid = {}"
                    .format(d["dir_name"], d["task_id"]))
                elif self.update_duplicates:
                    d["task_id"] = result["task_id"]
                    logger.info("Updating {} with taskid = {}"
                    .format(d["dir_name"], d["task_id"]))

                #Fireworks processing
                self.process_fw(old_task, d)
                coll.update({"dir_name": d["dir_name"]}, {"$set": d},
                            upsert=True)
                return d["task_id"], d
            else:
                logger.info("Skipping duplicate {}".format(d["dir_name"]))
                return result["task_id"], result

        else:
            d["task_id"] = 0
            logger.info("Simulated insert into database for {} with task_id {}"
            .format(d["dir_name"], d["task_id"]))
            return 0, d

    # AJ: take in old_task
    def process_fw(self, old_task, d):
        # AJ - this whole section is different
        sma = SNLMongoAdapter.auto_load()

        d['old_engine'] = old_task.get('engine')
        if 'fw_id' in old_task:
            d['old_fw_id'] = old_task['fw_id']

        d['fw_id'] = None
        d['task_type'] = 'GGA+U optimize structure (2x)' if old_task[
            'is_hubbard'] else 'GGA optimize structure (2x)'
        d['submission_id'] = None
        d['vaspinputset_name'] = None

        snl_d = sma.snl.find_one({'about._materialsproject.deprecated.mps_ids': old_task['mps_id']})
        if old_task.get('mps_id', -1) > 0 and snl_d:
            # grab the SNL from the SNL db
            del snl_d['_id']
            d['snl'] = snl_d
            d['snlgroup_id'] = sma.snlgroups.find_one({'all_snl_ids': d['snl']['snl_id']}, {'snlgroup_id': 1})['snlgroup_id']

        elif 'mps' in old_task and old_task['mps']:
            snl = mps_dict_to_snl(old_task['mps'])
            mpsnl, snlgroup_id = sma.add_snl(snl)
            d['snl'] = mpsnl.as_dict()
            d['snlgroup_id'] = snlgroup_id
        else:
            s = Structure.from_dict(old_task['input']['crystal'])
            snl = StructureNL(s, 'Anubhav Jain <ajain@lbl.gov>', remarks=['origin unknown'])
            mpsnl, snlgroup_id = sma.add_snl(snl)
            d['snl'] = mpsnl.as_dict()
            d['snlgroup_id'] = snlgroup_id


        if 'optimize structure' in d['task_type'] and 'output' in d:
            # create a new SNL based on optimized structure
            new_s = Structure.from_dict(d['output']['crystal'])
            old_snl = StructureNL.from_dict(d['snl'])
            history = old_snl.history
            history.append(
                {'name': 'Materials Project structure optimization',
                 'url': 'http://www.materialsproject.org',
                 'description': {'task_type': d['task_type'],
                                 'fw_id': d['fw_id'],
                                 'task_id': d['task_id']}})
            new_snl = StructureNL(new_s, old_snl.authors, old_snl.projects,
                                  old_snl.references, old_snl.remarks,
                                  old_snl.data, history)

            # add snl
            mpsnl, snlgroup_id = sma.add_snl(new_snl, snlgroup_guess=d['snlgroup_id'])

            d['snl_final'] = mpsnl.as_dict()
            d['snlgroup_id_final'] = snlgroup_id
            d['snlgroup_changed'] = (d['snlgroup_id'] !=
                                     d['snlgroup_id_final'])

        # custom processing for detecting errors
        dir_name = old_task['dir_name']
        new_style = os.path.exists(os.path.join(dir_name, 'FW.json'))
        vasp_signals = {}
        critical_errors = ["INPUTS_DONT_EXIST",
                           "OUTPUTS_DONT_EXIST", "INCOHERENT_POTCARS",
                           "VASP_HASNT_STARTED", "VASP_HASNT_COMPLETED",
                           "CHARGE_UNCONVERGED", "NETWORK_QUIESCED",
                           "HARD_KILLED", "WALLTIME_EXCEEDED",
                           "ATOMS_TOO_CLOSE", "DISK_SPACE_EXCEEDED"]

        last_relax_dir = dir_name

        if not new_style:
            # get the last relaxation dir
            # the order is relax2, current dir, then relax1. This is because
            # after completing relax1, the job happens in the current dir.
            # Finally, it gets moved to relax2.
            # There are some weird cases where both the current dir and relax2
            # contain data. The relax2 is good, but the current dir is bad.
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

        signals = sl.detect_all(last_relax_dir)

        signals = signals.union(WallTimeSignal().detect(dir_name))
        if not new_style:
            root_dir = os.path.dirname(dir_name)  # one level above dir_name
            signals = signals.union(WallTimeSignal().detect(root_dir))

        signals = signals.union(DiskSpaceExceededSignal().detect(dir_name))
        if not new_style:
            root_dir = os.path.dirname(dir_name)  # one level above dir_name
            signals = signals.union(DiskSpaceExceededSignal().detect(root_dir))

        signals = list(signals)

        critical_signals = [val for val in signals if val in critical_errors]

        vasp_signals['signals'] = signals
        vasp_signals['critical_signals'] = critical_signals

        vasp_signals['num_signals'] = len(signals)
        vasp_signals['num_critical'] = len(critical_signals)

        if len(critical_signals) > 0 and d['state'] == "successful":
            d["state"] = "error"

        d['analysis'] = d.get('analysis', {})
        d['analysis']['errors_MP'] = vasp_signals

        d['run_tags'] = ['PBE']
        d['run_tags'].extend(d['pseudo_potential']['labels'])
        d['run_tags'].extend([e+"="+str(d['hubbards'].get(e, 0)) for e in d['elements']])
