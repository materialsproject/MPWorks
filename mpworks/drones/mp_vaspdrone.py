import json
import os
import datetime
import logging
import pprint
import re
import traceback
from monty.io import zopen
from monty.os.path import zpath
from pymongo import MongoClient
import gridfs
from matgendb.creator import VaspToDbTaskDrone
from mpworks.drones.signals import VASPInputsExistSignal, \
    VASPOutputsExistSignal, VASPOutSignal, HitAMemberSignal, SegFaultSignal, \
    VASPStartedCompletedSignal, WallTimeSignal, DiskSpaceExceededSignal, \
    SignalDetectorList, Relax2ExistsSignal
from mpworks.snl_utils.snl_mongo import SNLMongoAdapter
from mpworks.workflows.wf_utils import get_block_part
from pymatgen import Composition, MontyEncoder
from pymatgen.core.structure import Structure
from pymatgen.entries.compatibility import MaterialsProjectCompatibility
from pymatgen.entries.computed_entries import ComputedEntry
from pymatgen.matproj.snl import StructureNL
from pymatgen.io.vasp.outputs import Vasprun, Outcar
from pymatgen.analysis.structure_analyzer import oxide_type


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
        if not os.path.exists(zpath(m_file)) or not(os.stat(m_file).st_size > 0 or os.stat(m_file+'.gz').st_size > 0):
            return False
    return True


class MPVaspDrone(VaspToDbTaskDrone):
    def assimilate(self, path, launches_coll=None):
        """
        Parses vasp runs. Then insert the result into the db. and return the
        task_id or doc of the insertion.

        Returns:
            If in simulate_mode, the entire doc is returned for debugging
            purposes. Else, only the task_id of the inserted doc is returned.
        """

        d = self.get_task_doc(path)
        if self.additional_fields:
            d.update(self.additional_fields)  # always add additional fields, even for failed jobs

        try:
            d["dir_name_full"] = d["dir_name"].split(":")[1]
            d["dir_name"] = get_block_part(d["dir_name_full"])
            d["stored_data"] = {}
        except:
            print 'COULD NOT GET DIR NAME'
            pprint.pprint(d)
            print traceback.format_exc()
            raise ValueError('IMPROPER PARSING OF {}'.format(path))

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
                    if ("task_id" not in d) or (not d["task_id"]):
                        d["task_id"] = "mp-{}".format(
                            db.counter.find_and_modify(
                                query={"_id": "taskid"},
                                update={"$inc": {"c": 1}})["c"])
                    logger.info("Inserting {} with taskid = {}"
                    .format(d["dir_name"], d["task_id"]))
                elif self.update_duplicates:
                    d["task_id"] = result["task_id"]
                    logger.info("Updating {} with taskid = {}"
                    .format(d["dir_name"], d["task_id"]))

                #Fireworks processing

                self.process_fw(path, d)

                try:
                    #Add oxide_type
                    struct=Structure.from_dict(d["output"]["crystal"])
                    d["oxide_type"]=oxide_type(struct)
                except:
                    logger.error("can't get oxide_type for {}".format(d["task_id"]))
                    d["oxide_type"] = None

                #Override incorrect outcar subdocs for two step relaxations
                if "optimize structure" in d['task_type'] and \
                    os.path.exists(os.path.join(path, "relax2")):
                    try:
                        run_stats = {}
                        for i in [1,2]:
                            o_path = os.path.join(path,"relax"+str(i),"OUTCAR")
                            o_path = o_path if os.path.exists(o_path) else o_path+".gz"
                            outcar = Outcar(o_path)
                            d["calculations"][i-1]["output"]["outcar"] = outcar.as_dict()
                            run_stats["relax"+str(i)] = outcar.run_stats
                    except:
                        logger.error("Bad OUTCAR for {}.".format(path))

                    try:
                        overall_run_stats = {}
                        for key in ["Total CPU time used (sec)", "User time (sec)",
                                    "System time (sec)", "Elapsed time (sec)"]:
                            overall_run_stats[key] = sum([v[key]
                                              for v in run_stats.values()])
                        run_stats["overall"] = overall_run_stats
                    except:
                        logger.error("Bad run stats for {}.".format(path))

                    d["run_stats"] = run_stats

                # add is_compatible
                mpc = MaterialsProjectCompatibility("Advanced")

                try:
                    func = d["pseudo_potential"]["functional"]
                    labels = d["pseudo_potential"]["labels"]
                    symbols = ["{} {}".format(func, label) for label in labels]
                    parameters = {"run_type": d["run_type"],
                              "is_hubbard": d["is_hubbard"],
                              "hubbards": d["hubbards"],
                              "potcar_symbols": symbols}
                    entry = ComputedEntry(Composition(d["unit_cell_formula"]),
                                          0.0, 0.0, parameters=parameters,
                                          entry_id=d["task_id"])

                    d['is_compatible'] = bool(mpc.process_entry(entry))
                except:
                    traceback.print_exc()
                    print 'ERROR in getting compatibility'
                    d['is_compatible'] = None


                #task_type dependent processing
                if 'static' in d['task_type']:
                    launch_doc = launches_coll.find_one({"fw_id": d['fw_id'], "launch_dir": {"$regex": d["dir_name"]}}, {"action.stored_data": 1})
                    for i in ["conventional_standard_structure", "symmetry_operations",
                              "symmetry_dataset", "refined_structure"]:
                        try:
                            d['stored_data'][i] = launch_doc['action']['stored_data'][i]
                        except:
                            pass

                #parse band structure if necessary
                if ('band structure' in d['task_type'] or "Uniform" in d['task_type'])\
                    and d['state'] == 'successful':
                    launch_doc = launches_coll.find_one({"fw_id": d['fw_id'], "launch_dir": {"$regex": d["dir_name"]}},
                                                        {"action.stored_data": 1})
                    vasp_run = Vasprun(zpath(os.path.join(path, "vasprun.xml")), parse_projected_eigen=False)

                    if 'band structure' in d['task_type']:
                        def string_to_numlist(stringlist):
                            g=re.search('([0-9\-\.eE]+)\s+([0-9\-\.eE]+)\s+([0-9\-\.eE]+)', stringlist)
                            return [float(g.group(i)) for i in range(1,4)]

                        for i in ["kpath_name", "kpath"]:
                            d['stored_data'][i] = launch_doc['action']['stored_data'][i]
                        kpoints_doc = d['stored_data']['kpath']['kpoints']
                        for i in kpoints_doc:
                            kpoints_doc[i]=string_to_numlist(kpoints_doc[i])
                        bs=vasp_run.get_band_structure(efermi=d['calculations'][0]['output']['outcar']['efermi'],
                                                       line_mode=True)
                    else:
                        bs=vasp_run.get_band_structure(efermi=d['calculations'][0]['output']['outcar']['efermi'],
                                                       line_mode=False)
                    bs_json = json.dumps(bs.as_dict(), cls=MontyEncoder)
                    fs = gridfs.GridFS(db, "band_structure_fs")
                    bs_id = fs.put(bs_json)
                    d['calculations'][0]["band_structure_fs_id"] = bs_id

                    # also override band gap in task doc
                    gap = bs.get_band_gap()
                    vbm = bs.get_vbm()
                    cbm = bs.get_cbm()
                    update_doc = {'bandgap': gap['energy'], 'vbm': vbm['energy'], 'cbm': cbm['energy'], 'is_gap_direct': gap['direct']}
                    d['analysis'].update(update_doc)
                    d['calculations'][0]['output'].update(update_doc)

                coll.update({"dir_name": d["dir_name"]}, d, upsert=True)

                return d["task_id"], d
            else:
                logger.info("Skipping duplicate {}".format(d["dir_name"]))
                return result["task_id"], result

        else:
            d["task_id"] = 0
            logger.info("Simulated insert into database for {} with task_id {}"
            .format(d["dir_name"], d["task_id"]))
            return 0, d

    def process_fw(self, dir_name, d):
        d["task_id_deprecated"] = int(d["task_id"].split('-')[-1])  # useful for WC and AJ

        # update the run fields to give species group in root, if exists
        for r in d['run_tags']:
            if "species_group=" in r:
                d["species_group"] = int(r.split("=")[-1])
                break

        # custom Materials Project post-processing for FireWorks
        with zopen(zpath(os.path.join(dir_name, 'FW.json'))) as f:
            fw_dict = json.load(f)
            d['fw_id'] = fw_dict['fw_id']
            d['snl'] = fw_dict['spec']['mpsnl']
            d['snlgroup_id'] = fw_dict['spec']['snlgroup_id']
            d['vaspinputset_name'] = fw_dict['spec'].get('vaspinputset_name')
            d['task_type'] = fw_dict['spec']['task_type']

            if not self.update_duplicates:
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

                    # enter new SNL into SNL db
                    # get the SNL mongo adapter
                    sma = SNLMongoAdapter.auto_load()

                    # add snl
                    mpsnl, snlgroup_id, spec_group = sma.add_snl(new_snl, snlgroup_guess=d['snlgroup_id'])
                    d['snl_final'] = mpsnl.as_dict()
                    d['snlgroup_id_final'] = snlgroup_id
                    d['snlgroup_changed'] = (d['snlgroup_id'] !=
                                             d['snlgroup_id_final'])
                else:
                    d['snl_final'] = d['snl']
                    d['snlgroup_id_final'] = d['snlgroup_id']
                    d['snlgroup_changed'] = False

        # custom processing for detecting errors
        new_style = os.path.exists(zpath(os.path.join(dir_name, 'FW.json')))
        vasp_signals = {}
        critical_errors = ["INPUTS_DONT_EXIST",
                           "OUTPUTS_DONT_EXIST", "INCOHERENT_POTCARS",
                           "VASP_HASNT_STARTED", "VASP_HASNT_COMPLETED",
                           "CHARGE_UNCONVERGED", "NETWORK_QUIESCED",
                           "HARD_KILLED", "WALLTIME_EXCEEDED",
                           "ATOMS_TOO_CLOSE", "DISK_SPACE_EXCEEDED", "NO_RELAX2", "POSITIVE_ENERGY"]

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

        if d['state'] == 'successful' and 'optimize structure' in d['task_type']:
            sl.append(Relax2ExistsSignal())

        signals = sl.detect_all(last_relax_dir)

        signals = signals.union(WallTimeSignal().detect(dir_name))
        if not new_style:
            root_dir = os.path.dirname(dir_name)  # one level above dir_name
            signals = signals.union(WallTimeSignal().detect(root_dir))

        signals = signals.union(DiskSpaceExceededSignal().detect(dir_name))
        if not new_style:
            root_dir = os.path.dirname(dir_name)  # one level above dir_name
            signals = signals.union(DiskSpaceExceededSignal().detect(root_dir))

        if d.get('output',{}).get('final_energy', None) > 0:
            signals.add('POSITIVE_ENERGY')

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

