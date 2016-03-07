from monty.os.path import zpath
import os
import json
from pymongo import MongoClient
import numpy as np
from decimal import Decimal

__author__ = 'Wei Chen'
__credits__ = 'Joseph Montoya'
__maintainer__ = 'Joseph Montoya <montoyjh@lbl.gov'

from fireworks.utilities.fw_serializers import FWSerializable
from fireworks.core.firework import FireTaskBase, FWAction
from pymatgen.io.vasp.inputs import Incar, Poscar
from pymatgen.analysis.elasticity.strain import DeformedStructureSet, IndependentStrain
from pymatgen.analysis.elasticity.stress import Stress
from pymatgen.analysis.elasticity.elastic import ElasticTensor
from fireworks.core.firework import Firework, Workflow
from mpworks.firetasks.vasp_io_tasks import VaspWriterTask, VaspToDBTask
from mpworks.firetasks.custodian_task import get_custodian_task
from fireworks.utilities.fw_utilities import get_slug
from pymatgen import Composition
from pymatgen.matproj.snl import StructureNL
from mpworks.workflows import snl_to_wf
from mpworks.firetasks.snl_tasks import AddSNLTask
from mpworks.snl_utils.mpsnl import MPStructureNL
from pymatgen.core.structure import Structure
from mpworks.workflows.wf_settings import QA_VASP, QA_DB, QA_VASP_SMALL
from pymatgen.io.vasp.inputs import Poscar, Kpoints

def update_spec_force_convergence(spec, user_vasp_settings=None):
    fw_spec = spec
    update_set = {"ENCUT": 700, "EDIFF": 0.000001, "ALGO":"N", "NPAR":2}
    if user_vasp_settings and user_vasp_settings.get("incar"):
            update_set.update(user_vasp_settings["incar"])
    fw_spec['vasp']['incar'].update(update_set)
    old_struct=Poscar.from_dict(fw_spec["vasp"]["poscar"]).structure
    if user_vasp_settings and user_vasp_settings.get("kpoints"):
        kpoints_density = user_vasp_settings["kpoints"]["kpoints_density"]
    else:
        kpoints_density = 7000
    k=Kpoints.automatic_density(old_struct, kpoints_density)
    fw_spec['vasp']['kpoints'] = k.as_dict()
    return fw_spec

class SetupFConvergenceTask(FireTaskBase, FWSerializable):
    # TODO: This firetask isn't yet used in the EC workflow
    _fw_name = "Setup Force Convergence Task"

    def run_task(self, fw_spec):
        incar = fw_spec['vasp']['incar']
        update_set = {"ENCUT": 700, "EDIFF": 0.000001}
        incar.update(update_set)
        #if fw_spec['double_kmesh']:
        kpoints = fw_spec['vasp']['kpoints']
        k = [int(round(2.5*k)) if int(round(2.5*k))%2 
             else int(round(2.5*k))+1 for k in kpoints['kpoints'][0]]
        kpoints['kpoints'] = [k]
        return FWAction()


class SetupElastConstTask(FireTaskBase, FWSerializable):
    _fw_name = "Setup Elastic Constant Task"

    def run_task(self, fw_spec):
        incar = Incar.from_file(zpath("INCAR"))
        incar.update({"ISIF": 2})
        incar.write_file("INCAR")
        return FWAction()

class SetupDeformedStructTask(FireTaskBase, FWSerializable):
    _fw_name = "Setup Deformed Struct Task"

    def run_task(self, fw_spec):
        # Read structure from previous relaxation
        relaxed_struct = fw_spec['output']['crystal']
        # Generate deformed structures
        d_struct_set = DeformedStructureSet(relaxed_struct, ns=0.06)
        wf=[]
        for i, d_struct in enumerate(d_struct_set.def_structs):
            fws=[]
            connections={}
            f = Composition(d_struct.formula).alphabetical_formula
            snl = StructureNL(d_struct, 'Joseph Montoya <montoyjh@lbl.gov>', 
                              projects=["Elasticity"])
            tasks = [AddSNLTask()]
            snl_priority = fw_spec.get('priority', 1)
            spec = {'task_type': 'Add Deformed Struct to SNL database', 
                    'snl': snl.as_dict(), 
                    '_queueadapter': QA_DB, 
                    '_priority': snl_priority}
            if 'snlgroup_id' in fw_spec and isinstance(snl, MPStructureNL):
                spec['force_mpsnl'] = snl.as_dict()
                spec['force_snlgroup_id'] = fw_spec['snlgroup_id']
                del spec['snl']
            fws.append(Firework(tasks, spec, 
                                name=get_slug(f + '--' + spec['task_type']), 
                                fw_id=-1000+i*10))
            connections[-1000+i*10] = [-999+i*10]
            spec = snl_to_wf._snl_to_spec(snl, 
                                          parameters={'exact_structure':True})
            spec = update_spec_force_convergence(spec)
            spec['deformation_matrix'] = d_struct_set.deformations[i].tolist()
            spec['original_task_id'] = fw_spec["task_id"]
            spec['_priority'] = fw_spec['_priority']*2
            #Turn off dupefinder for deformed structure
            del spec['_dupefinder']
            spec['task_type'] = "Optimize deformed structure"
            fws.append(Firework([VaspWriterTask(), SetupElastConstTask(),
                                 get_custodian_task(spec)], 
                                spec, 
                                name=get_slug(f + '--' + spec['task_type']), 
                                fw_id=-999+i*10))
            
            priority = fw_spec['_priority']*3
            spec = {'task_type': 'VASP db insertion', 
                    '_priority': priority,
                    '_allow_fizzled_parents': True, 
                    '_queueadapter': QA_DB, 
                    'elastic_constant':"deformed_structure", 
                    'clean_task_doc':True,
                    'deformation_matrix':d_struct_set.deformations[i].tolist(), 
                    'original_task_id':fw_spec["task_id"]}
            fws.append(Firework([VaspToDBTask(), AddElasticDataToDBTask()], spec,
                                name=get_slug(f + '--' + spec['task_type']),
                                fw_id=-998+i*10))
            connections[-999+i*10] = [-998+i*10]
            wf.append(Workflow(fws, connections))
        return FWAction(additions=wf)


class AddElasticDataToDBTask(FireTaskBase, FWSerializable):
    _fw_name = "Add Elastic Data to DB"

    def run_task(self, fw_spec):
        db_dir = os.environ['DB_LOC']
        db_path = os.path.join(db_dir, 'tasks_db.json')
        i = fw_spec['original_task_id']

        with open(db_path) as f:
            db_creds = json.load(f)
        connection = MongoClient(db_creds['host'], db_creds['port'])
        tdb = connection[db_creds['database']]
        tdb.authenticate(db_creds['admin_user'], db_creds['admin_password'])
        tasks = tdb[db_creds['collection']]
        elasticity = tdb['elasticity']
        ndocs = tasks.find({"original_task_id": i, 
                            "state":"successful"}).count()
        existing_doc = elasticity.find_one({"relaxation_task_id" : i})
        if existing_doc:
            print "Updating: " + i
        else:
            print "New material: " + i
        d = {"analysis": {}, "error": [], "warning": []}
        d["ndocs"] = ndocs
        o = tasks.find_one({"task_id" : i},
                           {"pretty_formula" : 1, "spacegroup" : 1,
                            "snl" : 1, "snl_final" : 1, "run_tags" : 1})
        if not o:
            raise ValueError("Cannot find original task id")
        # Get stress from deformed structure
        d["deformation_tasks"] = {}
        ss_dict = {}
        for k in tasks.find({"original_task_id": i}, 
                            {"deformation_matrix":1,
                             "calculations.output":1,
                             "state":1, "task_id":1}):
            defo = k['deformation_matrix']
            d_ind = np.nonzero(defo - np.eye(3))
            delta = Decimal((defo - np.eye(3))[d_ind][0])
            # Normal deformation
            if d_ind[0] == d_ind[1]:
                dtype = "_".join(["d", str(d_ind[0][0]), 
                                  "{:.0e}".format(delta)])
            # Shear deformation
            else:
                dtype = "_".join(["s", str(d_ind[0] + d_ind[1]),
                                  "{:.0e}".format(delta)])
            sm = IndependentStrain(defo)
            d["deformation_tasks"][dtype] = {"state" : k["state"],
                                             "deformation_matrix" : defo,
                                             "strain" : sm.tolist(),
                                             "task_id": k["task_id"]}
            if k["state"] == "successful":
                st = Stress(k["calculations"][-1]["output"] \
                            ["ionic_steps"][-1]["stress"])
                ss_dict[sm] = st
        d["snl"] = o["snl"]
        if "run_tags" in o.keys():
            d["run_tags"] = o["run_tags"]
            for tag in o["run_tags"]:
                if isinstance(tag, dict):
                    if "input_id" in tag.keys():
                        d["input_mp_id"] = tag["input_id"]
        d["snl_final"] = o["snl_final"]
        d["pretty_formula"] = o["pretty_formula"]

        # Old input mp-id style
        if o["snl"]["about"].get("_mp_id"):
            d["material_id"] = o["snl"]["about"]["_mp_id"]

        # New style
        elif "input_mp_id" in d:
            d["material_id"] = d["input_mp_id"]
        else:
            d["material_id"] = None
        d["relaxation_task_id"] = i

        calc_struct = Structure.from_dict(o["snl_final"])
        # TODO:
        # JHM: This test is unnecessary at the moment, but should be redone
        """
        conventional = is_conventional(calc_struct)
        if conventional:
            d["analysis"]["is_conventional"] = True
        else:
            d["analysis"]["is_conventional"] = False
        """
        d["spacegroup"]=o.get("spacegroup", "Unknown")
        
        if ndocs >= 20:
            # Perform Elastic tensor fitting and analysis
            result = ElasticTensor.from_stress_dict(ss_dict)
            d["elastic_tensor"] = result.tolist()
            kg_average = result.kg_average
            d.update({"K_Voigt":kg_average[0], "G_Voigt":kg_average[1], 
                      "K_Reuss":kg_average[2], "G_Reuss":kg_average[3], 
                      "K_Voigt_Reuss_Hill":kg_average[4], 
                      "G_Voigt_Reuss_Hill":kg_average[5]})
            d["universal_anisotropy"] = result.universal_anisotropy
            d["homogeneous_poisson"] = result.homogeneous_poisson
            if ndocs < 24:
                d["warning"].append("less than 24 tasks completed")

            # Perform filter checks
            symm_t = result.symmetrized
            d["symmetrized_tensor"] = symm_t.tolist()
            d["analysis"]["not_rare_earth"] = True
            for s in calc_struct.species:
                if s.is_rare_earth_metal:
                    d["analysis"]["not_rare_earth"] = False
            eigvals = np.linalg.eigvals(symm_t)
            eig_positive = np.all((eigvals > 0) & np.isreal(eigvals))
            d["analysis"]["eigval_positive"] = bool(eig_positive) 
            c11 = symm_t[0][0]
            c12 = symm_t[0][1]
            c13 = symm_t[0][2]
            c23 = symm_t[1][2]
            d["analysis"]["c11_c12"]= not (abs((c11-c12)/c11) < 0.05
                                           or c11 < c12)
            d["analysis"]["c11_c13"]= not (abs((c11-c13)/c11) < 0.05 
                                           or c11 < c13)
            d["analysis"]["c11_c23"]= not (abs((c11-c23)/c11) < 0.1 
                                           or c11 < c23)
            d["analysis"]["K_R"] = not (d["K_Reuss"] < 2)
            d["analysis"]["G_R"] = not (d["G_Reuss"] < 2)
            d["analysis"]["K_V"] = not (d["K_Voigt"] < 2)
            d["analysis"]["G_V"] = not (d["G_Voigt"] < 2)
            filter_state = np.all(d["analysis"].values())
            d["analysis"]["filter_pass"] = bool(filter_state)
            d["analysis"]["eigval"] = list(eigvals)

            # TODO:
            # JHM: eventually we can reintroduce the IEEE conversion
            #       but as of now it's not being used, and it should
            #       be in pymatgen
            """
            # IEEE Conversion
            try:
                ieee_tensor = IEEE_conversion.get_ieee_tensor(struct_final, result)
                d["elastic_tensor_IEEE"] = ieee_tensor[0].tolist()
                d["analysis"]["IEEE"] = True
            except Exception as e:
                d["elastic_tensor_IEEE"] = None
                d["analysis"]["IEEE"] = False
                d["error"].append("Unable to get IEEE tensor: {}".format(e))
            """
            # Add thermal properties
            nsites = calc_struct.num_sites
            volume = calc_struct.volume
            natoms = calc_struct.composition.num_atoms
            weight = calc_struct.composition.weight
            num_density = 1e30 * nsites / volume
            mass_density = 1.6605e3 * nsites * volume * weight / \
                           (natoms * volume)
            tot_mass = sum([e.atomic_mass for e in calc_struct.species])
            avg_mass =  1.6605e-27 * tot_mass / natoms
            y_mod = 9e9 * result.k_vrh * result.g_vrh / \
                    (3. * result.k_vrh * result.g_vrh)
            trans_v = 1e9 * result.k_vrh / mass_density**0.5
            long_v = 1e9 * result.k_vrh + \
                     4./3. * result.g_vrh / mass_density**0.5
            clarke = 0.87 * 1.3806e-23 * avg_mass**(-2./3.) * \
                     mass_density**(1./6.) * y_mod**0.5
            cahill = 1.3806e-23 / 2.48 * num_density**(2./3.) * long_v + \
                     2 * trans_v
            snyder_ac = 0.38483 * avg_mass * \
                        (long_v + 2./3.*trans_v)**3. / \
                        (300. * num_density**(-2./3.) * nsites**(1./3.))
            snyder_opt = 1.66914e-23 * (long_v + 2./3.*trans_v) / \
                         num_density**(-2./3.) * \
                         (1 - nsites**(-1./3.))
            snyder_total = snyder_ac + snyder_opt
            debye = 2.489e-11 * avg_mass**(-1./3.) * \
                    mass_density**(-1./6.) * y_mod**0.5

            d["thermal"]={"num_density" : num_density,
                          "mass_density" : mass_density,
                          "avg_mass" : avg_mass,
                          "num_atom_per_unit_formula" : natoms,
                          "youngs_modulus" : y_mod,
                          "trans_velocity" : trans_v,
                          "long_velocity" : long_v,
                          "clarke" : clarke,
                          "cahill" : cahill,
                          "snyder_acou_300K" : snyder_ac,
                          "snyder_opt" : snyder_opt,
                          "snyder_total" : snyder_total,
                          "debye": debye
                         }
        else:
            d['state'] = "Fewer than 20 successful tasks completed"
            return FWAction()

        if o["snl"]["about"].get("_kpoint_density"):
            d["kpoint_density"]= o["snl"]["about"].get("_kpoint_density")

        if d["error"]:
            raise ValueError("Elastic analysis failed: {}".format(d["error"]))
        elif d["analysis"]["filter_pass"]:
            d["state"] = "successful"
        else:
            d["state"] = "filter_failed"
        elasticity.update({"relaxation_task_id": d["relaxation_task_id"]}, 
                           d, upsert=True)
        return FWAction()
