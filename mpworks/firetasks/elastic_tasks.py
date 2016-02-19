from monty.os.path import zpath

__author__ = 'Wei Chen'
__credits__ = 'Joseph Montoya'
__maintainer__ = 'Joseph Montoya'


from fireworks.utilities.fw_serializers import FWSerializable
from fireworks.core.firework import FireTaskBase, FWAction
from pymatgen.io.vasp.inputs import Incar, Poscar
from pymatgen.analysis.elasticity.strain import DeformedStructureSet
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
        k = [int(round(2.5*k)) if int(round(2.5*k))%2 else int(round(2.5*k))+1 for k in kpoints['kpoints'][0]]
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
            fws.append(Firework([VaspToDBTask()], 
                                spec, 
                                name=get_slug(f + '--' + spec['task_type']), 
                                fw_id=-998+i*10))
            connections[-999+i*10] = [-998+i*10]
            wf.append(Workflow(fws, connections))
        return FWAction(additions=wf)

class AddElasticDataToDB(FireTaskBase, FWSerializable):
    _fw_name = "Add Elastic Data to DB"

    def run_task(self):
        db_dir = os.environ['DB_LOC']
        db_path = os.path.join(db_dir, 'tasks_db.json')

        with open(db_path) as f:
            db_creds = json.load(f)
            connection = MongoClient(creds['host'], creds['port'])
            tdb = connection[creds['database']]
            tdb.authenticate(creds['admin_use'], creds['admin_password'])
            ndocs = tasks.find({"original_task_id": i, 
                                "state":"successful"}).count()
            existing_doc = elasticity.find_one({"relaxation_task_id" : i})
            if existing_doc:
                print "Updating: " + i
            else:
                print "New material: " + i
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
                # Normal deformation
                if d_ind[0] == d_ind[1]:
                    dtype = "_".join(["d", str(d_ind[0][0]), 
                                      "{:.0e}".format(Decimal((defo - np.eye(e))[d_ind][0]))])
                # Shear deformation
                else:
                    dtype = "_".join(["s", str(d_ind[0] + d_ind[1]),
                                      "{:.0e}".format(Decimal((defo - np.eye(e))[d_ind][0]))])

                d["deformation_tasks"][dtype] = {"state" : k["state"],
                                                 "deformation_matrix" : defo,
                                                 "strain" : IndependentStrain(defo),
                                                 "task_id": k["task_id"]}

                if k["state"] == "successful":
                    st = Stress(k["calculations"][-1]["output"]\
                                ["ionic_steps"][-1]["stress"]) 
                    ss_dict[sm]=st
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
            elif "input_mp_id" in d:
                d["material_id"] = d["input_mp_id"]
            else:
                d["material_id"] = None
            d["relaxation_task_id"] = i

            calc_struct=Structure.from_dict(o["snl"])

            try:
                conventional = is_conventional(calc_struct)
                if conventional:
                    d["analysis"]["is_conventional"] = True
                else:
                    d["analysis"]["is_conventional"] = False
            except:
                print i+": get conventional cell error"
                d["error"].append("Unable to analyze conventional cell")

            d["spacegroup"]=o.get("spacegroup", "Unknown")

            try:
                e_above_hull = db2.tasks.find_one({"task_id":o["snl"]["about"]["_mp_id"]},
                                              {"analysis.e_above_hull":1})["analysis"]["e_above_hull"]
                    d["e_above_hull"] = e_above_hull
                except:
                    d["e_above_hull"] = "Unknown"

                if ndocs>=20:
                    try:
                        result = ElasticTensor.from_stress_dict(ss_dict)
                        d["elastic_tensor"] = result.tolist()
                        kg_average = result.kg_average
                        d.update({"K_Voigt":kg_average[0], "G_Voigt":kg_average[1], "K_Reuss":kg_average[2],
                                  "G_Reuss":kg_average[3], "K_Voigt_Reuss_Hill":kg_average[4], "G_Voigt_Reuss_Hill":kg_average[5]})
                        d["universal_anisotropy"] = result.universal_anisotropy
                        d["homogeneous_poisson"] = result.homogeneous_poisson
                        if ndocs == 24:
                            pass
                        else:
                            d["warning"].append("less than 24 tasks completed")
                    except:
                        print i+": determine Cij error"
                        d["error"].append("Unable to determine Cij")
                        # import pdb; pdb.set_trace()

                    #Add analysis field
                    if d.get("elastic_tensor"):
                        original_tensor = SQTensor(d["elastic_tensor"])
                        symmetrized_tensor = original_tensor.symmetrized
                        d["symmetrized_tensor"] = symmetrized_tensor.tolist()
                        d["analysis"]["not_rare_earth"] = True
                        for s in Structure.from_dict(o["snl"]).species:
                            if s.is_rare_earth_metal:
                                d["analysis"]["not_rare_earth"] = False
                        try:
                            d["elastic_tensor_IEEE"] = IEEE_conversion.get_ieee_tensor(Structure.from_dict(o["snl_final"]), d["elastic_tensor"])[0].tolist()
                            d["analysis"]["IEEE"] = True
                        except Exception as e:
                            d["elastic_tensor_IEEE"] = None
                            d["analysis"]["IEEE"] = False
                            d["error"].append("Unable to get IEEE tensor")
                            print e
                        eigvals = np.linalg.eigvals(symmetrized_tensor)
                        d["analysis"]["eigval"]=list(eigvals)
                        try:
                            eig_positive = eigvals > 0
                            eig_real = np.isreal(eigvals)
                            d["analysis"]["eigval_positive"] = bool(np.all(eig_positive & eig_real))
                            d["analysis"]["c11_c12"]= not (abs((symmetrized_tensor[0][0]-symmetrized_tensor[0][1])/symmetrized_tensor[0][0])*100<5 or
                                              symmetrized_tensor[0][0]< symmetrized_tensor[0][1])
                            d["analysis"]["c11_c13"]= not (abs((symmetrized_tensor[0][0]-symmetrized_tensor[0][2])/symmetrized_tensor[0][0])*100<5 or


