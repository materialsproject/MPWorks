"""
Firetasks related to intermetallic defect tasks
"""
__author__ = 'Bharat Medasani'

from monty.os.path import zpath
from fireworks.utilities.fw_serializers import FWSerializable
from fireworks.core.firework import FireTaskBase, FWAction
#from pymatgen.transformation.genstrain import DeformGeometry
from pymatgen.transformations.defect_transformation import \
        VacancyTransformation, AntisiteDefectTransformation
from fireworks.core.firework import FireWork, Workflow
from pymatgen.matproj.snl import StructureNL
from mpworks.workflows import snl_to_wf
from mpworks.firetasks.snl_tasks import AddSNLTask
from mpworks.snl_utils.mpsnl import MPStructureNL
from pymatgen.core.structure import Structure
from mpworks.workflows.wf_settings import QA_VASP, QA_DB, QA_VASP_SMALL
#from pymatgen.io.vaspio_set import MPVaspInputSet
from pymatgen.io.vaspio.vasp_input import Poscar, Kpoints

def  update_spec_defect_supercells(spec, user_vasp_settings=None):
    fw_spec = spec
    update_set = {"EDIFF": 1e-5, "ALGO": "N", "NPAR": 2, "ISIF": 2, 
            "EDIFFG": -1e-2}
    if user_vasp_settings and user_vasp_settings.get("incar"):
            update_set.update(user_vasp_settings["incar"])
    fw_spec['vasp']['incar'].update(update_set)
    old_struct=Poscar.from_dict(fw_spec["vasp"]["poscar"]).structure
    if user_vasp_settings and user_vasp_settings.get("kpoints"):
        kpoints_density = user_vasp_settings["kpoints"]["kpoints_density"]
    else:
        kpoints_density = 3000
    k=Kpoints.automatic_density(old_struct, kpoints_density)
    fw_spec['vasp']['kpoints'] = k.to_dict
    return fw_spec

def update_spec_bulk_supercell(spec, user_vasp_settings=None):
    fw_spec = spec
    update_set = {"IBRION": -1, "NSW": 0, "EDIFF": 1e-4, "ALGO": "N", "NPAR": 2}
    if user_vasp_settings and user_vasp_settings.get("incar"):
            update_set.update(user_vasp_settings["incar"])
    fw_spec['vasp']['incar'].update(update_set)
    old_struct=Poscar.from_dict(fw_spec["vasp"]["poscar"]).structure
    if user_vasp_settings and user_vasp_settings.get("kpoints"):
        kpoints_density = user_vasp_settings["kpoints"]["kpoints_density"]
    else:
        kpoints_density = 3000
    k=Kpoints.automatic_density(old_struct, kpoints_density)
    fw_spec['vasp']['kpoints'] = k.to_dict
    return fw_spec

'''
class SetupFConvergenceTask(FireTaskBase, FWSerializable):
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
'''

def get_sc_scale(inp_struct, final_site_no):
    lengths = inp_struct.lattice.abc
    no_sites = inp_struct.num_sites
    mult = (final_site_no/no_sites*lengths[0]*lengths[1]*lengths[2]) ** (1/3)
    num_mult = [int(round(mult/l)) for l in lengths]
    num_mult = [i if i > 0 else 1 for i in num_mult]
    return num_mult

class SetupDefectSupercellStructTask(FireTaskBase, FWSerializable):
    _fw_name = "Setup Deformed Struct Task"

    def run_task(self, fw_spec):
        # Read structure from spec
        inp_struct = Structure.from_dict(fw_spec['crystal'])
        # Generate defect supercell structures
        supercell_size_desired = fw_spec.get('supercell_size', 128)
        supercell_scaling = get_sc_scale(inp_struct, supercell_size_desired)
        bulk_supercell = inp_struct.copy().make_supercell(supercell_scaling)
        vt = VacancyTransformation(supercell_scaling)
        vac_supercells = vt.apply_transformation(inp_struct, 
                returned_ranked_list=999)
        ast = AntisiteDefectTransformation(supercell_scaling)
        antisite_supercells = ast.apply_transformation(inp_struct, 
                returned_ranked_list=999)
        supercell_defects = [bulk_supercell] + vac_supercells + \
                antisite_supercells
        wf=[]

        for i, sc in enumerate(supercell_defects):
            fws=[]
            connections={}
            d_struct = sc
            f = Composition(d_struct.formula).alphabetical_formula
            snl = StructureNL(d_struct, 'Bharat Medasani <bkmedasani@lbl.gov>', projects=["Intermetallic Defects"])

            tasks = [AddSNLTask()]
            snl_priority = fw_spec.get('priority', 1)
            spec = {
                'task_type': 'Add Defect Struct to SNL database', 'snl': snl.as_dict(),
                '_queueadapter': QA_DB, '_priority': snl_priority
            }
            if 'snlgroup_id' in fw_spec and isinstance(snl, MPStructureNL):
                spec['force_mpsnl'] = snl.as_dict()
                spec['force_snlgroup_id'] = fw_spec['snlgroup_id']
                del spec['snl']
            fws.append(Firework(tasks, spec, name=get_slug(f + '--' + spec['task_type']), fw_id=-1000+i*10))
            connections[-1000+i*10] = [-999+i*10]

            spec = snl_to_wf._snl_to_spec(snl, parameters={'exact_structure':True})
            if not i:
                spec = update_spec_bulk_supercell(spec)
                spec['task_type'] = "Static calculation of bulk supercell "
            else:
                spec = update_spec_defect_supercells(spec)
                spec['task_type'] = "Optimize defect supercell "

            #kpoints=fw_spec['vasp']['kpoints']
            #if "actual_points" in kpoints:
            #    kpoints.pop('actual_points')
            #spec['vasp']['kpoints']= kpoints
            #spec['deformation_matrix'] = strain.deformation_matrix.tolist()
            spec['original_task_id']=fw_spec["task_id"]
            spec['_priority'] = fw_spec['_priority']*2
            #Turn off dupefinder for deformed structure
            del spec['_dupefinder']

            fws.append(FireWork([VaspWriterTask(), get_custodian_task(spec)],
                                spec, name=get_slug(f + '--' + fw_spec['task_type']), fw_id=-999+i*10))

            priority = fw_spec['_priority']*3
            spec = {'task_type': 'VASP db insertion', '_priority': priority,
                    '_allow_fizzled_parents': True, '_queueadapter': QA_DB, 
                    #'elastic_constant':"deformed_structure", 
                    'clean_task_doc':True,
                    #'deformation_matrix':strain.deformation_matrix.tolist(), 
                    'original_task_id':fw_spec["task_id"]}
            fws.append(FireWork([VaspToDBTask()], spec, name=get_slug(f + '--' + spec['task_type']), fw_id=-998+i*10))
            connections[-999+i*10] = [-998+i*10]

            wf.append(Workflow(fws, connections))
        return FWAction(additions=wf)

