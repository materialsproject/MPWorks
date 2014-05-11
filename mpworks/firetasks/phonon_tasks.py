from monty.os.path import zpath

__author__ = 'weichen'


from fireworks.utilities.fw_serializers import FWSerializable
from fireworks.core.firework import FireTaskBase, FWAction
from pymatgen.phonons.genstrain import DeformGeometry
from fireworks.core.firework import FireWork, Workflow
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
#from pymatgen.io.vaspio_set import MPVaspInputSet
from pymatgen.io.vaspio.vasp_input import Poscar, Kpoints

def update_spec_force_convergence(spec):
    fw_spec = spec
    update_set = {"ENCUT": 700, "EDIFF": 0.000001, "ALGO":"N", "NPAR":2}
    fw_spec['vasp']['incar'].update(update_set)
    #old_struct=Structure.from_dict(fw_spec['output']['crystal'])
    old_struct=Poscar.from_dict(fw_spec["vasp"]["poscar"]).structure
    #mp_kpoints = MPVaspInputSet().get_kpoints(old_struct)
    #kpoints = mp_kpoints.to_dict
    #k = [int(round(2.2*k)) if int(round(2.2*k))%2 else int(round(2.2*k))+1 for k in kpoints['kpoints'][0]]
    k=Kpoints.automatic_density(old_struct, 7000)
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
class SetupDeformedStructTask(FireTaskBase, FWSerializable):
    _fw_name = "Setup Deformed Struct Task"

    def run_task(self, fw_spec):
        # Read structure from previous relaxation
        relaxed_struct = Structure.from_dict(fw_spec['output']['crystal'])
        # Generate deformed structures
        deformed_structs = DeformGeometry(relaxed_struct, ns=0.06)
        wf=[]

        for i, strain in enumerate(deformed_structs.keys()):
            fws=[]
            connections={}
            d_struct = deformed_structs[strain]
            f = Composition.from_formula(d_struct.formula).alphabetical_formula
            snl = StructureNL(d_struct, 'Wei Chen <weichen@lbl.gov>',projects=["Elasticity"])

            tasks = [AddSNLTask()]
            snl_priority = fw_spec.get('priority', 1)
            spec = {'task_type': 'Add Deformed Struct to SNL database', 'snl': snl.to_dict,
                    '_queueadapter': QA_DB, '_priority': snl_priority}
            if 'snlgroup_id' in fw_spec and isinstance(snl, MPStructureNL):
                spec['force_mpsnl'] = snl.to_dict
                spec['force_snlgroup_id'] = fw_spec['snlgroup_id']
                del spec['snl']
            fws.append(FireWork(tasks, spec, name=get_slug(f + '--' + spec['task_type']), fw_id=-1000+i*10))
            connections[-1000+i*10] = [-999+i*10]

            spec = snl_to_wf._snl_to_spec(snl, parameters={'exact_structure':True})
            incar=fw_spec['vasp']['incar']
            incar.update({"ISIF":2})
            spec['vasp']['incar']=incar
            kpoints=fw_spec['vasp']['kpoints']
            if "actual_points" in kpoints:
                kpoints.pop('actual_points')
            spec['vasp']['kpoints']= kpoints
            spec['deformation_matrix'] = strain.deformation_matrix.tolist()
            spec['original_task_id']=fw_spec["task_id"]
            #Turn off dupefinder for deformed structure
            del spec['_dupefinder']

            spec['task_type'] = "Calculate deformed structure static optimize"
            fws.append(FireWork([VaspWriterTask(), get_custodian_task(spec)],
                                spec, name=get_slug(f + '--' + fw_spec['task_type']), fw_id=-999+i*10))

            priority = fw_spec['_priority']
            spec = {'task_type': 'VASP db insertion', '_priority': priority,
            '_allow_fizzled_parents': True, '_queueadapter': QA_DB, 'elastic_constant':"deformed_structure", 'clean_task_doc':True,
            'deformation_matrix':strain.deformation_matrix.tolist(), 'original_task_id':fw_spec["task_id"]}
            fws.append(FireWork([VaspToDBTask()], spec, name=get_slug(f + '--' + spec['task_type']), fw_id=-998+i*10))
            connections[-999+i*10] = [-998+i*10]

            wf.append(Workflow(fws, connections))
        return FWAction(additions=wf)

