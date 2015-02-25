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



class SetupDFPTDielectricsTask(FireTaskBase, FWSerializable):
    _fw_name = "Setup DFPT Dielectrics Task"

    def run_task(self, fw_spec):
        # Read structure from previous relaxation
        relaxed_struct = Structure.from_dict(fw_spec['output']['crystal'])
        # Generate deformed structures
        #deformed_structs = DeformGeometry(relaxed_struct, ns=0.06)
        wf=[]

        for i, strain in enumerate(deformed_structs.keys()):
            fws=[]
            connections={}
            #d_struct = deformed_structs[strain]
            f = Composition.from_formula(relaxed_struct.formula).alphabetical_formula
            snl = StructureNL(relaxed_struct, 'Ioannis Petousis <petousis@stanford.edu>',projects=["Static Dielectrics", "force_convergence"])

            tasks = [AddSNLTask()]
            snl_priority = fw_spec.get('priority', 1)
            spec = {'task_type': 'Add F-relaxed Struct to SNL database', 'snl': snl.to_dict,
                    '_queueadapter': QA_DB, '_priority': snl_priority}
            if 'snlgroup_id' in fw_spec and isinstance(snl, MPStructureNL):
                spec['static_dielectrics_mpsnl'] = snl.to_dict
                spec['static_dielectrics_snlgroup_id'] = fw_spec['snlgroup_id']
                del spec['snl']
            fws.append(FireWork(tasks, spec, name=get_slug(f + '--' + spec['task_type']), fw_id=-1000))
            connections[-1000] = [-999]

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
            spec['_priority'] = fw_spec['_priority']*2
            #Turn off dupefinder for deformed structure
            del spec['_dupefinder']

            spec['task_type'] = "Calculate deformed structure static optimize"
            fws.append(FireWork([VaspWriterTask(), get_custodian_task(spec)],
                                spec, name=get_slug(f + '--' + fw_spec['task_type']), fw_id=-999+i*10))

            priority = fw_spec['_priority']*3
            spec = {'task_type': 'VASP db insertion', '_priority': priority,
            '_allow_fizzled_parents': True, '_queueadapter': QA_DB, 'elastic_constant':"deformed_structure", 'clean_task_doc':True,
            'deformation_matrix':strain.deformation_matrix.tolist(), 'original_task_id':fw_spec["task_id"]}
            fws.append(FireWork([VaspToDBTask()], spec, name=get_slug(f + '--' + spec['task_type']), fw_id=-998+i*10))
            connections[-999+i*10] = [-998+i*10]

            wf.append(Workflow(fws, connections))
        return FWAction(additions=wf)
