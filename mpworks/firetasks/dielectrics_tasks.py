from monty.os.path import zpath

__author__ = 'Ioannis Petousis'

from fireworks.utilities.fw_serializers import FWSerializable
from fireworks.core.firework import FireTaskBase, FWAction
from fireworks.core.firework import Firework, Workflow
from mpworks.firetasks.vasp_io_tasks import VaspWriterTask, VaspToDBTask
from mpworks.firetasks.custodian_task import get_custodian_task
from mpworks.firetasks.raman_tasks import SetupRamanTask
from fireworks.utilities.fw_utilities import get_slug
from pymatgen import Composition
from pymatgen.matproj.snl import StructureNL
from mpworks.workflows import snl_to_wf
from mpworks.firetasks.snl_tasks import AddSNLTask
from mpworks.snl_utils.mpsnl import MPStructureNL
from pymatgen.core.structure import Structure
from mpworks.workflows.wf_settings import QA_VASP, QA_DB, QA_VASP_SMALL, QA_CONTROL
from pymatgen.io.vasp.inputs import Poscar, Kpoints, Incar
from pymatgen.io.vasp.sets import MPStaticDielectricDFPTVaspInputSet


class SetupDFPTDielectricsTask(FireTaskBase, FWSerializable):
    _fw_name = "Setup DFPT Dielectrics Task"

    def run_task(self, fw_spec):
        # Read structure from previous relaxation
        previous_dir = fw_spec['_job_info'][-1]
        if os.path.isfile(prev_dir+"POSCAR"):
            filename = "POSCAR"
        else:
            filename = "POSCAR.gz"
        relaxed_struct = Structure.from_file(previous_dir+filename)
        # relaxed_struct = Structure.from_dict(fw_spec['output']['crystal'])
        # Generate deformed structures
        #deformed_structs = DeformGeometry(relaxed_struct, ns=0.06)
        wf=[]

        #for i, strain in enumerate(deformed_structs.keys()):
        fws=[]
        connections={}
        #d_struct = deformed_structs[strain]
        f = Composition(relaxed_struct.formula).alphabetical_formula
        snl = StructureNL(relaxed_struct, 'Ioannis Petousis <petousis@stanford.edu>',projects=["Static Dielectrics", "force_convergence"])

        tasks = [AddSNLTask()]
        snl_priority = fw_spec.get('priority', 1)
        spec = {'task_type': 'Add F-relaxed Struct to SNL database', 'snl': snl.as_dict(), '_queueadapter': QA_DB, '_priority': snl_priority}
        if 'snlgroup_id' in fw_spec and isinstance(snl, MPStructureNL):
            spec['static_dielectrics_mpsnl'] = snl.as_dict()
            spec['static_dielectrics_snlgroup_id'] = fw_spec['snlgroup_id']
            del spec['snl']
        fws.append(Firework(tasks, spec, name=get_slug(f + '--' + spec['task_type']), fw_id=10))
        connections[10] = [11]

        #incar = Incar.from_file(zpath("INCAR"))
        #ediff = incar['EDIFF']
        #encut = incar['ENCUT']
        #ediff = fw_spec['vasp']['incar']['EDIFF']
        #encut = fw_spec['vasp']['incar']['ENCUT']
        spec = snl_to_wf._snl_to_spec(snl, parameters={'exact_structure':True})
        mpvis = MPStaticDielectricDFPTVaspInputSet()
        incar = mpvis.get_incar(snl.structure)
        incar.update({"EDIFF":"1.0E-6", "ENCUT":"600", "NWRITE":"3"})
        kpoints_density = 3000
        k=Kpoints.automatic_density(snl.structure, kpoints_density, force_gamma=True)
        spec['vasp']['incar'] = incar.as_dict()
        spec['vasp']['kpoints'] = k.as_dict()
        #kpoints=fw_spec['vasp']['kpoints']
        #kpoints = Kpoints.from_file(zpath("KPOINTS"))
        #if "actual_points" in kpoints:
        #    kpoints.pop('actual_points')
        #spec['vasp']['kpoints']= kpoints
        #spec['deformation_matrix'] = strain.deformation_matrix.tolist()
        #spec['original_task_id']=fw_spec["task_id"]

        # spec['_priority'] = fw_spec['_priority']*2
        # del spec['_dupefinder']
        spec['task_type'] = "Static Dielectrics"
        spec["_pass_job_info"] = True
        spec['_allow_fizzled_parents'] = False
        spec['_queueadapter'] = QA_VASP
        fws.append(Firework([VaspWriterTask(), get_custodian_task(spec)], spec, name=get_slug(f + '--' + spec['task_type']), fw_id=11))

        priority = fw_spec['_priority']*3
        spec = {'task_type': 'VASP db insertion', '_priority': priority, "_pass_job_info": True, '_allow_fizzled_parents': True, '_queueadapter': QA_DB, 'dielectrics':"force_relaxed_structure"}
        fws.append(Firework([VaspToDBTask()], spec, name=get_slug(f + '--' + spec['task_type']), fw_id=12))
        connections[11] = [12]

        spec= {'task_type': 'Setup Raman Task', '_priority': priority, "_pass_job_info": True, '_allow_fizzled_parents': False, '_queueadapter': QA_CONTROL}
        fws.append(Firework([SetupRamanTask()], spec, name=get_slug(f + '--' + spec['task_type']), fw_id=13))
        connections[12] = [13]

        wf.append(Workflow(fws, connections))
        return FWAction(additions=wf)
