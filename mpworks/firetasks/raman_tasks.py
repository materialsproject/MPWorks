from monty.os.path import zpath

__author__ = 'Ioannis Petousis, Kevin Hong Ding'


from fireworks.utilities.fw_serializers import FWSerializable
from fireworks.core.firework import FireTaskBase, FWAction
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
from mpworks.workflows.wf_settings import QA_VASP, QA_DB, QA_VASP_SMALL, QA_CONTROL
from pymatgen.io.vaspio.vasp_input import Poscar, Kpoints, Incar
from pymatgen.io.vaspio_set import MPStaticDielectricDFPTVaspInputSet
from raman_functions import get_modes_from_OUTCAR, get_mass_list, get_nat_type


class SetupRamanTask(FireTaskBase, FWSerializable):
    _fw_name = "Setup Raman Task"

    def run_task(self, fw_spec):
        # Read structure from previous dielectric run
        previous_dir = fw_spec['_job_info'][-1]
        if os.path.isfile(prev_dir+"POSCAR"):
            filename = "POSCAR"
        else:
            filename = "POSCAR.gz"
        relaxed_struct = Structure.from_file(previous_dir+filename)
        # relaxed_struct = Structure.from_dict(fw_spec['output']['crystal'])
        nat = relaxed_struct.num_sites
        ntype = relaxed_struct.ntypesp
        nat_type = get_nat_type(relaxed_struct)
        mass_map = get_mass_list(relaxed_struct)
        step_size = 0.01
        if os.path.isfile(previous_dir+"OUTCAR"):
            filename = "OUTCAR"
        else:
            filename = "OUTCAR.gz"
        with open(previous_dir+filename, 'r') as outcar_fh:
            eigvals, eigvecs, norms = get_modes_from_OUTCAR(outcar_fh,nat,mass_map)

        raman_count = 0
        for mode in range(len(eigvals)):
            eigval = eigvals[mode]
            eigvec = eigvecs[mode]
            norm = norms[mode]
            for disp in [-1, 1]:
                new_struct = relaxed_struct.copy()
                for site in range(nat):
                    translation_vector = eigvec[site]*step_size*disp/norm
                    new_struct.translate_sites([site], translation_vector)

                spec = snl_to_wf._snl_to_spec(snl, parameters=parameters)
                mpvis = MPStaticDielectricDFPTVaspInputSet()
                incar = mpvis.get_incar(snl.structure)
                incar.update({"EDIFF":"1.0E-6", "ENCUT":"600", "NWRITE":"3"})
                spec['vasp']['incar'] = incar.as_dict()
                kpoints_density = 3000
                k=Kpoints.automatic_density(snl.structure, kpoints_density, force_gamma=True)
                spec['vasp']['kpoints'] = k.as_dict()
                spec['task_type'] = "Raman_"+str(mode+1)+"."+str(disp)
                spec["_pass_job_info"] = True
                spec['_allow_fizzled_parents'] = False
                spec['_queueadapter'] = QA_VASP
                fws.append(Firework([VaspWriterTask(), get_custodian_task(spec)], spec, name=get_slug(f + '--' + spec['task_type']), fw_id=100+raman_count))
                priority = fw_spec['_priority']
                spec = {'task_type': 'VASP db insertion', '_priority': priority, "_pass_job_info": True, '_allow_fizzled_parents': True, '_queueadapter': QA_DB}
                fws.append(Firework([VaspToDBTask()], spec, name=get_slug(f + '--' + spec['task_type']), fw_id=100+raman_count+1))
                connections[100+raman_count] = [100+raman_count+1]
                connections[100+raman_count+1] = -1
                raman_count += 2

        passed_vars = [eigvals, eigvecs, norms]

        spec= {'task_type': 'Setup Raman Verification Task', '_priority': priority, "_pass_job_info": True, '_allow_fizzled_parents': False, '_queueadapter': QA_CONTROL}
        spec['passed_vars'] = []
        fws.append(Firework([SetupRamanVerificationTask()], spec, name=get_slug(f + '--' + spec['task_type']), fw_id=-1))

        wf.append(Workflow(fws, connections))
        return FWAction(additions=wf, stored_data={'passed_vars': passed_vars}, mod_spec=[{'_push': {'passed_vars': passed_vars}}])


class SetupRamanVerificationTask(FireTaskBase, FWSerializable):
    _fw_name = "Setup Raman Verification Task"

    def run_task(self, fw_spec):
        from math import pi

        passed_vars = fw_spec['passed_vars'][0]

        num_of_eigvals = len(passed_vars[0])
        initial_calc_index = -2*num_of_eigvals - 1
        step_size = 0.01
        inital_dir = fw_spec['_job_info'][initial_calc_index]
        if os.path.isfile(inital_dir+"POSCAR"):
            filename = "POSCAR"
        else:
            filename = "POSCAR.gz"
        relaxed_struct = Structure.from_file(initial_dir+filename)
        vol = relaxed_struct.volume
        activity_max = -float("Inf")
        raman_results = []
        ii = 0
        for mode in range(num_of_eigvals):
            eigenval = passed_vars[0][mode]
            eigenvec = passed_vars[1][mode]
            norm = passed_vars[2][mode]
            ra = [[0.0 for x in range(3)] for y in range(3)]
            for coeff in [-0.5, 0.5]:
                parent_index = -4*num_of_eigvals - 2*ii
                previous_dir = fw_spec['_job_info'][parent_index]
                if os.path.isfile(previous_dir+"OUTCAR"):
                    filename = "OUTCAR"
                else:
                    filename = "OUTCAR.gz"
                with open(previous_dir+filename, 'r') as outcar_fh:
                    epsilon = get_epsilon_from_OUTCAR(outcar_fh)
                for m in range(3):
                    for n in range(3):
                        ra[m][n] += epsilon[m][n] * coeff/step_size * norm * vol/(4.0*pi)


            alpha = (ra[0][0] + ra[1][1] + ra[2][2])/3.0
            beta2 = ( (ra[0][0] - ra[1][1])**2 + (ra[0][0] - ra[2][2])**2 + (ra[1][1] - ra[2][2])**2 + 6.0 * (ra[0][1]**2 + ra[0][2]**2 + ra[1][2]**2) )/2.0
            activity = 45.0*alpha**2 + 7.0*beta2
            raman_results.append([alpha, beta2, activity])
            if activity > activity_max:
                activity_max = activity
                eigenval_max = eigenval
                eigenvec_max = eigenvec
                norm_max = norm
                max_mode_index = mode
            ii += 1


        step_size = 0.005
        raman_count = 0
        for disp in [-1, 1]:
            new_struct = relaxed_struct.copy()
            for site in range(nat):
                translation_vector = eigvec_max[site]*step_size*disp/norm_max
                new_struct.translate_sites([site], translation_vector)

            spec = snl_to_wf._snl_to_spec(snl, parameters=parameters)
            mpvis = MPStaticDielectricDFPTVaspInputSet()
            incar = mpvis.get_incar(snl.structure)
            incar.update({"EDIFF":"1.0E-6", "ENCUT":"600", "NWRITE":"3"})
            spec['vasp']['incar'] = incar.as_dict()
            kpoints_density = 3000
            k=Kpoints.automatic_density(snl.structure, kpoints_density, force_gamma=True)
            spec['vasp']['kpoints'] = k.as_dict()
            spec['task_type'] = "Raman_"+str(mode+1)+"."+str(disp)
            spec["_pass_job_info"] = True
            spec['_allow_fizzled_parents'] = False
            spec['_queueadapter'] = QA_VASP
            fws.append(Firework([VaspWriterTask(), get_custodian_task(spec)], spec, name=get_slug(f + '--' + spec['task_type']), fw_id=1000+raman_count))
            priority = fw_spec['_priority']
            spec = {'task_type': 'VASP db insertion', '_priority': priority, "_pass_job_info": True, '_allow_fizzled_parents': True, '_queueadapter': QA_DB}
            fws.append(Firework([VaspToDBTask()], spec, name=get_slug(f + '--' + spec['task_type']), fw_id=1000+raman_count+1))
            connections[1000+raman_count] = [1000+raman_count+1]
            connections[1000+raman_count+1] = -10
            raman_count += 2

        passed_vars = [eigvals, eigvecs, raman_results, max_mode_index]

        spec= {'task_type': 'Verify Raman Task', '_priority': priority, "_pass_job_info": True, '_allow_fizzled_parents': False, '_queueadapter': QA_CONTROL}
        spec['passed_vars'] = []
        fws.append(Firework([VerifyRamanTask()], spec, name=get_slug(f + '--' + spec['task_type']), fw_id=-10))

        wf.append(Workflow(fws, connections))
        return FWAction(additions=wf, stored_data={'passed_vars': passed_vars}, mod_spec=[{'_push': {'passed_vars': passed_vars}}])


class VerifyRamanTask(FireTaskBase, FWSerializable):
    _fw_name = "Verify Raman Task"

    def run_task(self, fw_spec):
        from math import pi

        # num_of_eigvals = ?
        # initial_calc_index = -2*num_of_eigvals - 3
        # step_size = 0.01
        # inital_dir = fw_spec['_job_info'][initial_calc_index]
        # if os.path.isfile(inital_dir+"POSCAR"):
        #     filename = "POSCAR"
        # else:
        #     filename = "POSCAR.gz"
        # relaxed_struct = Structure.from_file(initial_dir+filename)
        # vol = relaxed_struct.volume
        # "get eigenvalue, eigenvector and norm for max activity mode from initial calculation"
        #
        # "For the 0.001 step_size calculation:"
        # step_size = 0.01
        # ra = [[0.0 for x in range(3)] for y in range(3)]
        # for coeff in [-0.5, 0.5]:
        #     "get epsilon for the 2 calculaitons"
        #     for m in range(3):
        #         for n in range(3):
        #             ra[m][n] += epsilon[m][n] * coeff/step_size * norm * vol/(4.0*pi)
        #
        # alpha = (ra[0][0] + ra[1][1] + ra[2][2])/3.0
        # beta2 = ( (ra[0][0] - ra[1][1])**2 + (ra[0][0] - ra[2][2])**2 + (ra[1][1] - ra[2][2])**2 + 6.0 * (ra[0][1]**2 + ra[0][2]**2 + ra[1][2]**2) )/2.0
        # activity = 45.0*alpha**2 + 7.0*beta2

        passed_vars = fw_spec['passed_vars'][0]

        "For the 0.005 step_size calculaiton:"
        step_size = 0.005
        ra = [[0.0 for x in range(3)] for y in range(3)]
        ii = 0
        for coeff in [-0.5, 0.5]:
            parent_index = -2*(2 - ii)
            previous_dir = fw_spec['_job_info'][parent_index]
            if os.path.isfile(previous_dir+"OUTCAR"):
                filename = "OUTCAR"
            else:
                filename = "OUTCAR.gz"
            with open(previous_dir+filename, 'r') as outcar_fh:
                epsilon = get_epsilon_from_OUTCAR(outcar_fh)
            for m in range(3):
                for n in range(3):
                    ra[m][n] += epsilon[m][n] * coeff/step_size * norm * vol/(4.0*pi)
            ii += 1

        alpha = (ra[0][0] + ra[1][1] + ra[2][2])/3.0
        beta2 = ( (ra[0][0] - ra[1][1])**2 + (ra[0][0] - ra[2][2])**2 + (ra[1][1] - ra[2][2])**2 + 6.0 * (ra[0][1]**2 + ra[0][2]**2 + ra[1][2]**2) )/2.0
        activity = 45.0*alpha**2 + 7.0*beta2

        raman_results = passed_vars[5]
        max_mode_index = passed_vars[4]
        ae = abs(raman_results[max_mode_index][2] - activity)
        are = abs(raman_results[max_mode_index][2] - activity) / raman_results[max_mode_index][2]
        if ae < 3 or are < 0.1:
            # Insert to database:
            d = {}
            d['eigvalues'] = passed_vars[0]
            d['eigvectors'] = passed_vars[1]
            d['norms'] = passed_vars[2]
            d['alpha'] = raman_results[0]
            d['beta2'] = raman_results[1]
            d['activity'] = raman_results[2]
