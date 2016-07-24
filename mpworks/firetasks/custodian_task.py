from gzip import GzipFile
import logging
import socket

from fireworks.fw_config import FWData
from monty.os.path import which
from custodian.vasp.handlers import VaspErrorHandler, NonConvergingErrorHandler, \
    FrozenJobErrorHandler, MeshSymmetryErrorHandler, PositiveEnergyErrorHandler
from custodian.vasp.validators import VasprunXMLValidator
from fireworks.core.firework import FireTaskBase, FWAction
from fireworks.utilities.fw_serializers import FWSerializable
from custodian.custodian import Custodian
from custodian.vasp.jobs import VaspJob
import shlex
import os
from fireworks.utilities.fw_utilities import get_slug
from mpworks.workflows.wf_utils import j_decorate, ScancelJobStepTerminator
from pymatgen.io.vasp.inputs import Incar
from monty.json import MontyDecoder

__author__ = 'Anubhav Jain'
__copyright__ = 'Copyright 2013, The Materials Project'
__version__ = '0.1'
__maintainer__ = 'Anubhav Jain'
__email__ = 'ajain@lbl.gov'
__date__ = 'Mar 15, 2013'


def check_incar(task_type):
    errors = []
    incar = Incar.from_file("INCAR")

    if 'deformed' in task_type:
        if incar['ISIF'] != 2:
            errors.append("Deformed optimization requires ISIF = 2")

    if 'static' in task_type or 'Uniform' in task_type or 'band structure' in task_type:
        if incar["IBRION"] != -1:
            errors.append("IBRION should be -1 for non structure optimization runs")

        if "NSW" in incar and incar["NSW"] != 0:
            errors.append("NSW must be 0 for non structure optimization runs")

    if 'static' in task_type and not incar.get("LCHARG", True):
            errors.append("LCHARG must be True for static runs")

    if 'Uniform' in task_type and incar["ICHARG"] != 11:
        errors.append("ICHARG must be 11 for Uniform runs")

    if 'band structure' in task_type and incar["ICHARG"] != 11:
        errors.append("ICHARG must be 11 for band structure runs")

    if 'GGA+U' in task_type:
        # check LDAU
        if not incar["LDAU"]:
            errors.append("GGA+U requires LDAU parameter")

        if not incar["LMAXMIX"] >= 4:
            errors.append("GGA+U requires LMAXMIX >= 4")

        if not sum(incar["LDAUU"]) > 0:
            errors.append("GGA+U requires sum(LDAUU)>0")

    return errors


class VaspCustodianTask(FireTaskBase, FWSerializable):
    _fw_name = "Vasp Custodian Task"

    def __init__(self, parameters):
        self.update(parameters)
        self.jobs = self['jobs']
        dec = MontyDecoder()
        self.handlers = map(dec.process_decoded, self['handlers'])
        self.max_errors = self.get('max_errors', 1)
        self.gzip_output = self.get('gzip_output', True)

    def run_task(self, fw_spec):

        # write a file containing the formula and task_type for somewhat
        # easier file system browsing
        self._write_formula_file(fw_spec)

        fw_env = fw_spec.get("_fw_env", {})

        if "mpi_cmd" in fw_env:
            mpi_cmd = fw_spec["_fw_env"]["mpi_cmd"]
        elif which("mpirun"):
            mpi_cmd = "mpirun"
        elif which("aprun"):
            mpi_cmd = "aprun"
        else:
            raise ValueError("No MPI command found!")

        # TODO: last two env vars, i.e. SGE and LoadLeveler, are untested
        env_vars = ['PBS_NP', 'SLURM_NTASKS', 'NSLOTS', 'LOADL_TOTAL_TASKS']
        nproc = None
        for env_var in env_vars:
            nproc = os.environ.get(env_var, None)
            if nproc is not None:
                break
        if nproc is None:
            raise ValueError("None of the env vars {} found to set nproc!".format(env_vars))

        fw_data = FWData()
        if (not fw_data.MULTIPROCESSING) or (fw_data.NODE_LIST is None):
            if "srun" in mpi_cmd:
                mpi_cmd += " -v"
            v_exe = shlex.split('{} -n {} {}'.format(mpi_cmd, nproc, fw_env.get("vasp_cmd", "vasp")))
            gv_exe = shlex.split('{} -n {} {}'.format(mpi_cmd, nproc, fw_env.get("gvasp_cmd", "gvasp")))
        else:
            v_exe, gv_exe = self._get_vasp_cmd_in_job_packing(fw_data, fw_env, mpi_cmd)

        print('host:', os.environ['HOSTNAME'])

        stderr_file = "std_err.txt"
        for job in self.jobs:
            job.vasp_cmd = v_exe
            job.gamma_vasp_cmd = gv_exe
            job.stderr_file = stderr_file
        if v_exe[0] == "srun":
            scancel_terminator = ScancelJobStepTerminator(stderr_file)
            terminate_func = scancel_terminator.cancel_job_step
        else:
            terminate_func = None

        incar_errors = check_incar(fw_spec['task_type'])
        if incar_errors:
            raise ValueError("Critical error: INCAR does not pass checks: {}".format(incar_errors))

        logging.basicConfig(level=logging.DEBUG)

        c = Custodian(self.handlers, self.jobs, max_errors=self.max_errors, gzipped_output=False,
                      validators=[VasprunXMLValidator()],
                      terminate_func=terminate_func)  # manual gzip
        custodian_out = c.run()

        if self.gzip_output:
            for f in os.listdir(os.getcwd()):
                if not f.lower().endswith("gz") and not f.endswith(".OU") and not f.endswith(".ER"):
                    with open(f, 'rb') as f_in, \
                            GzipFile('{}.gz'.format(f), 'wb') as f_out:
                        f_out.writelines(f_in)
                    os.remove(f)

        all_errors = set()
        for run in custodian_out:
            for correction in run['corrections']:
                all_errors.update(correction['errors'])

        stored_data = {'error_list': list(all_errors)}
        update_spec = {'prev_vasp_dir': os.getcwd(),
                       'prev_task_type': fw_spec['task_type'],
                       'mpsnl': fw_spec['mpsnl'],
                       'snlgroup_id': fw_spec['snlgroup_id'],
                       'run_tags': fw_spec['run_tags'],
                       'parameters': fw_spec.get('parameters')}

        return FWAction(stored_data=stored_data, update_spec=update_spec)

    @staticmethod
    def _get_vasp_cmd_in_job_packing(fw_data, fw_env, mpi_cmd):
        tasks_per_node_flag = {"srun": "--ntasks-per-node",
                               "mpirun": "--npernode",
                               "aprun": "-N"}
        nodelist_flag = {"srun": "--nodelist",
                         "mpirun": "--host",
                         "aprun": "-L"}
        ranks_num_flag = {"srun": "--ntasks",
                          "mpirun": "-n",
                          "aprun": "-n"}
        nodes_spec = {"srun": "--nodes {}".format(len(fw_data.NODE_LIST)),
                      "mpirun": "",
                      "aprun": ""}
        verbose_flag = {"srun": "-v",
                        "mpirun": "",
                        "aprun": ""}
        mpirun = mpi_cmd.split()[0]
        fw_data = FWData()
        #  Don't honor the SLURM_NTASKS in case of job packing, Because SLURM_NTASKS is referring
        #  to total number of processes of the parent job
        sub_nproc = fw_data.SUB_NPROCS
        vasp_cmds = [fw_env.get("vasp_cmd", "vasp"), fw_env.get("gvasp_cmd", "gvasp")]
        vasp_exes = [shlex.split('{mpi_cmd} {verbose_flag} {nodes_spec} {ranks_flag} {nproc} {tpn_flag} {tpn} '
                                 '{nl_flag} {nl} {vasp_cmd}'.
                                 format(mpi_cmd=mpi_cmd,
                                        verbose_flag=verbose_flag,
                                        nodes_spec=nodes_spec[mpirun],
                                        ranks_flag=ranks_num_flag[mpirun],
                                        nproc=sub_nproc,
                                        tpn_flag=tasks_per_node_flag[mpirun],
                                        tpn=int(fw_data.SUB_NPROCS) / len(fw_data.NODE_LIST),
                                        nl_flag=nodelist_flag[mpirun],
                                        nl=','.join(fw_data.NODE_LIST),
                                        vasp_cmd=vasp_cmd))
                     for vasp_cmd in vasp_cmds]
        v_exe, gv_exe = vasp_exes
        return v_exe, gv_exe

    @staticmethod
    def _write_formula_file(fw_spec):
        filename = get_slug(
            'JOB--' + fw_spec['mpsnl'].structure.composition.reduced_formula +
            '--' + fw_spec['task_type'])
        with open(filename, 'w+') as f:
            f.write('')


def get_custodian_task(spec):
    task_type = spec['task_type']
    v_exe = 'VASP_EXE'  # will be transformed to vasp executable on the node
    handlers = [VaspErrorHandler(), FrozenJobErrorHandler(),
                MeshSymmetryErrorHandler(), NonConvergingErrorHandler(), PositiveEnergyErrorHandler()]

    if 'optimize structure (2x)' in task_type:
        jobs = VaspJob.double_relaxation_run(v_exe)
    elif 'static' in task_type or 'deformed' in task_type:
        jobs = [VaspJob(v_exe)]
    else:
        # non-SCF runs
        jobs = [VaspJob(v_exe)]
        handlers = []

    params = {'jobs': [j_decorate(j.as_dict()) for j in jobs],
              'handlers': [h.as_dict() for h in handlers], 'max_errors': 5}

    return VaspCustodianTask(params)
