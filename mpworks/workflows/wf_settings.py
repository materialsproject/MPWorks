__author__ = 'Anubhav Jain'
__copyright__ = 'Copyright 2013, The Materials Project'
__version__ = '0.1'
__maintainer__ = 'Anubhav Jain'
__email__ = 'ajain@lbl.gov'
__date__ = 'May 10, 2013'

QA_VASP = {'nnodes': 2}  # don't change nnodes unless other parts of code are also changed
QA_VASP_SMALL = {'nnodes': 2, 'walltime': '72:00:00'}  # small walltime jobs
QA_DB = {'nnodes': 1, 'walltime': '1:00:00',
         'pre_rocket': '#PBS -V\nulimit -v hard\nmodule load python/2.7.3\nsource $VENV_LOC'}
QA_CONTROL = {'nnodes': 1, 'walltime': '00:30:00'}
MOVE_TO_GARDEN_DEV = False
MOVE_TO_GARDEN_PROD = False

RUN_LOCS = ['/project/projectdirs/matgen/garden/',
            '/project/projectdirs/matgen/garden/dev',
            '/project/projectdirs/matgen/garden/control_blocks',
            '/global/scratch/sd/matcomp/',
            '/scratch/scratchdirs/matcomp/', '/scratch2/scratchdirs/matcomp/',
            '/global/scratch/sd/matcomp/aj_tests/',
            '/global/scratch/sd/matcomp/wc_tests/',
            '/global/scratch/sd/matcomp/aj_prod/',
            '/global/scratch2/sd/matcomp/mp_prod']
