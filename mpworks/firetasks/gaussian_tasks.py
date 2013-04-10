import subprocess
from fireworks.core.firework import FireTaskBase
from fireworks.utilities.fw_serializers import FWSerializable
from pymatgen import Molecule
from pymatgen.io.gaussianio import GaussianInput

__author__ = 'Anubhav Jain'
__copyright__ = 'Copyright 2013, The Materials Project'
__version__ = '0.1'
__maintainer__ = 'Anubhav Jain'
__email__ = 'ajain@lbl.gov'
__date__ = 'Apr 10, 2013'


class GaussianTask(FireTaskBase, FWSerializable):
    """
    Write Gaussian input and run Gaussian
    """

    _fw_name = "Gaussian Task"

    def run_task(self, fw_spec):
        mol = Molecule.from_dict(fw_spec['mol'])
        gi = GaussianInput(mol, fw_spec['charge'], fw_spec['spin_multiplicity'], fw_spec['title'], fw_spec['functional'], fw_spec['basis_set'], fw_spec['route_parameters'], fw_spec['input_parameters'], fw_spec['link0_parameters'])

        with open('gaussian.in') as f:
            f.write(str(gi))

        subprocess.call(['g09l', 'gaussian.in', 'output'])
