from pprint import pprint
from fireworks.core.firework import FireWork, Workflow
from mpworks.firetasks.gaussian_tasks import GaussianTask
from pymatgen import Molecule

__author__ = 'Anubhav Jain'
__copyright__ = 'Copyright 2013, The Materials Project'
__version__ = '0.1'
__maintainer__ = 'Anubhav Jain'
__email__ = 'ajain@lbl.gov'
__date__ = 'Apr 10, 2013'


def mol_to_wf(mol):
    spec = {}
    spec['molecule'] = mol.to_dict
    spec['charge'] = 0
    spec['spin_multiplicity'] = 1
    spec['title'] = 'first test job'
    spec['functional'] = 'B3LYP'
    spec['basis_set'] = '6-31+G(d)'
    spec['route_parameters'] = {'Opt':'', 'SCF':'Tight'}
    spec['input_parameters'] = None
    spec['link0_parameters'] = {'%mem': '100MW', '%chk':'molecule'}
    spec['_category'] = 'Molecules'

    fw = FireWork([GaussianTask()], spec)

    return Workflow.from_FireWork(fw)


if __name__ == '__main__':
    coords = [[0.000000, 0.000000, 0.000000],
              [0.000000, 0.000000, 1.089000],
              [1.026719, 0.000000, -0.363000],
              [-0.513360, -0.889165, -0.363000],
              [-0.513360, 0.889165, -0.363000]]
    mol = Molecule(["C", "H", "H", "H", "H"], coords)

    wf = mol_to_wf(mol)

    wf.to_file('CH3.yaml')