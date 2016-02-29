#!/usr/bin/env python

"""

"""
import gzip
import json
import logging
import os
import shutil
import sys
from monty.os.path import zpath
from custodian.vasp.handlers import UnconvergedErrorHandler
from fireworks.core.launchpad import LaunchPad

from fireworks.utilities.fw_serializers import FWSerializable
from fireworks.core.firework import FireTaskBase, FWAction, Firework, Workflow
from fireworks.utilities.fw_utilities import get_slug
from mpworks.drones.mp_vaspdrone import MPVaspDrone
from mpworks.dupefinders.dupefinder_vasp import DupeFinderVasp
from mpworks.firetasks.custodian_task import get_custodian_task
from mpworks.firetasks.vasp_setup_tasks import SetupUnconvergedHandlerTask
from mpworks.workflows.wf_settings import QA_VASP, QA_DB, MOVE_TO_GARDEN_PROD, MOVE_TO_GARDEN_DEV
from mpworks.workflows.wf_utils import last_relax, get_loc, move_to_garden
from pymatgen import Composition
from pymatgen.io.vasp.inputs import Incar, Poscar, Potcar, Kpoints
from pymatgen.matproj.snl import StructureNL

__author__ = 'Anubhav Jain'
__copyright__ = 'Copyright 2013, The Materials Project'
__version__ = '0.1'
__maintainer__ = 'Anubhav Jain'
__email__ = 'ajain@lbl.gov'
__date__ = 'Mar 15, 2013'


class VaspWriterTask(FireTaskBase, FWSerializable):
    """
    Write VASP input files based on the fw_spec
    """

    _fw_name = "Vasp Writer Task"

    def run_task(self, fw_spec):
        fw_spec['vasp']['incar'].write_file('INCAR')
        fw_spec['vasp']['poscar'].write_file('POSCAR')
        fw_spec['vasp']['potcar'].write_file('POTCAR')
        fw_spec['vasp']['kpoints'].write_file('KPOINTS')


class VaspCopyTask(FireTaskBase, FWSerializable):
    """
    Copy the VASP run directory in 'prev_vasp_dir' to the current dir
    """

    _fw_name = "Vasp Copy Task"

    def __init__(self, parameters=None):
        """
        :param parameters: (dict) Potential keys are 'use_CONTCAR', and 'files'
        """
        parameters = parameters if parameters else {}
        self.update(parameters)  # store the parameters explicitly set by the user

        default_files = ['INCAR', 'POSCAR', 'KPOINTS', 'POTCAR', 'OUTCAR',
                         'vasprun.xml', 'OSZICAR']

        if not parameters.get('skip_CHGCAR'):
            default_files.append('CHGCAR')

        self.missing_CHGCAR_OK = parameters.get('missing_CHGCAR_OK', True)

        self.files = parameters.get('files', default_files)  # files to move
        self.use_contcar = parameters.get('use_CONTCAR', True)  # whether to move CONTCAR to POSCAR

        if self.use_contcar:
            self.files.append('CONTCAR')
            self.files = [x for x in self.files if x != 'POSCAR']  # remove POSCAR

    def run_task(self, fw_spec):
        prev_dir = get_loc(fw_spec['prev_vasp_dir'])

        if '$ALL' in self.files:
            self.files = os.listdir(prev_dir)

        for file in self.files:
            prev_filename = last_relax(os.path.join(prev_dir, file))
            dest_file = 'POSCAR' if file == 'CONTCAR' and self.use_contcar else file
            if prev_filename.endswith('.gz'):
                dest_file += '.gz'

            print 'COPYING', prev_filename, dest_file
            if self.missing_CHGCAR_OK and 'CHGCAR' in dest_file and not os.path.exists(zpath(prev_filename)):
                print 'Skipping missing CHGCAR'
            else:
                shutil.copy2(prev_filename, dest_file)
                if '.gz' in dest_file:
                    # unzip dest file
                    f = gzip.open(dest_file, 'rb')
                    file_content = f.read()
                    with open(dest_file[0:-3], 'wb') as f_out:
                        f_out.writelines(file_content)
                    f.close()
                    os.remove(dest_file)



        return FWAction(stored_data={'copied_files': self.files})


class VaspToDBTask(FireTaskBase, FWSerializable):
    """
    Enter the VASP run directory in 'prev_vasp_dir' to the database.
    """

    _fw_name = "Vasp to Database Task"

    def __init__(self, parameters=None):
        """
        :param parameters: (dict) Potential keys are 'additional_fields', and 'update_duplicates'
        """
        parameters = parameters if parameters else {}
        self.update(parameters)

        self.additional_fields = self.get('additional_fields', {})
        self.update_duplicates = self.get('update_duplicates', True)  # off so DOS/BS doesn't get entered twice

    def run_task(self, fw_spec):
        if '_fizzled_parents' in fw_spec and not 'prev_vasp_dir' in fw_spec:
            prev_dir = get_loc(fw_spec['_fizzled_parents'][0]['launches'][0]['launch_dir'])
            update_spec = {}  # add this later when creating new FW
            fizzled_parent = True
            parse_dos = False
        else:
            prev_dir = get_loc(fw_spec['prev_vasp_dir'])
            update_spec = {'prev_vasp_dir': prev_dir,
                           'prev_task_type': fw_spec['prev_task_type'],
                           'run_tags': fw_spec['run_tags'], 'parameters': fw_spec.get('parameters')}
            fizzled_parent = False
            parse_dos = 'Uniform' in fw_spec['prev_task_type']
        if 'run_tags' in fw_spec:
            self.additional_fields['run_tags'] = fw_spec['run_tags']
        else:
            self.additional_fields['run_tags'] = fw_spec['_fizzled_parents'][0]['spec']['run_tags']

        if MOVE_TO_GARDEN_DEV:
            prev_dir = move_to_garden(prev_dir, prod=False)

        elif MOVE_TO_GARDEN_PROD:
            prev_dir = move_to_garden(prev_dir, prod=True)

        # get the directory containing the db file
        db_dir = os.environ['DB_LOC']
        db_path = os.path.join(db_dir, 'tasks_db.json')

        logging.basicConfig(level=logging.INFO)
        logger = logging.getLogger('MPVaspDrone')
        logger.setLevel(logging.INFO)
        sh = logging.StreamHandler(stream=sys.stdout)
        sh.setLevel(getattr(logging, 'INFO'))
        logger.addHandler(sh)
        with open(db_path) as f:
            db_creds = json.load(f)
            drone = MPVaspDrone(host=db_creds['host'], port=db_creds['port'],
                                database=db_creds['database'], user=db_creds['admin_user'],
                                password=db_creds['admin_password'],
                                collection=db_creds['collection'], parse_dos=parse_dos,
                                additional_fields=self.additional_fields,
                                update_duplicates=self.update_duplicates)
            t_id, d = drone.assimilate(prev_dir, launches_coll=LaunchPad.auto_load().launches)

        mpsnl = d['snl_final'] if 'snl_final' in d else d['snl']
        snlgroup_id = d['snlgroup_id_final'] if 'snlgroup_id_final' in d else d['snlgroup_id']
        update_spec.update({'mpsnl': mpsnl, 'snlgroup_id': snlgroup_id})

        print 'ENTERED task id:', t_id
        stored_data = {'task_id': t_id}
        if d['state'] == 'successful':
            update_spec['analysis'] = d['analysis']
            update_spec['output'] = d['output']
            update_spec['vasp']={'incar':d['calculations'][-1]['input']['incar'],
                                 'kpoints':d['calculations'][-1]['input']['kpoints']}
            update_spec["task_id"]=t_id
            # Add elasticity analysis firework
            additions = []
            if 'ndoc' in d and d['ndoc'] >= 20:
                spec = {'original_task_id' : d['original_task_id'],
                        'task_type' : 'Add Elastic Data to DB'}
                snl = StructureNL.from_dict(mpsnl)
                f = Composition(
                        snl.structure.composition.reduced_formula).alphabetical_formula
                additions += [Firework([AddElasticDataToDB()], spec, fw_id = -1,
                                     name = get_slug(f + '--' + spec['task_type']))]

            return FWAction(stored_data=stored_data, update_spec=update_spec,
                            additions = additions)

        # not successful - first test to see if UnconvergedHandler is needed
        if not fizzled_parent:
            unconverged_tag = 'unconverged_handler--{}'.format(fw_spec['prev_task_type'])
            output_dir = last_relax(os.path.join(prev_dir, 'vasprun.xml'))
            ueh = UnconvergedErrorHandler(output_filename=output_dir)
            if ueh.check() and unconverged_tag not in fw_spec['run_tags']:
                print 'Unconverged run! Creating dynamic FW...'

                spec = {'prev_vasp_dir': prev_dir,
                        'prev_task_type': fw_spec['task_type'],
                        'mpsnl': mpsnl, 'snlgroup_id': snlgroup_id,
                        'task_type': fw_spec['prev_task_type'],
                        'run_tags': list(fw_spec['run_tags']),
                        'parameters': fw_spec.get('parameters'),
                        '_dupefinder': DupeFinderVasp().to_dict(),
                        '_priority': fw_spec['_priority']}
                # Pass elastic tensor spec
                if 'deformation_matrix' in fw_spec.keys():
                    spec['deformation_matrix'] = fw_spec['deformation_matrix']
                    spec['original_task_id'] = fw_spec['original_task_id']
                snl = StructureNL.from_dict(spec['mpsnl'])
                spec['run_tags'].append(unconverged_tag)
                spec['_queueadapter'] = QA_VASP

                fws = []
                connections = {}

                f = Composition(
                    snl.structure.composition.reduced_formula).alphabetical_formula

                fws.append(Firework(
                    [VaspCopyTask({'files': ['INCAR', 'KPOINTS', 'POSCAR', 'POTCAR', 'CONTCAR'],
                                   'use_CONTCAR': False}), SetupUnconvergedHandlerTask(),
                     get_custodian_task(spec)], spec, name=get_slug(f + '--' + spec['task_type']),
                    fw_id=-2))

                spec = {'task_type': 'VASP db insertion', '_allow_fizzled_parents': True,
                        '_priority': fw_spec['_priority'], '_queueadapter': QA_DB,
                        'run_tags': list(fw_spec['run_tags'])}
                if 'deformation_matrix' in fw_spec.keys():
                    spec['deformation_matrix'] = fw_spec['deformation_matrix']
                    spec['original_task_id'] = fw_spec['original_task_id']
                spec['run_tags'].append(unconverged_tag)
                fws.append(
                    Firework([VaspToDBTask()], spec, name=get_slug(f + '--' + spec['task_type']),
                             fw_id=-1))
                connections[-2] = -1

                wf = Workflow(fws, connections)

                return FWAction(detours=wf)

        # not successful and not due to convergence problem - FIZZLE
        raise ValueError("DB insertion successful, but don't know how to fix this Firework! Can't continue with workflow...")


class AddElasticDataToDB(FireTaskBase, FWSerializable):
    _fw_name = "Add Elastic Data to DB"

    def run_task(self, fw_spec):
        db_dir = os.environ['DB_LOC']
        db_path = os.path.join(db_dir, 'tasks_db.json')
        i = fw_spec['original_task_id']

        with open(db_path) as f:
            db_creds = json.load(f)
        connection = MongoClient(db_creds['host'], db_creds['port'])
        tdb = connection[db_creds['database']]
        tdb.authenticate(db_creds['admin_use'], db_creds['admin_password'])
        ndocs = tasks.find({"original_task_id": i, 
                            "state":"successful"}).count()
        existing_doc = tdb.elasticity.find_one({"relaxation_task_id" : i})
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
            delta = Decimal((defo - np.eye(e))[d_ind][0])
            # Normal deformation
            if d_ind[0] == d_ind[1]:
                dtype = "_".join(["d", str(d_ind[0][0]), 
                                  "{:.0e}".format(delta)])
            # Shear deformation
            else:
                dtype = "_".join(["s", str(d_ind[0] + d_ind[1]),
                                  "{:.0e}".format(delta)])

            d["deformation_tasks"][dtype] = {"state" : k["state"],
                                             "deformation_matrix" : defo,
                                             "strain" : IndependentStrain(defo),
                                             "task_id": k["task_id"]}
            if k["state"] == "successful":
                st = Stress(k["calculations"][-1]["output"] \
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

        # New style
        elif "input_mp_id" in d:
            d["material_id"] = d["input_mp_id"]
        else:
            d["material_id"] = None
        d["relaxation_task_id"] = i

        calc_struct = Structure.from_dict(o["snl_final"])
        conventional = is_conventional(calc_struct)
        if conventional:
            d["analysis"]["is_conventional"] = True
        else:
            d["analysis"]["is_conventional"] = False

        d["spacegroup"]=o.get("spacegroup", "Unknown")
        
        if ndocs >= 20:
            # Perform Elastic tensor fitting and analysis
            result = ElasticTensor.from_stress_dict(ss_dict)
            d["elastic_tensor"] = result.tolist()
            kg_average = result.kg_average
            d.update({"K_Voigt":kg_average[0], "G_Voigt":kg_average[1], 
                      "K_Reuss":kg_average[2], "G_Reuss":kg_average[3], 
                      "K_Voigt_Reuss_Hill":kg_average[4], 
                      "G_Voigt_Reuss_Hill":kg_average[5]})
            d["universal_anisotropy"] = result.universal_anisotropy
            d["homogeneous_poisson"] = result.homogeneous_poisson
            if ndocs < 24:
                d["warning"].append("less than 24 tasks completed")

            # Perform filter checks
            symm_t = result.symmetrized
            d["symmetrized_tensor"] = symm_t.tolist()
            d["analysis"]["not_rare_earth"] = True
            for s in calc_struct.species:
                if s.is_rare_earth_metal:
                    d["analysis"]["not_rare_earth"] = False
            eigvals = np.linalg.eigvals(symmetrized_tensor)
            eig_positive = np.all((eigvals > 0) & np.isreal(eigvals))
            d["analysis"]["eigval_positive"] = bool(eig_positive) 
            c11 = symm_t[0][0]
            c12 = symm_t[0][1]
            c13 = symm_t[0][2]
            c23 = symm_t[1][2]
            d["analysis"]["c11_c12"]= not (abs((c11-c12)/c11) < 0.05
                                           or c11 < c12)
            d["analysis"]["c11_c13"]= not (abs((c11-c13)/c11) < 0.05 
                                           or c11 < c13)
            d["analysis"]["c11_c23"]= not (abs((c11-c23)/c11) < 0.1 
                                           or c11 < c23)
            d["analysis"]["K_R"] = not (d["K_Reuss"] < 2)
            d["analysis"]["G_R"] = not (d["G_Reuss"] < 2)
            d["analysis"]["K_V"] = not (d["K_Voigt"] < 2)
            d["analysis"]["G_V"] = not (d["G_Voigt"] < 2)
            filter_state = np.all(d["analysis"].values())
            d["analysis"]["filter_pass"] = bool(filter_state)
            d["analysis"]["eigval"] = list(eigvals)

            # JHM: eventually we can reintroduce the IEEE conversion
            #       but as of now it's not being used, and it should
            #       be in pymatgen
            """
            # IEEE Conversion
            try:
                ieee_tensor = IEEE_conversion.get_ieee_tensor(struct_final, result)
                d["elastic_tensor_IEEE"] = ieee_tensor[0].tolist()
                d["analysis"]["IEEE"] = True
            except Exception as e:
                d["elastic_tensor_IEEE"] = None
                d["analysis"]["IEEE"] = False
                d["error"].append("Unable to get IEEE tensor: {}".format(e))
            """
            # Add thermal properties
            nsites = calc_struct.num_sites
            volume = calc_struct.volume
            natoms = calc_struct.composition.num_atoms
            weight = calc_struct.composition.weight
            num_density = 1e30 * nsites / volume
            mass_density = 1.6605e3 * nsites * volume * weight / \
                           (natoms * volume)
            tot_mass = sum([e.atomic_mass for e in relaxed_struct.species])
            avg_mass =  1.6605e-27 * tot_mass / natoms
            y_mod = 9e9 * result.k_vrh * result.g_vrh / \
                    (3. * result.k_vrh * result.g_vrh)
            trans_v = 1e9 * result.k_vrh / mass_density**0.5
            long_v = 1e9 * result.k_vrh + \
                     4./3. * result.g_vrh / mass_density**0.5
            clarke = 0.87 * 1.3806e-23 * avg_mass**(-2./3.) * \
                     mass_density**(1./6.) * y_mod**0.5
            cahill = 1.3806e-23 / 2.48 * num_density**(2./3.) * long_v + \
                     2 * trans_v
            snyder_ac = 0.38483 * avg_mass * \
                        (long_v + 2./3.*trans_v)**3. / \
                        (300. * num_density**(-2./3.) * nsites**(1./3.))
            snyder_opt = 1.66914e-23 * (long_v + 2./3.*trans_v) / \
                         num_density**(-2./3.) * \
                         (1 - nsites**(-1./3.))
            snyder_total = snyder_ac + snyder_opt
            debye = 2.489e-11 * avg_mass**(-1./3.) * \
                    mass_density**(-1./6.) * y_mod**0.5

            d["thermal"]={"num_density" : num_density,
                          "mass_density" : mass_density,
                          "avg_mass" : avg_mass,
                          "num_atom_per_unit_formula" : natoms,
                          "youngs_modulus" : y_mod,
                          "trans_velocity" : trans_v,
                          "long_velocity" : long_v,
                          "clarke" : clarke,
                          "cahill" : cahill,
                          "snyder_acou_300K" : snyder_ac,
                          "snyder_opt" : snyder_opt,
                          "snyder_total" : snyder_total,
                          "debye": debye
                         }
        else:
            d['state'] = "Fewer than 20 successful tasks completed"
            return FWAction()

        if o["snl"]["about"].get("_kpoint_density"):
            d["kpoint_density"]= o["snl"]["about"].get("_kpoint_density")

        if d["error"]:
            raise ValueError("Elastic analysis failed: {}".format(d["error"]))
        elif d["analysis"]["filter_pass"]:
            d["state"] = "successful"
        else:
            d["state"] = "filter_failed"
        tdb.elasticity.insert(d)
        return FWAction()
