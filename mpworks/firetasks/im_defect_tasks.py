from __future__ import division
"""
Firetasks related to intermetallic defect tasks
"""
__author__ = 'Bharat Medasani'

from monty.os.path import zpath
from fireworks.utilities.fw_serializers import FWSerializable
from fireworks.core.firework import FireTaskBase, FWAction
from fireworks.utilities.fw_utilities import get_slug
#from pymatgen.transformation.genstrain import DeformGeometry
from pymatgen.transformations.defect_transformations import \
        VacancyTransformation, AntisiteDefectTransformation
from pymatgen.analysis.defects import Vacancy
from fireworks.core.firework import Firework, Workflow
from pymatgen.matproj.snl import StructureNL
from mpworks.workflows import snl_to_wf
from mpworks.firetasks.snl_tasks import AddSNLTask
from mpworks.firetasks.custodian_task import get_custodian_task
from mpworks.firetasks.vasp_io_tasks import VaspWriterTask, VaspToDBTask
from mpworks.snl_utils.mpsnl import MPStructureNL
from pymatgen.core.structure import Structure
from pymatgen.core.composition import Composition
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
    fw_spec['vasp']['kpoints'] = k.as_dict()
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
    fw_spec['vasp']['kpoints'] = k.as_dict()
    return fw_spec


def get_sc_scale(inp_struct, final_site_no):
    lengths = inp_struct.lattice.abc
    print 'lengths', lengths
    no_sites = inp_struct.num_sites
    print 'no_sites', no_sites
    mult = (final_site_no/no_sites*lengths[0]*lengths[1]*lengths[2]) ** (1.0/3)
    print 'mult', mult
    num_mult = [int(round(mult/l)) for l in lengths]
    print 'no_mult', num_mult
    num_mult = [i if i > 0 else 1 for i in num_mult]
    print 'modified no_mult', num_mult
    return num_mult

def get_workflow(d_struct, data, history, i, fw_spec):
    fws=[]
    connections={}
    f = Composition(d_struct.formula).alphabetical_formula
    snl = StructureNL(d_struct, 'Bharat Medasani <bkmedasani@lbl.gov>', projects=["IM_Defects"],
            data=data, history=history)

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

    spec['_priority'] = fw_spec['_priority']*2
    trackers = [Tracker('FW_job.out'), Tracker('FW_job.error'), Tracker('vasp.out'), Tracker('OUTCAR'), Tracker('OSZICAR'), Tracker('OUTCAR.relax1'), Tracker('OUTCAR.relax2')]
    trackers_db = [Tracker('FW_job.out'), Tracker('FW_job.error')]
             # run GGA structure optimization
    #spec = _snl_to_spec(snl, enforce_gga=True, parameters=parameters)
    spec.update(snl_spec)
    spec['_queueadapter'] = QA_VASP
    spec['_trackers'] = trackers
                                 
    #Turn off dupefinder for supercell structure
    del spec['_dupefinder']

    fws.append(Firework([VaspWriterTask(), get_custodian_task(spec)],
                        spec, name=get_slug(f + '--' + fw_spec['task_type']), fw_id=-999+i*10))

    priority = fw_spec['_priority']*3
    spec = {'task_type': 'VASP db insertion', '_priority': priority,
            '_allow_fizzled_parents': True, '_queueadapter': QA_DB, 
            'clean_task_doc':True,
            }
    fws.append(Firework([VaspToDBTask()], spec, name=get_slug(f + '--' + spec['task_type']), fw_id=-998+i*10))
    connections[-999+i*10] = [-998+i*10]
    return Workflow(fws, connections)

class SetupDefectSupercellStructTask(FireTaskBase, FWSerializable):
    _fw_name = "Setup Deformed Struct Task"

    def run_task(self, fw_spec):
        # Read structure from spec
        snl = StructureNL.from_dict(fw_spec['snl'])
        inp_struct = snl.structure#Structure.from_dict(fw_spec['structure'])
        # Generate defect supercell structures
        cellmax = fw_spec.get('supercell_size', 128)
        supercell_scale = get_sc_scale(inp_struct, cellmax)
        #bulk_supercell = inp_struct.copy()
        #bulk_supercell.make_supercell(supercell_scaling)
        #vt = VacancyTransformation(supercell_scaling)
        #vac_supercells = vt.apply_transformation(inp_struct, 
        #        return_ranked_list=999)
        #ast = AntisiteDefectTransformation(supercell_scaling)
        #antisite_supercells = ast.apply_transformation(inp_struct, 
        #        return_ranked_list=999)
        #supercell_defects = [bulk_supercell] + \
        #        list(sc_dict['structure'] for sc_dict in vac_supercells) + \
        #        list(sc_dict['structure'] for sc_dict in antisite_supercells)

        vac = Vacancy(inp_struct, {}, {})
        scs = vac.make_supercells_with_defects(supercell_scale)
        site_no = scs[0].num_sites
        if site_no > cellmax:
            max_sc_dim = max(sc_scale)
            i = sc_scale.index(max_sc_dim)
            sc_scale[i] -= 1
            scs = vac.make_supercells_with_defects(sc_scale)

        wfs = []
        for i in range(len(scs)):
            sc = scs[i]
            f = Composition(sc.formula).alphabetical_formula
            data = snl.data if snl.data else {'_base_mpid': mpid}
            history = snl.history if snl.history else [] 

            if not i:
                data['_type'] = 'bulk'
                wf = get_workflow(sc, data, history, i, fw_spec)
                wfs.append(wf)
            else:
                data['_type'] = 'defect'

                type_defect  = 'vacancy'
                blk_str_sites = set(scs[0].sites)
                vac_str_sites = set(sc.sites)
                vac_sites = blk_str_sites - vac_str_sites
                vac_site = list(vac_sites)[0]
                site_mult = vac.get_defectsite_multiplicity(i-1)
                vac_site_specie = vac_site.specie
                vac_symbol = vac_site.specie.symbol

                data['_defect_data'] = {
                        'type': type_defect, 'site_specie': vac_symbol,
                        'site_multiplicity': site_mult
                        }
                j = 1
                k = (i-1)*(len(scs)-1)+j
                wf = get_workflow(sc, data, history, k, fw_spec)
                wfs.append(wf)
                
                #vac_dir ='vacancy_{}_mult-{}_sitespecie-{}'.format(str(i), site_mult, vac_symbol)
                # Antisite generation at all vacancy sites
                struct_species = scs[0].types_of_specie
                for specie in set(struct_species)-set([vac_site_specie]):
                    j += 1
                    k = (i-1)*(len(scs)-1)+j
                    print 'i', 'j', 'k', i, j, k
                    type_defect = 'antisite'
                    subspecie_symbol = specie.symbol
                    anti_struct = sc.copy()
                    anti_struct.append(specie, vac_site.frac_coords)

                    data['_defect_data'] = {
                            'type': type_defect, 'site_specie': vac_symbol, 
                            'site_multiplicity': site_mult, 
                            'substitution_specie': subspecie_symbol
                            }
                    wf = get_workflow(anti_struct, data, history, k, fw_spec)
                    wfs.append(wf)



        #for i, sc in enumerate(supercell_defects):
        #    fws=[]
        #    connections={}
        #    d_struct = sc
        #    type = 'bulk' if not i else 'defect'
        #    data['_type'] = type
#
##
#            wf.append()
        return FWAction(additions=wfs)

