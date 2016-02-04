import time, yaml, sys, os
from fireworks.core.launchpad import LaunchPad
from fireworks.core.firework import Firework, Workflow
from mpworks.firetasks.controller_tasks import AddEStructureTask
from fireworks.utilities.fw_utilities import get_slug
from mpworks.snl_utils.snl_mongo import SNLMongoAdapter
from pymongo import MongoClient
from collections import Counter
from datetime import datetime
from fnmatch import fnmatch
from custodian.vasp.handlers import VaspErrorHandler

cwd = os.getcwd()

# DONE manually: "mp-987" -> fw_id: 119629

lpdb = LaunchPad.from_file('/global/homes/m/matcomp/mp_prod/config/config_Mendel/my_launchpad.yaml')
spec = {'task_type': 'Controller: add Electronic Structure v2', '_priority': 100000}
sma = SNLMongoAdapter.from_file('/global/homes/m/matcomp/mp_prod/config/dbs/snl_db.yaml')
with open('/global/homes/m/matcomp/mp_prod/materials_db_prod.yaml') as f:
    creds = yaml.load(f)
client = MongoClient(creds['host'], creds['port'])
db = client[creds['db']]
db.authenticate(creds['username'], creds['password'])
materials = db['materials']
tasks = db['tasks']

def append_wf(fw_id, parent_fw_id=None):
    wf = lpdb.workflows.find_one({'nodes':fw_id}, {'parent_links':1,'links':1,'name':1})
    try:
        if parent_fw_id is None:
            parent_fw_id = wf['parent_links'][str(fw_id)][-1]
        # non-defused AddEStructureTask v2 already in children?
        for child_fw_id in wf['links'][str(parent_fw_id)]:
            if child_fw_id == parent_fw_id: continue
            child_fw = lpdb.fireworks.find_one({'fw_id': child_fw_id}, {'spec.task_type':1, 'state':1})
            if child_fw['spec']['task_type'] == 'Controller: add Electronic Structure v2':
                if child_fw['state'] == 'DEFUSED':
                    lpdb.reignite_fw(child_fw_id)
                    print 'AddEStructureTask v2', child_fw_id , 'reignited for', fw_id
                elif child_fw['state'] == 'FIZZLED':
                    lpdb.rerun_fw(child_fw_id)
                    print 'AddEStructureTask v2', child_fw_id , 'marked for rerun for', fw_id
                elif child_fw['state'] == 'COMPLETED':
                    print 'AddEStructureTask v2 already successfully run for', fw_id
                    sec_child_fw_id = wf['links'][str(child_fw_id)][0]
		    sec_child_fw = lpdb.fireworks.find_one({'fw_id': sec_child_fw_id}, {'spec.task_type':1, 'state':1})
		    if sec_child_fw['state'] == 'FIZZLED':
                        lpdb.rerun_fw(sec_child_fw_id)
		        print 'FIZZLED -> marked for rerun:', sec_child_fw_id, sec_child_fw['spec']['task_type']
                else:
                    print 'AddEStructureTask v2 added but neither DEFUSED, FIZZLED, or COMPLETED for', fw_id
                return
        f = lpdb.get_wf_summary_dict(fw_id)['name'].replace(' ', '_')
        name = get_slug(f + '--' + spec['task_type'])
        fw = Firework([AddEStructureTask()], spec, name=name)
        lpdb.append_wf(Workflow([fw]), [parent_fw_id])
        print name, 'added for', fw_id
    except ValueError:
        raise ValueError('could not append controller task to wf', wf['name'])

if __name__ == "__main__":
    nfws = 0

    #append_wf(42391, parent_fw_id=69272)
    #append_wf(51449, parent_fw_id=76078)
    #for doc in lpdb.fireworks.find(
    #    {'spec.task_type': 'Controller: add Electronic Structure v2', 'spec._priority': {'$exists':1}},
    #    {'fw_id': 1, 'spec._priority': 1, 'state': 1 }
    #):
    #    if (doc['state'] == 'FIZZLED' or doc['state'] == 'READY') and doc['spec']['_priority'] == 100000:
    #        print nfws, doc['fw_id']
    #        lpdb.defuse_fw(doc['fw_id'])
    #        nfws += 1

    #for doc in lpdb.fireworks.find(
    #    {'spec.task_type': 'Controller: add Electronic Structure', 'state': 'COMPLETED',
    #     'spec.analysis': {'$exists':1}, 'fw_id': {'$gte': 155067}}, # new controllers added -> fizzled GGA static reruns
    #    {'fw_id': 1, 'spec.analysis.bandgap': 1}
    #):
    #    fw_id, bandgap = doc['fw_id'], doc['spec']['analysis']['bandgap'] 
    #    if bandgap > 0 and bandgap < 0.5:
    #        try:
    #            append_wf(fw_id)
    #            time.sleep(.5)
    #        except ValueError:
    #            continue
    #        nfws += 1
    #        #if nfws > 10: break
    #print 'nfws =', nfws

    mp_ids = [
	'mp-2123', 'mp-10886', 'mp-582799', 'mp-21477', 'mp-535', 'mp-21293', 'mp-8700',
	'mp-9568', 'mp-973', 'mp-505622', 'mp-20839', 'mp-1940', 'mp-16521', 'mp-30354',
	'mp-568953', 'mp-454', 'mp-1010', 'mp-1416', 'mp-21385', 'mp-27659', 'mp-22481',
	'mp-569529', 'mp-1057', 'mp-1834', 'mp-2336', 'mp-12857', 'mp-21109', 'mp-30387',
	'mp-30599', 'mp-21884', 'mp-11397', 'mp-11814', 'mp-510437', 'mp-12565', 'mp-33032',
        'mp-20885', 'mp-1891',
	"mp-987", "mp-1542", "mp-2252", "mp-966", "mp-6945", "mp-1598",
	"mp-7547", "mp-554340", "mp-384", "mp-2437", "mp-1167", "mp-571266",
        "mp-560338", "mp-27253", "mp-1705", "mp-2131", "mp-676", "mp-2402", "mp-9588",
        "mp-2452", "mp-690", "mp-30033", "mp-10155", "mp-9921", "mp-9548", "mp-569857",
        "mp-29487", "mp-909", "mp-1536", "mp-28391", "mp-558811", "mp-1033", "mp-1220",
        "mp-7817", "mp-30952", "mp-569175", "mp-1683", "mp-27821", "mp-554243",
        "mp-557837", "mp-867227", "mp-862871", "mp-861979", "mp-24289", "mp-684690",
        "mp-551905", "mp-437", "mp-1806", "mp-556395", "mp-14288", "mp-1944",
        "mp-15339", "mp-568208", "mp-28096", "mp-542613", "mp-862983", "mp-864974",
        "mp-865966", "mp-20401", "mp-864898", "mp-546711", "mp-8429", "mp-867171",
        "mp-862705", "mp-864768", "mp-7984", "mp-864844", "mp-865147", "mp-867909",
        "mp-861629", "mp-961673", "mp-753287", "mp-4701", "mp-3532", "mp-864839",
        "mp-867333", "mp-12335", "mp-7492", "mp-867149", "mp-862316", "mp-865680",
        "mp-867193", "mp-5456", "mp-862699", "mp-866139", "mp-866166", "mp-867788",
        "mp-865791", "mp-864630", "mp-862719", "mp-865502", "mp-862721", "mp-862717",
        "mp-865050", "mp-867223", "mp-865965", "mp-560547", "mp-4826", "mp-8015",
        "mp-865097", "mp-861507", "mp-861901", "mp-866165", "mp-11980", "mp-867113",
        "mp-10887", "mp-862672", "mp-19799", "mp-867207", "mp-865989", "mp-865681",
        "mp-31055", "mp-861656", "mp-864762", "mp-862297", "mp-10810", "mp-644280",
        "mp-18971", "mp-19149", "mp-867781", "mp-865044", "mp-867307", "mp-867169",
        "mp-8995", "mp-865912", "mp-863707", "mp-866164", "mp-13312", "mp-754540",
        "mp-866117", "mp-28250", "mp-20761", "mp-866229", "mp-10809", "mp-8717",
        "mp-862947", "mp-866154", "mp-864933", "mp-861725", "mp-865519", "mp-20999",
        "mp-865867", "mp-10608", "mp-867833", "mp-540609", "mp-866105", "mp-862318",
        "mp-5229", "mp-865986", "mp-22966", "mp-37514", "mp-7006", "mp-862445",
        "mp-862691", "mp-541226", "mp-865659", "mp-867810", "mp-7473", "mp-10140",
        "mp-867799", "mp-865130", "mp-866096", "mp-10113", "mp-862486", "mp-8994",
        "mp-865433", "mp-29506", "mp-21035", "mp-865010", "mp-8014", "mp-28237",
        "mp-4047", "mp-4505", "mp-867881", "mp-861736", "mp-867258", "mp-22120",
        "mp-865001", "mp-865278", "mp-864883", "mp-6989", "mp-865963", "mp-19279",
        "mp-864654", "mp-608311", "mp-865933", "mp-867280", "mp-866219", "mp-28698",
        "mp-14006", "mp-29624", "mp-867761", "mp-22793", "mp-9872", "mp-867926",
        "mp-510273", "mp-32526", "mp-862296", "mp-19238", "mp-3530", "mp-3332",
        "mp-866106", "mp-867807", "mp-21017", "mp-20325", "mp-557912", "mp-867769",
        "mp-29751", "mp-11695", "mp-865603", "mp-865186", "mp-19035", "mp-865929",
        "mp-864618", "mp-867872", "mp-8397", "mp-865308", "mp-567636", "mp-867266",
        "mp-865183", "mp-510268", "mp-862694", "mp-8013", "mp-867271", "mp-578618",
        "mp-865713", "mp-865167", "mp-29009", "mp-865128", "mp-32497", "mp-864684",
        "mp-862473", "mp-865280", "mp-3020", "mp-27193", "mp-861937", "mp-867896",
        "mp-862374", "mp-28872", "mp-23425", "mp-10417"
    ]
    mp_ids = [ "mp-134", "mp-127", "mp-58", "mp-135", "mp-70", "mp-1" ]
    mp_ids = [doc['task_id'] for doc in materials.find({'has_bandstructure': False}, {'task_id':1}).skip(400)]
    print '#mp_ids =', len(mp_ids)

    counter = Counter()
    materials_wBS = []
    for matidx, material in enumerate(materials.find({'task_id': {'$in': mp_ids}}, {'task_id': 1, '_id': 0, 'snlgroup_id_final': 1, 'has_bandstructure': 1, 'pretty_formula': 1})):
        mp_id, snlgroup_id = material['task_id'], material['snlgroup_id_final']
	url = 'https://materialsproject.org/materials/' + mp_id
        if material['has_bandstructure']:
            materials_wBS.append((mp_id, material['pretty_formula']))
            counter['has_bandstructure'] += 1
        print matidx, '========', mp_id, snlgroup_id, '============='
        fw_list = list(lpdb.fireworks.find(
            {'spec.snlgroup_id': snlgroup_id},
            {'_id': 0, 'state': 1, 'name': 1, 'fw_id': 1, 'spec.snlgroup_id': 1, 'spec.task_type': 1, 'launches': 1}
        ))
        if len(fw_list) > 0:
            has_gga_static = False
            for fw in fw_list:
                if fw['spec']['task_type'] == 'GGA static v2':
                    has_gga_static = True
                    if fw['state'] == 'FIZZLED':
                        #counter[fw['spec']['task_type']] += 1
                        print '--'.join([fw['name'], str(fw['fw_id'])]), fw['state']
                        launch_dir = lpdb.launches.find_one({'launch_id': fw['launches'][0]}, {'launch_dir':1})['launch_dir']
                        launch_subdir = '/'.join(launch_dir.split('/')[-2:])
                        if 'oasis' in launch_dir:
                            launch_dir = os.path.join('/global/projecta/projectdirs/matgen/scratch/mp_prod', launch_subdir)
                        if 'scratch2/sd' in launch_dir:
                            launch_dir = os.path.join('/global/projecta/projectdirs/matgen/scratch/mp_prod', launch_subdir)
                        try:
                            os.chdir(launch_dir)
                        except:
                            launch_dir = launch_dir.replace('scratch/mp_prod', 'garden/dev')
                            try:
                                os.chdir(launch_dir)
                            except:
                                launch_dir = launch_dir.replace('garden/dev', 'garden')
                                try:
                                    os.chdir(launch_dir)
                                except:
                                    print '    |===> could not find launch directory in usual locations'
                                    lpdb.rerun_fw(fw['fw_id'])
                                    print '    |===> marked for RERUN'
                                    counter['LOCATION_NOT_FOUND'] += 1
                                    continue
                        print '    |===>', launch_dir
                        vaspout = os.path.join(launch_dir, "vasp.out")
                        if not os.path.exists(vaspout):
                            vaspout = os.path.join(launch_dir, "vasp.out.gz")
                        h = VaspErrorHandler(vaspout)
                        try:
                            h.check()
                        except:
                            counter['GGA_static_handler_check_error'] += 1
                        d = h.correct()
                        if d['errors']:
                            for err in d['errors']:
                                counter['GGA_static_' + err] += 1
                            if 'brmix' in d['errors']:
                                lpdb.rerun_fw(fw['fw_id'])
                                print '    |===> BRMIX error -> marked for RERUN with alternative strategy'
                        else:
                            print '    |===> no vasp error indicated -> TODO'
                            counter['GGA_STATIC_NO_VASP_ERROR'] += 1
                        os.chdir(cwd)
                    else:
                        workflow = lpdb.workflows.find_one(
                            {'nodes': fw['fw_id']},
                            {'state': 1, '_id': 0, 'fw_states': 1, 'nodes': 1, 'updated_on': 1, 'parent_links': 1}
                        )
                        if workflow is None:
                            print '      |==> workflow not found', fw['fw_id']
                            counter['WF_NOT_FOUND'] += 1
                            continue
                        is_new = bool(datetime(2016, 1, 1) < workflow['updated_on'])
                        if workflow['state'] == 'FIZZLED':
                            for fw_id_fizzled, fw_state in workflow['fw_states'].iteritems():
                                if fw_state == 'FIZZLED':
                                    fw_fizzled = lpdb.fireworks.find_one({'fw_id': int(fw_id_fizzled)}, {'_id': 0, 'name': 1, 'fw_id': 1, 'spec.task_type': 1})
                                    counter[fw_fizzled['spec']['task_type']] += 1
                                    print url, is_new, material['has_bandstructure'], fw_id_fizzled
                                    print 'http://fireworks.dash.materialsproject.org/wf/'+str(fw['fw_id']), workflow['state']
                                    print '      |==>', '--'.join([fw_fizzled['name'], fw_id_fizzled])
                                    if fnmatch(fw_fizzled['spec']['task_type'], '*Boltztrap*'):
                                        print '      |====> marked for RERUN (Boltztrap, physical constants from scipy, missing libmkl_lapack.so, BoltzTrap_TE -> pymatgen)'
                                        #lpdb.rerun_fw(fw_fizzled['fw_id'])
                                        continue
                                    elif fw_fizzled['spec']['task_type'] == 'GGA Uniform v2':
                                        fw_id_rerun = str(fw_fizzled['fw_id'])
                                        while 1:
                                            fw_id_rerun = str(workflow['parent_links'][fw_id_rerun][-1])
                                            fw_rerun = lpdb.fireworks.find_one({'fw_id': int(fw_id_rerun)}, {'_id': 0, 'spec.task_type': 1})
                                            if fw_rerun['spec']['task_type'] != 'VASP db insertion':
                                                print 'http://fireworks.dash.materialsproject.org/wf/'+fw_id_rerun
                                                break
                                        lpdb.rerun_fw(int(fw_id_rerun))
                                        print '      |====> marked for RERUN (could not get valid results from prev_vasp_dir, GGAstatic vasprun.xml validation error)'
                                    elif fw_fizzled['spec']['task_type'] == 'GGA band structure v2':
                                        print '           |===> marked for RERUN (trial & error)'
                                        try:
                                            lpdb.rerun_fw(fw_fizzled['fw_id'])
                                        except:
                                            print '           |===> could not rerun firework'
                                            counter['WF_LOCKED'] += 1
                                    elif fw_fizzled['spec']['task_type'] == 'VASP db insertion':
                                        print '           |===> marked for RERUN (trial & error)'
                                        lpdb.rerun_fw(fw_fizzled['fw_id'])
                                        #sys.exit(0)
                                    break
                        elif workflow['state'] == 'COMPLETED':
                            print url, is_new, material['has_bandstructure'], workflow['nodes'][0]
                            if not is_new and not material['has_bandstructure']:
                                #lpdb.rerun_fw(fw['fw_id'])
                                print '    |===> marked for RERUN with alternative brmix strategy (WF completed but BS missing)'
                                counter['WF_COMPLETED_MISSING_BS'] += 1
                                #sys.exit(0)
                            else:
                                counter['COMPLETED'] += 1
            if not has_gga_static:
                print 'ERROR: no GGA static run found!'
                print '\n'.join([
                    '--'.join([fw['name'], str(fw['fw_id']), fw['state']]) for fw in fw_list
                ])
                counter['NO_GGA_STATIC'] += 1
                #break
        else:
            print 'ERROR: no fireworks found!'
            counter['NO_FWS'] += 1
            #break
    print '#mp_ids =', len(mp_ids)
    print counter
    #print materials_wBS
