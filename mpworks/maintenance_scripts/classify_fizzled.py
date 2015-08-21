from collections import defaultdict
import os
from pymongo import MongoClient
import yaml
from fireworks.core.launchpad import LaunchPad

__author__ = 'Anubhav Jain'
__copyright__ = 'Copyright 2013, The Materials Project'
__version__ = '0.1'
__maintainer__ = 'Anubhav Jain'
__email__ = 'ajain@lbl.gov'
__date__ = 'Nov 11, 2013'

# This script tries to examine the FIZZLED FWS and classify them into groups
# This can be used to identify the greatest causes of failure and fix those first
# The types of failure groups will need to be updated

def get_parent_launch_locs(fw_id, lpdb):
    parent_fw_id = lpdb.workflows.find_one({"nodes": fw_id}, {"parent_links":1})['parent_links'][str(fw_id)][0]
    launch_ids = lpdb.fireworks.find_one({"fw_id": parent_fw_id},{'launches': 1})['launches']
    locs = []
    ran_fws = []
    for l in launch_ids:
        d = lpdb.launches.find_one({"launch_id": l}, {'launch_dir': 1, 'fw_id': 1})
        launch_loc = str(d['launch_dir'])
        ran_fws.append(d['fw_id'])
        locs.append("/project/projectdirs/matgen/garden/"+launch_loc[launch_loc.find('block_'):])

    return locs, parent_fw_id, ran_fws

def get_task_info(fw_id, tdb):
    x = tdb.tasks.find_one({"fw_id": fw_id}, {"analysis": 1})
    warnings = x['analysis'].get('warnings', [])
    warnings.extend(x['analysis']['errors_MP']['signals'])
    errors = x['analysis'].get('errors', [])
    errors.extend(x['analysis']['errors_MP']['critical_signals'])

    warnings = set(warnings)
    errors = set(errors)
    warnings = warnings.difference(errors)
    return set(warnings), set(errors)


if __name__ == '__main__':
    module_dir = os.path.dirname(os.path.abspath(__file__))
    lp_f = os.path.join(module_dir, 'my_launchpad.yaml')
    lpdb = LaunchPad.from_file(lp_f)

    tasks_f = os.path.join(module_dir, 'tasks_read.yaml')
    creds = {}
    with open(tasks_f) as f:
        creds = yaml.load(f)

    connection = MongoClient(creds['host'], creds['port'])
    tdb = connection[creds['db']]
    tdb.authenticate(creds['username'], creds['password'])


    except_dict = defaultdict(int)
    fizzled_fws = []



    for f in lpdb.fireworks.find({"state": "FIZZLED"}, {"fw_id":1}):
        fizzled_fws.append(f['fw_id'])

    for l in lpdb.launches.find({"state": "FIZZLED", "action":{"$ne": None}}, {"action":1, 'fw_id': 1, 'time_start': 1, 'launch_dir':1}, timeout=False):
        if l['fw_id'] in fizzled_fws:
            except_str = l['action']['stored_data'].get('_exception')
            if 'Disk quota exceeded' in except_str:
                 except_dict['DISK_QUOTA_EXCEEDED'] = except_dict['DISK_QUOTA_EXCEEDED']+1
                 print l['fw_id'], '*'
                 lpdb.rerun_fw(l['fw_id'])
            elif 'No such file' in except_str:
                # this is due to missing CHGCAR from Michael's old runs
                except_dict['NO_SUCH_FILE'] = except_dict['NO_SUCH_FILE']+1
            elif 'IMPROPER PARSING' in except_str:
                except_dict['IMPROPER_PARSING'] = except_dict['IMPROPER_PARSING']+1
            elif 'get valid results from relaxed run' in except_str:
                except_dict['INVALID_RESULTS'] = except_dict['INVALID_RESULTS']+1
            elif 'dir does not exist!' in except_str:
                except_dict['MISSING_DIR'] = except_dict['MISSING_DIR']+1
            elif 'Stale NFS file handle' in except_str:
                except_dict['STALE_NFS'] = except_dict['STALE_NFS']+1
            elif 'File exists' in except_str:
                except_dict['FILE_EXISTS'] = except_dict['FILE_EXISTS']+1
            elif 'MemoryError' in except_str:
                except_dict['MEMORY_ERROR'] = except_dict['MEMORY_ERROR']+1
            elif 'DB insertion successful, but don\'t know how to fix' in except_str:
                except_dict['NO_FIX'] = except_dict['NO_FIX']+1
                """
                launches, pfw_id, ran_fws = get_parent_launch_locs(l['fw_id'], lpdb)
                print '--',l['fw_id']
                for idx, l in enumerate(launches):
                    print l
                    print get_task_info(ran_fws[idx], tdb)
                """


            elif 'Poscar.from_string' in except_str and 'chunks[0]' in except_str:
                except_dict['POSCAR_PARSE'] = except_dict['POSCAR_PARSE']+1
            elif 'TypeError: integer argument expected, got float' in except_str:
                except_dict['MAXRUN_TYPE'] = except_dict['MAXRUN_TYPE']+1
            elif 'cannot import name DupeFinderDB' in except_str:
                except_dict['DUPEFINDER_DB'] = except_dict['DUPEFINDER_DB']+1
            elif 'jinja2' in except_str:
                except_dict['JINJA2'] = except_dict['JINJA2']+1
            elif 'run_tags' in except_str:
                except_dict['RUN_TAGS'] = except_dict['RUN_TAGS']+1
            else:
                except_dict[except_str] = except_dict[except_str]+1

    print '-----'
    for k, v in except_dict.iteritems():
        print {"{}\t{}".format(v, k)}

