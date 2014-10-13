__author__ = 'Anubhav Jain'
__copyright__ = 'Copyright 2014, The Materials Project'
__version__ = '0.1'
__maintainer__ = 'Anubhav Jain'
__email__ = 'ajain@lbl.gov'
__date__ = 'Oct 13, 2014'


def deprecate_snl(snl_db, snl_id, remarks):
    remarks.append('DEPRECATED')
    # FIX SNL
    remarks.extend(snl_db.snl.find_one({'snl_id': snl_id}, {'about.remarks': 1})['about']['remarks'])
    remarks = list(set(remarks))

    # push existing remarks
    print('PUSH these', remarks)
    snl_db.snl.update({'snl_id': snl_id}, {'$set': {"about.remarks": remarks}})

    # FIX SNLGROUPS
    sg = snl_db.snlgroups.find_one({'canonical_snl.snl_id': snl_id}, {'snlgroup_id': 1}).get('snlgroup_id')
    if sg is not None:
        snl_db.snlgroups.update({'snlgroup_id': sg}, {'$set': {"canonical_snl.about.remarks": remarks}})
        print('also need to update snlgroup {}'.format(sg))