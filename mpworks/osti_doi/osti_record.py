import os
import requests
import logging
import datetime
from pymongo import MongoClient
from monty.serialization import loadfn
from collections import OrderedDict
from dicttoxml import dicttoxml
from xml.dom.minidom import parseString
from pybtex.database.input import bibtex
from StringIO import StringIO
from xmltodict import parse

logger = logging.getLogger('osti')

class OstiMongoAdapter(object):
    """adapter to connect to materials database and collection"""
    def __init__(self, db):
        self.doi_key = 'doi'
        self.matcoll = db.materials
        self.doicoll = db.dois

    @classmethod
    def from_config(cls, db_yaml='materials_db_dev.yaml'):
        config = loadfn(os.path.join(os.environ['DB_LOC'], db_yaml))
        client = MongoClient(config['host'], config['port'], j=False)
        db = client[config['db']]
        db.authenticate(config['username'], config['password'])
        return OstiMongoAdapter(db)

    def _reset(self):
        """remove `doi` keys from matcoll, clear and reinit doicoll"""
        # NOTE: make sure all existing DOIs are listed here for reinit
        logger.info(self.doicoll.remove())
        logger.info(self.doicoll.insert([
            {'_id': 'mp-12661', 'doi': '10.17188/1178752', 'valid': False,
             'created_at': datetime.datetime.utcnow().isoformat()},
            {'_id': 'mp-20379', 'doi': '10.17188/1178753', 'valid': False,
             'created_at': datetime.datetime.utcnow().isoformat()},
            {'_id': 'mp-4', 'doi': '10.17188/1178763', 'valid': False,
             'created_at': datetime.datetime.utcnow().isoformat()},
        ]))

    def get_all_dois(self):
        dois = {}
        for doc in self.matcoll.find(
            {self.doi_key: {'$exists': True}},
            {'_id': 0, 'task_id': 1, self.doi_key: 1}
        ):
            dois[doc['task_id']] = doc[self.doi_key]['doi']
        return dois

    def get_materials_cursor(self, l, n):
        if l is None:
            return self.matcoll.find({self.doi_key: {'$exists': False}}, limit=n)
        else:
            mp_ids = [ 'mp-{}'.format(el) for el in l ]
            return self.matcoll.find({'task_id': {'$in': mp_ids}})

    def get_osti_id(self, mat):
        # empty osti_id = new submission -> new DOI
        # check for existing doi to distinguish from edit/update scenario
        doi_entry = self.doicoll.find_one({'_id': mat['task_id']})
        return '' if doi_entry is None else doi_entry['doi'].split('/')[-1]

    def insert_dois(self, dois):
        """save doi info to doicoll, only record update time if exists"""
        dois_insert = [
            {'_id': mpid, 'doi': d['doi'], 'valid': False,
             'created_at': datetime.datetime.utcnow().isoformat()}
            for mpid,d in dois.iteritems() if not d['updated']
        ]
        if dois_insert: logger.info(self.doicoll.insert(dois_insert))
        dois_update = [ mpid for mpid,d in dois.iteritems() if d['updated'] ]
        if dois_update:
            logger.info(self.doicoll.update(
                {'_id': {'$in': dois_update}},
                {'$set': {'updated_at': datetime.datetime.utcnow().isoformat()}},
                multi=True
            ))

    def validate_dois(self):
        """for invalid DOIs: validate via CrossRef and save bibtex"""
        headers = {'Accept': 'text/bibliography; style=bibtex'}
        for doc in self.matcoll.find(
            {'{}.valid'.format(self.doi_key): False},
            {'_id': 0, self.doi_key: 1, 'task_id': 1}
        ):
            r = requests.get(doc[self.doi_key]['url'], headers=headers)
            if r.status_code == 200:
                logger.info(self.matcoll.update(
                    {'task_id': doc['task_id']}, {'$set': {
                        # valid DOIs are ready to be synced to matcoll_prod
                        '{}.valid'.format(self.doi_key): True,
                        '{}.synced'.format(self.doi_key): False,
                        '{}.bibtex'.format(self.doi_key): r.content,
                    }}
                ))

    def sync_dois(self):
        """sync valid and unsynced DOIs to matcoll_prod"""
        raise NotImplementedError("TODO")

class OstiRecord(object):
    """object defining a MP-specific record for OSTI"""
    def __init__(self, l=None, n=0):
        self.endpoint = 'https://www.osti.gov/elink/2416api'
        self.bibtex_parser = bibtex.Parser()
        self.matad = OstiMongoAdapter.from_config()
        self.materials = self.matad.get_materials_cursor(l, n)
        research_org = 'Lawrence Berkeley National Laboratory (LBNL), Berkeley, CA (United States)'
        self.records = []
        for material in self.materials:
            self.material = material
            # prepare record
            self.records.append(OrderedDict([
                ('osti_id', self.matad.get_osti_id(material)),
                ('dataset_type', 'SM'),
                ('title', self._get_title()),
                ('creators', 'Kristin Persson'),
                ('product_nos', self.material['task_id']),
                ('contract_nos', 'AC02-05CH11231; EDCBEE'),
                ('originating_research_org', research_org),
                ('publication_date', self._get_publication_date()),
                ('language', 'English'),
                ('country', 'US'),
                ('sponsor_org', 'USDOE Office of Science (SC), Basic Energy Sciences (BES) (SC-22)'),
                ('site_url', self._get_site_url(self.material['task_id'])),
                ('contact_name', 'Kristin Persson'),
                ('contact_org', 'LBNL'),
                ('contact_email', 'kapersson@lbl.gov'),
                ('contact_phone', '+1(510)486-7218'),
                ('related_resource', 'https://materialsproject.org/citing'),
                ('contributor_organizations', 'MIT; UC Berkeley; Duke; U Louvain'), # not listed in research_org
                ('subject_categories_code', '36 MATERIALS SCIENCE'),
                ('keywords', self._get_keywords()),
                ('description', 'Computed materials data using density '
                 'functional theory calculations. These calculations determine '
                 'the electronic structure of bulk materials by solving '
                 'approximations to the Schrodinger equation. For more '
                 'information, see https://materialsproject.org/docs/calculations')
            ]))
            if not self.records[-1]['osti_id']:
                self.records[-1].pop('osti_id', None)
        self.records_xml = parseString(dicttoxml(
            self.records, custom_root='records', attr_type=False
        ))
        items = self.records_xml.getElementsByTagName('item')
        for item in items:
            self.records_xml.renameNode(item, '', item.parentNode.nodeName[:-1])
        logger.info(self.records_xml.toprettyxml())

    def submit(self):
        """submit generated records to OSTI"""
        r = requests.post(
            self.endpoint, data=self.records_xml.toxml(),
            auth=(os.environ['OSTI_USER'], os.environ['OSTI_PASSWORD'])
        )
        logger.info(r.content)
        records = parse(r.content)['records']['record']
        records = [ records ] if not isinstance(records, list) else records
        dois = {}
        for ridx,record in enumerate(records):
            if record['status'] == 'SUCCESS':
                dois[record['product_nos']] = {
                    'doi': record['doi'],
                    'updated': bool('osti_id' in self.records[ridx])
                }
            else:
                logger.warning('ERROR for %s: %s' % (
                    record['product_nos'], record['status_message']
                ))
        #dois = {
        #    u'mp-12661': {'updated': True, 'doi': u'10.17188/1178752'},
        #    u'mp-20379': {'updated': True, 'doi': u'10.17188/1178753'},
        #    u'mp-4': {'updated': False, 'doi': u'10.17188/1178763'},
        #}
        self.matad.insert_dois(dois)

    def _get_title(self):
        formula = self.material['pretty_formula']
        sg_num = self.material['spacegroup']['number']
        return 'Materials Data on %s (SG:%d) by Materials Project' % (
            formula, sg_num
        )

    def _get_creators(self):
        creators = []
        for author in self.material['snl_final']['about']['authors']:
            names = author['name'].split()
            last_name = names[-1]
            first_name = ' '.join(names[:-1])
            creators.append(', '.join([last_name, first_name]))
        return '; '.join(creators)

    def _get_publication_date(self):
        return self.material['created_at'].strftime('%m/%d/%Y')

    def _get_site_url(self, mp_id):
        return 'https://materialsproject.org/materials/%s' % mp_id

    def _get_related_resource(self):
        bib_data = self.bibtex_parser.parse_stream(StringIO(
            self.material['snl_final']['about']['references']
        ))
        related_resource = []
        for entry in bib_data.entries.values():
            related_resource.append(entry.fields.get('url'))
        return ', '.join(filter(None, related_resource))

    def _get_keywords(self):
        keywords = '; '.join([
            'crystal structure',
            self.material['snl_final']['reduced_cell_formula_abc'],
            self.material['snl_final']['chemsystem'],
            '; '.join([
                '-'.join(['ICSD', str(iid)]) for iid in self.material['icsd_ids']
            ]),
        ])
        keywords += '; electronic bandstructure' if self.material['has_bandstructure'] else ''
        return keywords
