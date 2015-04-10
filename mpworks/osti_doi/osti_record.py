import os
import requests
import logging
from pymongo import MongoClient
from monty.serialization import loadfn
from collections import OrderedDict
from dicttoxml import dicttoxml
from xml.dom.minidom import parseString
from pybtex.database.input import bibtex
from StringIO import StringIO
from xmltodict import parse

class MaterialsAdapter(object):
    """adapter to connect to materials database and collection"""
    def __init__(self, db_yaml='materials_db_dev.yaml'):
        self.doi_key = 'doi'
        config = loadfn(os.path.join(os.environ['DB_LOC'], db_yaml))
        client = MongoClient(config['host'], config['port'], j=False)
        client[config['db']].authenticate(config['username'], config['password'])
        self.matcoll = client[config['db']].materials

    def _reset(self):
        """remove `doi` keys from all documents"""
        logging.info(self.matcoll.update(
            {self.doi_key: {'$exists': 1}},
            {'$unset': {'contributed_data': 1}},
            multi=True
        ))

    def get_materials_cursor(l, n):
        if l is None:
            return self.matcoll.find({self.doi_key: {'$exists': False}}, limit=n)
        else:
            mp_ids = [ 'mp-{}'.format(el) for el in l ]
            return self.matcoll.find({'task_id': {'$in': mp_ids}})

    def get_osti_id(mat):
        osti_id = '' # empty osti_id = new submission -> new DOI
        # check for existing doi to distinguish from edit/update scenario
        if self.doi_key in mat and mat[self.doi_key] is not None:
            osti_id = mat[self.doi_key].split('/')[-1]
        return osti_id

class OstiRecord(object):
    """object defining a MP-specific record for OSTI"""
    def __init__(self, l=None, n=0):
        self.endpoint = 'https://www.osti.gov/elinktest/2416api' # TODO move to prod
        self.bibtex_parser = bibtex.Parser()
        self.matad = MaterialsAdapter() # TODO: move to materials_db_prod
        self.materials = get_materials_cursor(l, n)
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
                ('description', 'Computed materials data using density functional theory calculations. These calculations determine the electronic structure of bulk materials by solving approximations to the Schrodinger equation. For more information, see https://materialsproject.org/docs/calculations')
            ]))
        self.records_xml = parseString(dicttoxml(
            self.records, custom_root='records', attr_type=False
        ))
        items = self.records_xml.getElementsByTagName('item')
        for item in items:
            self.records_xml.renameNode(item, '', item.parentNode.nodeName[:-1])
        logging.info(self.records_xml.toprettyxml())

    def submit(self):
        """submit generated records to OSTI"""
        #r = requests.post(
        #    self.endpoint, data=self.records_xml.toxml(),
        #    auth=(os.environ['OSTI_USER'], os.environ['OSTI_PASSWORD'])
        #)
        content = '<?xml version="1.0" encoding="UTF-8"?><records><record><osti_id>1282772</osti_id><product_nos>mp-12661</product_nos><title>Materials Data on Cd3In (SG:221) by Materials Project</title><contract_nos>AC02-05CH11231; EDCBEE</contract_nos><doi>10.15483/1282772</doi><status>SUCCESS</status><status_message></status_message></record></records>'
        # TODO content -> r.content
        for record in parse(content)['records'].itervalues():
            if record['status'] == 'SUCCESS':
                doi = 'http://doi.org/' + record['doi']
                # TODO save to mat_db
                logging.info(doi)
            else:
                logging.warning('ERROR for %s: %s' % (
                    record['product_nos'], record['status_message']
                ))

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
