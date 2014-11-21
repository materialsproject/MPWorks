import os
from pymongo import MongoClient
from monty.serialization import loadfn
from collections import OrderedDict
from dicttoxml import dicttoxml
from xml.dom.minidom import parseString
from pybtex.database.input import bibtex
from StringIO import StringIO

class OstiRecord(object):
    def __init__(self, mp_id, db_yaml='materials_db_dev.yaml'):
        self.bibtex_parser = bibtex.Parser()
        config = loadfn(os.path.join(os.environ['DB_LOC'], db_yaml))
        client = MongoClient(config['host'], config['port'], j=False)
        client[config['db']].authenticate(config['username'], config['password'])
        materials = client[config['db']].materials
        self.mp_id = mp_id
        self.material = materials.find_one({'task_id': mp_id})
        research_org = 'Lawrence Berkeley National Laboratory (LBNL), Berkeley, CA (United States)'
        self.record_dict = OrderedDict([
            ('osti_id', ''), # empty = new submission -> new DOI
            ('dataset_type', 'SM'),
            ('title', self._get_title()),
            ('creators', 'Kristin Persson'),
            ('product_nos', self.mp_id),
            ('contract_nos', 'AC02-05CH11231; EDCBEE'),
            ('originating_research_org', research_org),
            ('publication_date', self._get_publication_date()),
            ('language', 'English'),
            ('country', 'US'),
            ('sponsor_org', 'USDOE Office of Science (SC), Basic Energy Sciences (BES) (SC-22)'),
            ('site_url', self._get_site_url()),
            ('contact_name', 'Kristin Persson'),
            ('contact_org', 'LBNL'),
            ('contact_email', 'kapersson@lbl.gov'),
            ('contact_phone', '+1(510)486-7218'),
            ('related_resource', 'https://materialsproject.org/citing'),
            ('contributor_organizations', 'TODO'), # not listed in research_org
            ('subject_categories_code', '36 MATERIALS SCIENCE'),
            ('keywords', self._get_keywords()),
            ('description', 'We use density functional theory as implemented in the Vienna Ab Initio Simulation Package (VASP) software to evaluate the total energy of compounds. Input structures are taken from the Inorganic Crystal Structure Database (ICSD), and all cell and atomic positions relaxed twice in our calculations using the AFLOW software package. For more, see https://materialsproject.org/docs/calculations')
        ])
        self.record_xml = parseString(dicttoxml(
            {'record': self.record_dict}, custom_root='records', attr_type=False
        )).toprettyxml()

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

    def _get_site_url(self):
        return 'https://materialsproject.org/materials/%s' % self.mp_id

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
