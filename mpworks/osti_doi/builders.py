import requests, json, os, datetime, logging
from matgendb.builders.core import Builder
from osti_record import OstiRecord
from bs4 import BeautifulSoup
import plotly.plotly as py
from plotly.graph_objs import *

today = datetime.date.today()
dirname = os.path.dirname(os.path.realpath(__file__))
backupfile = os.path.join(dirname, 'dois.json')
logfile = os.path.join(dirname, 'logs', 'dois_{}.log'.format(today))
_log = logging.getLogger('mg.build')
_log.setLevel(logging.INFO)
fh = logging.FileHandler(logfile)
fh.setLevel(logging.INFO)
formatter = logging.Formatter('####### %(asctime)s #######\n%(message)s')
fh.setFormatter(formatter)
_log.addHandler(fh)

stream_ids = ['645h22ynck', '96howh4ip8', 'nnqpv5ra02']
py.sign_in(
    os.environ.get('MP_PLOTLY_USER'),
    os.environ.get('MP_PLOTLY_APIKEY'),
    stream_ids=stream_ids
)

class DoiBuilder(Builder):
    """Builder to obtain DOIs for all/new materials"""

    def get_items(self, nmats=2, dois=None, materials=None):
        """DOIs + Materials iterator

        :param nmats: number of materials for which to request DOIs
        :type nmats: int
        :param dois: 'dois' collection in 'mg_core_dev/prod'
        :type dois: QueryEngine
        :param materials: 'materials' collection in 'mg_core_dev/prod'
        :type materials: QueryEngine
        """
        self.nmats = nmats
        self.doi_qe = dois
        self.mat_qe = materials
        self.headers = {'Accept': 'text/bibliography; style=bibtex'}
        # loop the mp-id's
        # w/o valid DOI in doicoll *OR*
        # w/ valid DOI in doicoll but w/o doi key in matcoll
        mp_ids = [
            {'_id': doc['_id'], 'doi': doc['doi'], 'valid': False}
            for doc in self.doi_qe.collection.find({'valid': False})
        ]
        valid_mp_ids = self.doi_qe.collection.find({'valid': True}).distinct('_id')
        missing_mp_ids = self.mat_qe.collection.find(
            {'task_id': {'$in': valid_mp_ids}, 'doi': {'$exists': False}},
            {'_id': 0, 'task_id': 1}
        ).distinct('task_id')
        mp_ids += list(self.doi_qe.collection.find(
            {'_id': {'$in': missing_mp_ids}},
            {'doi': 1, 'valid': 1, 'bibtex': 1}
        ))
        return mp_ids

    def process_item(self, item):
        """validate DOI, save bibtex and build into matcoll"""
        if not item['valid']:
            #doi_url = 'http://doi.org/{}'.format(item['doi'])
            #doi_url = 'http://dx.doi.org/10.1038/nrd842'
            #r = requests.get(doi_url, headers=self.headers)
            if item['doi'] is None:
                # try loading doi from backup file, a.k.a reset item['doi'] (fixed manually)
                if os.path.exists(backupfile):
                    with open(backupfile, 'r') as infile:
                        data = json.load(infile)
                        for d in data:
                            if d['_id'] == item['_id'] and d['doi'] is not None:
                                item['doi'] = d['doi']
                                _log.info(self.doi_qe.collection.update(
                                    {'_id': item['_id']}, {'$set': {'doi': item['doi']}}
                                ))
                                break
                # if mp-id not found in backup (not fixed manually)
                if item['doi'] is None:
                    _log.warning('missing DOI for {}. Fix manually in dois.json and rerun!'.format(item['_id']))
                    return 0
            osti_id = item['doi'].split('/')[-1]
            doi_url = 'http://www.osti.gov/dataexplorer/biblio/{}/cite/bibtex'.format(osti_id)
            try:
                r = requests.get(doi_url)
            except Exception as ex:
                _log.warning('validation exception: {} -> {} -> {}'.format(
                    item['_id'], item['doi'], ex
                ))
                return 0
            _log.info('validate {} -> {} -> {}'.format(item['_id'], item['doi'], r.status_code))
            if r.status_code == 200:
                soup = BeautifulSoup(r.content, "html.parser")
                rows = soup.find_all('div', attrs={"class" : "csl-entry"})
                if len(rows) == 1:
                    bibtex = rows[0].text.strip()
                    _log.info(self.doi_qe.collection.update(
                        {'_id': item['_id']}, {'$set': {
                            'valid': True, 'bibtex': bibtex
                        }}
                    ))
                    # only validated DOIs are ready to be built into matcoll
                    _log.info(self.mat_qe.collection.update(
                        {'task_id': item['_id']}, {'$set': {
                            'doi': item['doi'], 'doi_bibtex': bibtex
                        }}
                    ))
        else:
            _log.info('re-build {} -> {}'.format(item['_id'], item['doi']))
            _log.info(self.mat_qe.collection.update(
                {'task_id': item['_id']}, {'$set': {
                    'doi': item['doi'], 'doi_bibtex': item['bibtex']
                }}
            ))

    def finalize(self, errors):
        osti_record = OstiRecord(
            n=self.nmats,
            doicoll=self.doi_qe.collection,
            matcoll=self.mat_qe.collection
        )
        osti_record.submit()
        with open(backupfile, 'w') as outfile:
            l = list(self.doi_qe.collection.find(
                fields={'created_at': True, 'doi': True}
            ))
            json.dump(l, outfile, indent=2)
        # push results to plotly streaming graph
        counts = [
            self.mat_qe.collection.count(),
            self.doi_qe.collection.count(),
            len(osti_record.matad.get_all_dois())
        ]
        for idx,stream_id in enumerate(stream_ids):
            s = py.Stream(stream_id)
            s.open()
            s.write(dict(x=today, y=counts[idx]))
            s.close()
        return True
