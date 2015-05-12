import requests, json, os, datetime, glob
from matgendb.builders.core import Builder
from matgendb.builders.util import get_builder_log
from osti_record import OstiRecord
from bs4 import BeautifulSoup

_log = get_builder_log('osti_doi')

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
        osti_record = OstiRecord(
            n=nmats, doicoll=dois.collection, matcoll=materials.collection
        )
        osti_record.submit()
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
            osti_id = item['doi'].split('/')[-1]
            doi_url = 'http://www.osti.gov/dataexplorer/biblio/{}/cite/bibtex'.format(osti_id)
            r = requests.get(doi_url)
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
        dirname = os.path.dirname(os.path.realpath(__file__))
        filenames = glob.glob(os.path.join(dirname, 'dois_*.json'))
        filename = 'dois_{}.json'.format(datetime.date.today())
        filepath = os.path.join(dirname, filename)
        with open(filepath, 'w') as outfile:
            l = list(self.doi_qe.collection.find(
                fields={'created_at': True, 'doi': True}
            ))
            json.dump(l, outfile, indent=2)
            for path in filenames:
                if path != filepath:
                    os.remove(path)
        return True
