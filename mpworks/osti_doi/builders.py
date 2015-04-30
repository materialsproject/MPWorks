import requests
from matgendb.builders.core import Builder
from matgendb.builders.util import get_builder_log

_log = get_builder_log('osti_doi')

class DoiValidator(Builder):
    """validate DOIs against CrossRef and built into materials collection"""

    def get_items(self, dois=None, materials=None):
        """DOIs iterator

        :param dois: 'dois' collection in 'mg_core_dev/prod'
        :type dois: QueryEngine
        :param materials: 'materials' collection in 'mg_core_dev/prod'
        :type materials: QueryEngine
        """
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
        """validate DOI via CrossRef, save bibtex and build into matcoll"""
        if not item['valid']:
            doi_url = 'http://doi.org/{}'.format(item['doi'])
            #doi_url = 'http://dx.doi.org/10.1038/nrd842'
            r = requests.get(doi_url, headers=self.headers)
            _log.info('validate {} -> {} -> {}'.format(item['_id'], item['doi'], r.status_code))
            if r.status_code == 200:
                _log.info(self.doi_qe.collection.update(
                    {'_id': item['_id']}, {'$set': {
                        'valid': True, 'bibtex': r.content
                    }}
                ))
                # only validated DOIs are ready to be built into matcoll
                _log.info(self.mat_qe.collection.update(
                    {'task_id': item['_id']}, {'$set': {
                        'doi': item['doi'], 'doi_bibtex': r.content
                    }}
                ))
        else:
            _log.info('re-build {} -> {}'.format(item['_id'], item['doi']))
            _log.info(self.mat_qe.collection.update(
                {'task_id': item['_id']}, {'$set': {
                    'doi': item['doi'], 'doi_bibtex': item['bibtex']
                }}
            ))
