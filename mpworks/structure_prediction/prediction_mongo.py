import json
import os
import datetime
import logging

from materials_django.utils import connector
from materials_django.settings import PRODUCTION

from pymongo import MongoClient, ASCENDING, DESCENDING
from pymatgen import Composition, Specie, Structure
from pymatgen.matproj.snl import StructureNL

import yaml

__author__ = 'William Richards'
__copyright__ = 'Copyright 2013, The Materials Project'
__version__ = '0.1'
__maintainer__ = 'William Richards'
__email__ = 'wrichard@mit.edu'
__date__ = 'Jun 10, 2013'

class SPStructuresMongoAdapter(object):
    # This is the user interface to prediction starting structures
    
    def __init__(self, db):
        """
        Args:
            db:
                authenticated connection to the apps_db
        """
        self.db = db
        self.coll = db.sp_structures
        self.ensure_indices()

    def ensure_indices(self):
        self.coll.ensure_index([('_materialsproject.nspecies', ASCENDING),
                                ('_materialsproject.species', ASCENDING)])

    def get_structures(self, species):
        o = []
        for e in self.coll.find({'_materialsproject.nspecies' : len(species), 
                                 '_materialsproject.species' : {'$all' : species}}):
            o.append(Structure.from_dict(e))
        return o
    
    def insert_structure(self, structure):
        assert isinstance(structure, Structure)
        species = set()
        oxi_decorated = True
        for site in structure:
            for sp in site.species_and_occu.keys():
                oxi_decorated &= isinstance(sp, Specie)
                species.add(sp)
                
        if not oxi_decorated:
            logging.debug("Oxidation state decorating " + str(structure.composition))
            from pymatgen.transformations.standard_transformations import AutoOxiStateDecorationTransformation  
            t = AutoOxiStateDecorationTransformation()
            structure = t.apply_transformation(structure)
            
        species = set()
        for site in structure:
            for sp in site.species_and_occu.keys():
                oxi_decorated &= isinstance(sp, Specie)
                species.add(str(sp))
        
        d = structure.to_dict
        d['_materialsproject'] = {'nspecies': len(species), 'species': list(species)}
        
        self.coll.insert(d)
    
    @classmethod
    def auto_load(cls):
        return cls(connector.app_db(PRODUCTION))


class SPSubmissionsMongoAdapter(object):
    # This is the user interface to prediction submissions

    def __init__(self, db):
        """
        Args:
            db:
                authenticated connection to the apps_db
        """
        self.db = db
        self.pred_coll = db.sp_predictions
        self.results_coll = db.sp_results
        self.id_coll = db.id_counters
        self.ensure_indices()
        
    def ensure_indices(self):
        self.pred_coll.drop_indexes()
        self.pred_coll.ensure_index('structure_predictor_id', unique=True)
        self.results_coll.ensure_index([('structure_predictor_id', ASCENDING),
                                        ('crystal_id', ASCENDING)],
                                       unique=True)
        self.id_coll.ensure_index('collection', unique=True)
        
        #initialize counter
        if not self.id_coll.find_one({'collection': 'sp_predictions'}):
            max_e = self.pred_coll.find_one({}, {'structure_predictor_id':1}, 
                                            sort=[('structure_predictor_id', DESCENDING)])
            self.id_coll.insert({'collection': 'sp_predictions',
                                 'next_id' : 1})
        
    def submit_prediction(self, elements, element_oxidation_states, threshold, submitter_email):
        sp_id = self.id_coll.find_and_modify({'collection': 'sp_predictions'},
                                             {'$inc' : {'next_id': 1}})['next_id']
        d = {'submitter_email': submitter_email,
             'email': submitter_email,
             'user_name': submitter_email,
             'state': 'SUBMITTED',
             'state_details': {},
             'submitted_at': datetime.datetime.utcnow().isoformat(),
             'elements': elements,
             'element_oxidation_states': element_oxidation_states,
             'threshold': threshold,
             'structure_predictor_id': sp_id}
        self.pred_coll.insert(d)
        return sp_id
    
    def insert_results(self, submission_id, results):
        """
        results is a list of snl dictionaries with additional keys:
            _materialsproject.probability
            _materialsproject.crystal_id
        """
        sp_id = int(submission_id)
        self.pred_coll.update({'structure_predictor_id': sp_id},
                         {'$set' : {'ncrystals': len(results),
                                    'state': 'COMPLETED',
                                    'completed_at': datetime.datetime.utcnow().isoformat()}})
        for r in results:
            r['structure_predictor_id'] = sp_id
            self.results_coll.insert(r)
    
    @classmethod
    def auto_load(cls):
        return cls(connector.app_db(PRODUCTION))

