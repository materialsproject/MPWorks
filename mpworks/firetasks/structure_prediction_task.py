from mpworks.structure_prediction.prediction_mongo import SPStructuresMongoAdapter, \
                                                          SPSubmissionsMongoAdapter
from pymatgen.structure_prediction.substitution_probability import SubstitutionPredictor
from pymatgen.transformations.standard_transformations import SubstitutionTransformation
from pymatgen.analysis.structure_matcher import StructureMatcher, ElementComparator
from pymatgen.alchemy.filters import RemoveDuplicatesFilter
from pymatgen.alchemy.transmuters import StandardTransmuter
from pymatgen.alchemy.materials import TransformedStructure
from fireworks.utilities.fw_serializers import FWSerializable
from fireworks.core.firework import FireTaskBase, FWAction
from pymatgen import Specie
from pymatgen.io.cifio import CifWriter
from pymatpro.abitbx.utils import _space_group_info_to_dict, find_symmetry
from pymatgen import write_structure


import subprocess
import json
import os

class StructurePredictionTask(FireTaskBase, FWSerializable):
    _fw_name = "Structure Prediction Task"
    
    def run_task(self, fw_spec):
        db = SPStructuresMongoAdapter.auto_load()
        structures = []
        species = []
        for el, oxi_states in fw_spec['element_oxidation_states'].items():
            for oxi in oxi_states:
                species.append(Specie(el, int(oxi)))
        t = float(fw_spec['threshold'])
        for p in SubstitutionPredictor(threshold = t).list_prediction(species):
            subs = p['substitutions']
            if len(set(subs.values())) < len(species):
                continue
            st = SubstitutionTransformation(subs)
            target = map(str, subs.keys())
            for snl in db.get_snls(target):
                ts = TransformedStructure.from_snl(snl)
                ts.append_transformation(st)
                if ts.final_structure.charge == 0:
                    structures.append({'ts': ts,
                                       'probability' : p['probability']})
        
        #remove duplicates, keeping highest probability
        sm = StructureMatcher(comparator=ElementComparator(),
                              primitive_cell=False)
        structures.sort(key = lambda x: x['probability'], reverse = True)
        filtered_structs = []
        for s in structures:
            found = False
            for s2 in filtered_structs:
                if sm.fit(s['ts'], s2['ts']):
                    found = True
                    break
            if not found:
                filtered_structs.append(s)
        
        results = []
        for i, s in enumerate(filtered_structs):
            entry = s['ts'].to_snl({}).to_dict
            entry['sp_crystal_id'] = i
            entry['pretty_formula'] = s['ts'].composition.reduced_formula
            entry['probability'] = s['probability']
            entry['cif'] = str(CifWriter(s['ts'].final_structure))
            write_structure(s['ts'].final_structure, 'prediction_symmetry.cif')
            subprocess.check_call(["cctbx.python", 
                                   os.path.join(os.path.dirname(os.path.abspath(__file__)), "structure_prediction_cctbx.py")])
            with open('symmetrydict.json') as f:
                entry['space_group'] = json.load(f)
            results.append(entry)
        
        
        
        submissions = SPSubmissionsMongoAdapter.auto_load()
        submissions.insert_results(fw_spec['structure_predictor_id'],
                                   results)