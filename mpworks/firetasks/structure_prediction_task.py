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

class StructurePredictionTask(FireTaskBase, FWSerializable):
    _fw_name = "Structure Prediction Task"
    
    def run_task(self, fw_spec):
        db = SPStructuresMongoAdapter.auto_load()
        tstructs = []
        species = fw_spec['species']
        t = fw_spec['threshold']
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
                    tstructs.append(ts)

        transmuter = StandardTransmuter(tstructs)
        f = RemoveDuplicatesFilter(structure_matcher=StructureMatcher(
                                    comparator=ElementComparator(),
                                    primitive_cell=False))
        transmuter.apply_filter(f)
        results = []
        for ts in transmuter.transformed_structures:
            results.append(ts.to_snl([]).to_dict)
        submissions = SPSubmissionsMongoAdapter.auto_load()
        submissions.insert_results(fw_spec['submission_id'],
                                   results)