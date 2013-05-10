import datetime
from pymatgen import Structure, PMGJSONDecoder, Molecule, Composition
from pymatgen.analysis.structure_matcher import StructureMatcher, ElementComparator
from pymatgen.matproj.snl import StructureNL

__author__ = 'Anubhav Jain'
__copyright__ = 'Copyright 2013, The Materials Project'
__version__ = '0.1'
__maintainer__ = 'Anubhav Jain'
__email__ = 'ajain@lbl.gov'
__date__ = 'Apr 24, 2013'

# TODO: document


def get_meta_from_structure(structure):
    comp = structure.composition
    elsyms = sorted(set([e.symbol for e in comp.elements]))
    # TODO: this won't work for molecules

    # the complex logic set/list is to prevent duplicates if there are
    # multiple oxidation states
    meta = {'nsites': len(structure.sites),
            'elements': elsyms,
            'nelements': len(elsyms),
            'formula': comp.formula,
            'reduced_cell_formula': comp.reduced_formula,
            'reduced_cell_formula_abc': Composition.from_formula(comp
                .reduced_formula).alphabetical_formula,
            'composition_dict': comp.to_dict,
            'anonymized_formula': comp.anonymized_formula,
            'chemsystem': '-'.join(elsyms),
            'is_ordered': structure.is_ordered,
            'is_valid': structure.is_valid()}

    return meta


class MPStructureNL(StructureNL):
    # adds snl_id, spacegroup, and autometa properties to StructureNL.

    def __init__(self, *args, **kwargs):
        super(MPStructureNL, self).__init__(*args, **kwargs)
        if not self.sg_num:
            raise ValueError('An MPStructureNL must have a spacegroup assigned!')
        self.snl_autometa = get_meta_from_structure(self.structure)

    @property
    def snl_id(self):
        return self.data['_materialsproject']['snl_id']

    @property
    def sg_num(self):
        return self.data['_materialsproject']['spacegroup']['number']

    @property
    def snlgroup_key(self):
        return self.snl_autometa['reduced_cell_formula_abc'] + "--" + str(self.sg_num)

    @property
    def to_dict(self):
        m_dict = super(MPStructureNL, self).to_dict
        m_dict.update(self.snl_autometa)
        m_dict['snl_id'] = self.snl_id
        m_dict['snlgroup_key'] = self.snlgroup_key
        return m_dict

    @staticmethod
    def from_dict(d):
        a = d["about"]
        dec = PMGJSONDecoder()

        created_at = dec.process_decoded(a["created_at"]) if "created_at" in a \
            else None
        data = {k: v for k, v in d["about"].items()
                if k.startswith("_")}
        data = dec.process_decoded(data)

        structure = Structure.from_dict(d) if "lattice" in d \
            else Molecule.from_dict(d)
        return MPStructureNL(structure, a["authors"],
                           projects=a.get("projects", None),
                           references=a.get("references", ""),
                           remarks=a.get("remarks", None), data=data,
                           history=a.get("history", None),
                           created_at=created_at)

    @staticmethod
    def from_snl(snl, snl_id, sg_num, sg_symbol, hall, xtal_system, lattice_type, pointgroup):
        # make a copy of SNL
        snl2 = StructureNL.from_dict(snl.to_dict)
        if '_materialsproject' not in snl2.data:
            snl2.data['_materialsproject'] = {}

        snl2.data['_materialsproject']['snl_id'] = snl_id
        snl2.data['_materialsproject']['spacegroup'] = {}
        sg = snl2.data['_materialsproject']['spacegroup']
        sg['symbol'] = sg_symbol
        sg['number'] = sg_num
        sg['point_group'] = pointgroup
        sg['crystal_system'] = xtal_system
        sg['hall'] = hall
        sg['lattice_type'] = lattice_type

        return MPStructureNL.from_dict(snl2.to_dict)


class SNLGroup():
    def __init__(self, snlgroup_id, canonical_snl, all_snl_ids=None):
        # Auto fields
        self.created_at = datetime.datetime.utcnow()
        self.updated_at = datetime.datetime.utcnow()

        # User fields
        self.snlgroup_id = snlgroup_id
        self.canonical_snl = canonical_snl

        self.all_snl_ids = all_snl_ids if all_snl_ids else []
        if self.canonical_snl.snl_id not in self.all_snl_ids:
            self.all_snl_ids.append(canonical_snl.snl_id)

        # Convenience fields
        self.canonical_structure = canonical_snl.structure
        self.snl_autometa = get_meta_from_structure(self.canonical_structure)

    @property
    def to_dict(self):
        d = self.snl_autometa
        d['created_at'] = self.created_at
        d['updated_at'] = self.updated_at
        d['snlgroup_id'] = self.snlgroup_id
        d['canonical_snl'] = self.canonical_snl.to_dict
        d['all_snl_ids'] = self.all_snl_ids
        d['num_snl'] = len(self.all_snl_ids)

        d['snlgroup_key'] = self.canonical_snl.snlgroup_key
        return d

    @staticmethod
    def from_dict(d):
        return SNLGroup(d['snlgroup_id'], MPStructureNL.from_dict(d['canonical_snl']), d['all_snl_ids'])

    def add_if_belongs(self, cand_snl):

        # no need to compare if different formulas or spacegroups
        if cand_snl.snlgroup_key != self.canonical_snl.snlgroup_key:
            return False

        # no need to compare if one is ordered, the other disordered
        if not (cand_snl.structure.is_ordered == self.canonical_structure.is_ordered):
            return False

        # make sure the structure is not already in all_structures
        if cand_snl.snl_id in self.all_snl_ids:
            print 'WARNING: add_if_belongs() has detected that you are trying to add the same SNL id twice!'
            return False

        #try a structure fit to the canonical structure

        # use default Structure Matcher params from April 24, 2013, as suggested by Shyue
        # we are using the ElementComparator() because this is how we want to group results
        sm = StructureMatcher(ltol=0.2, stol=0.3, angle_tol=5, primitive_cell=True, scale=True, attempt_supercell=False, comparator=ElementComparator())

        if not sm.fit(cand_snl.structure, self.canonical_structure):
            return False

        # everything checks out, add to the group
        self.all_snl_ids.append(cand_snl.snl_id)
        self.updated_at = datetime.datetime.utcnow()

        return True