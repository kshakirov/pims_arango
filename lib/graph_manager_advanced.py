from arango import ArangoClient
import itertools, json, os


class GraphManagerAdvanced:
    VERTEX_COLLECTION = 'entity'
    EDGE_COLLECTION = 'reference'
    DB = 'PimsDbFull'
    DUMP_FILE = 'dump.json'
    EDGE_FILE = 'dump_edge.json'

    def __init__(self, hostname, login, password):
        client = ArangoClient(protocol='http', host=hostname, port=8529)
        self.db = client.db(self.DB, username=login, password=password)
        self.pimsGraph = self.db.graph('PimsGraphFull')

    def filter_data_to_save(self, entity_data):
        return {k: v for k, v in entity_data.items() if not type(v) is dict}

    def filter_out_key(self, entity_data):
        return {k: v for k, v in entity_data.items() if not k == '_key'}

    def has_reference(self, parent_id, child_id):
        references = self.pimsGraph.edge_collection(self.EDGE_COLLECTION)
        key = '%(p_id)s_%(ch_id)d' % {'p_id': parent_id, 'ch_id': child_id}
        return references.has(key)

    def filter_references_to_batch(self, entity_data):
        entity_key = entity_data['_key']
        refs = {k: v for k, v in entity_data.items() if
                k == 'reference'}
        refs = {"key": entity_data['_key'], 'refs': refs}
        return refs

    def filter_references_to_bulk(self, entity_data):
        refs = {k: v for k, v in entity_data.items() if
                k == 'reference'}
        # refs = list(map(lambda r: r['id'], list(refs.values())))
        if 'reference' in refs:
            return refs['reference'].values()
        else:
            return []

    def update_entity(self, entity_key, entity_data):
        entities = self.pimsGraph.vertex_collection(self.VERTEX_COLLECTION)
        # entity = entities.get(str(key))
        entities.update(entity_data)

    def add_reference(self, parent_id, child_id):
        references = self.pimsGraph.edge_collection(self.EDGE_COLLECTION)
        edge_data = {'_key': '%(p_id)s_%(ch_id)d' % {'p_id': parent_id, 'ch_id': child_id},
                     '_from': 'entity/%(p_id)s' % {'p_id': parent_id}, '_to': 'entity/%(ch_id)d' % {'ch_id': child_id}}
        references.insert(edge_data)

    def prep_reference(self, ref_dict):
        parent_id = ref_dict['key']
        refs = []
        for key in ref_dict['refs']['reference']:
            child_id = ref_dict['refs']['reference'][key]
            ref = {'_key': '%(p_id)s_%(ch_id)d' % {'p_id': parent_id, 'ch_id': child_id},
                   '_from': 'entity/%(p_id)s' % {'p_id': parent_id}, '_to': 'entity/%(ch_id)d' % {'ch_id': child_id},
                   'edge_id': key}
            refs.append(ref)

        return refs

    def prep_entity_type_ref(self, entity):
        ref = {
            '_key': 'entity_type_%(entity_type_id)d_%(entity_id)s' % {'entity_type_id': entity['entity_type_id'],
                                                                      'entity_id': entity['_key']},
            '_from': 'entity/entity_type_%(entity_type_id)d' % {'entity_type_id': entity['entity_type_id']},
            '_to': 'entity/%(entity_id)s' % {'entity_id': entity['_key']},
            'edge_id': 0}
        return ref

    def filter_non_existing_references(self, entity_key, entity_data):
        ref_dict = {k: v for k, v in entity_data.items() if not self.has_reference(entity_key, v['_key'])}
        ref_list = list(map(lambda k: ref_dict[k]['id']))
        return ref_list

    def save_references(self, entity_key, entity_data):
        entities = self.pimsGraph.vertex_collection(self.VERTEX_COLLECTION)
        references_to_save = self.filter_references_to_save(entity_data)
        for ref in references_to_save:
            if not entities.has(str(ref)):
                entities.insert({'_key': str(ref)})
            self.add_reference(entity_key, ref)

    def insert_entity(self, entity_data):
        entity_key = entity_data['_key']
        entities = self.pimsGraph.vertex_collection(self.VERTEX_COLLECTION)
        data_to_save = self.filter_data_to_save(entity_data)
        entities.insert(data_to_save)
        self.save_references(entity_key, entity_data)

    def upsert_entity(self, entity_data):
        entities = self.pimsGraph.vertex_collection(self.VERTEX_COLLECTION)
        if not entities.has(str(entity_data['_key'])):
            self.insert_entity(entity_data)
        else:
            self.update_entity(entity_data['_key'], self.filter_data_to_save(entity_data))
        self.save_references(entity_data['_key'], entity_data)

    def prep_reference_bulk(self, entities_batch):
        references_data = list(map(lambda e: self.filter_references_to_bulk(e), entities_batch))
        references_data = list(filter(None, references_data))
        references_data = list(itertools.chain.from_iterable(list(map(lambda l: l, references_data))))
        return list(map(lambda l: {'_key': str(l)}, references_data))

    def prep_reference_batch(self, entities_batch):
        references_data = list(map(lambda e: self.filter_references_to_batch(e), entities_batch))
        references_data = list(filter(lambda l: not l['refs'] == {}, references_data))
        return list(map(lambda l: self.prep_reference(l), references_data))

    def import_bulk(self, entities_batch):
        entities = self.db.collection(self.VERTEX_COLLECTION)
        references_data = self.prep_reference_bulk(entities_batch)
        entities_data = list(map(lambda e: self.filter_data_to_save(e), entities_batch))
        entities.import_bulk(entities_data + references_data, halt_on_error=False, details=True, from_prefix=None,
                             to_prefix=None,
                             overwrite=None,
                             on_duplicate='update', sync=None)

    def create_entity_type_reference(self, entities_batch):
        return list(map(lambda e: self.prep_entity_type_ref(e),entities_batch))

    def upsert_batch(self, entities_batch):
        self.import_bulk(entities_batch)
        references = self.prep_reference_batch(entities_batch)
        entity_references = self.create_entity_type_reference(entities_batch)
        with self.db.begin_batch_execution(return_result=False) as batch_db:
            batch_col = batch_db.collection(self.EDGE_COLLECTION)
            for ref in (references + entity_references):
                batch_col.insert(ref)
