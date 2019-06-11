from arango import ArangoClient


class GraphManagerAdvanced:
    VERTEX_COLLECTION = 'entity'
    EDGE_COLLECTION = 'reference'
    DB = 'PimsDbFull'

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

    def filter_references_to_save(self, entity_data):
        entity_key = entity_data['_key']
        refs = {k: v for k, v in entity_data.items() if k == 'reference' and  not self.has_reference(entity_key,v['id'])}
        refs = list(map(lambda r: r['id'], list(refs.values())))
        return refs

    def update_entity(self, entity_key, entity_data):
        entities = self.pimsGraph.vertex_collection(self.VERTEX_COLLECTION)
        # entity = entities.get(str(key))
        entities.update(entity_data)

    def add_reference(self, parent_id, child_id):
        references = self.pimsGraph.edge_collection(self.EDGE_COLLECTION)
        edge_data = {'_key': '%(p_id)s_%(ch_id)d' % {'p_id': parent_id, 'ch_id': child_id},
                     '_from': 'entity/%(p_id)s' % {'p_id': parent_id}, '_to': 'entity/%(ch_id)d' % {'ch_id': child_id}}
        references.insert(edge_data)

    def filter_non_existing_references(self, entity_key, entity_data):
        ref_dict = {k: v for k, v in entity_data.items() if not self.has_reference(entity_key, v['_key'])}
        ref_list = list(map(lambda k: ref_dict[k]['id']))
        return ref_list

    def save_references(self, entity_key, entity_data):
        references_to_save = self.filter_references_to_save(entity_data)
        for ref in references_to_save:
            self.add_reference(entity_key, ref)

    def add_entity(self, entity_data):
        entity_key = entity_data['_key']
        entities = self.pimsGraph.vertex_collection(self.VERTEX_COLLECTION)
        data_to_save = self.filter_data_to_save(entity_data)
        entities.insert(data_to_save)
        self.save_references(entity_key, entity_data)

    def add_pair(self, entity_data):
        entities = self.pimsGraph.vertex_collection(self.VERTEX_COLLECTION)
        if not entities.has(str(entity_data['_key'])):
            self.add_entity(entity_data)
        else:
            self.update_entity(entity_data['_key'], self.filter_data_to_save(entity_data))
        self.save_references(entity_data['_key'], entity_data)

    def add_pairs(self, entites):
        for entity in entites:
            self.add_pair(entity)

    def get_all_parents(self, entity_id):
        response = self.pimsGraph.traverse(start_vertex='entity/%(id)s' % {'id': entity_id},
                                           direction='inbound', strategy='bfs',
                                           edge_uniqueness='global',
                                           vertex_uniqueness='global')

        return ':'.join(map(lambda r: str(r['name']), response['vertices']))

    def get_all_parents_with_id(self, entity_id):
        response = self.pimsGraph.traverse(start_vertex='entity/%(id)s' % {'id': entity_id},
                                           direction='inbound', strategy='bfs',
                                           edge_uniqueness='global',
                                           vertex_uniqueness='global')

        return ':'.join(map(lambda r: r['_key'] + '-' + str(r['name']), response['vertices']))
