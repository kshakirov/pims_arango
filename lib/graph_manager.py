from arango import ArangoClient


class GraphManager:
    VERTEX_COLLECTION = 'entity'
    EDGE_COLLECTION = 'reference'

    def __init__(self, hostname, login, password):
        client = ArangoClient(protocol='http', host=hostname, port=8529)
        self.db = client.db('PimsDb', username=login, password=password)
        self.pimsGraph = self.db.graph('PimsGraph')

    def add_entity(self, entity_data):
        entities = self.pimsGraph.vertex_collection(self.VERTEX_COLLECTION)
        entities.insert({'_key': str(entity_data['id']), 'name': entity_data['name']})

    def add_reference(self,parent_id, child_id):
        references = self.pimsGraph.edge_collection(self.EDGE_COLLECTION)
        edge_data = {'_key': '%(p_id)d_%(ch_id)d'%{'p_id': parent_id, 'ch_id':child_id},
                                '_from': 'entity/%(p_id)d'%{'p_id': parent_id},'_to': 'entity/%(ch_id)d'%{'ch_id': child_id }}
        references.insert(edge_data)

    def add_pair(self,entity_data):
        entities = self.pimsGraph.vertex_collection(self.VERTEX_COLLECTION)
        if not entities.has(str(entity_data['id'])):
            self.add_entity(entity_data)
        elif not entities.has(str(entity_data['reference_id'])):
            self.add_entity({'id': entity_data['reference_id'],'name': None })
        else:
            print()
        self.add_reference(entity_data['reference_id'], entity_data['id'])
    def get_all_parents(self,entity_id):
        response = self.pimsGraph.traverse(start_vertex='entity/%(id)d'%{'id':entity_id},
                                direction='inbound', strategy='bfs',
                                edge_uniqueness='global',
                                vertex_uniqueness='global')


        return  ':'.join(map(lambda  r: str(r['name']),response['vertices']))





