from bottle import get, post, run, response, request
from json import dumps
import sys, bottle

sys.path.append('lib/')
from graph_manager import GraphManager

app = bottle.default_app()  # or bottle.Bottle() if you prefer
app.config['gm'] = GraphManager('localhost', 'root', 'servantes')


@app.post('/entity')
def index():
    return 200


@app.get('/entity/<id>/fullpath')
def index(id):
    g_manager = app.config.get('gm')
    path = g_manager.get_all_parents(id)
    r = [{"path": path}]
    response.content_type = 'application/json'
    return dumps(r)


@app.post('/entity/parent/add')
def index():
    body = request.json
    print(body)
    g_manager = app.config.get('gm')
    g_manager.add_pair(body)


run(host='localhost', port=8081)
