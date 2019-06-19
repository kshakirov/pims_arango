from bottle import get, post, run, response, request
from json import dumps
import sys, bottle,time

sys.path.append('lib/')
from graph_manager_advanced import GraphManagerAdvanced

app = bottle.default_app()  # or bottle.Bottle() if you prefer
app.config['gm'] = GraphManagerAdvanced('localhost', 'root', 'servantes')


@app.get('/entity')
def index():
    return 200


@app.post('/entity/')
def index():
    body = request.json
    g_manager = app.config.get('gm')
    g_manager.upsert_entity(body)


@app.post('/entity/batch')
def index():
    entities = request.json
    g_manager = app.config.get('gm')
    g_manager.upsert_batch(entities)


app = application = app
