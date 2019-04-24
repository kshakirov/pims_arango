from bottle import get, post, run, response
from json import dumps
import sys, bottle
sys.path.append('lib/')
from graph_manager import GraphManager


app = bottle.default_app()             # or bottle.Bottle() if you prefer
app.config['gm'] = GraphManager('localhost', 'root', 'servantes')


@app.post('/entity')
def index():
    return 200

@app.get('/entity/parent/fullpath')
def index():
    g_manager = app.config.get('gm')
    path = g_manager.get_all_parents(1)
    r = [{"path": path}]
    response.content_type = 'application/json'
    return dumps(r)




run(host='localhost', port=8081)
