from bottle import get, post, run, response
from json import dumps


@post('/lambda/python/converters')
def index():
    r = [{"id": 1}]
    response.content_type = 'application/json'
    return dumps(r)


run(host='localhost', port=8081)
