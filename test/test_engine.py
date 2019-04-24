import sys
sys.path.append('/home/kshakirov/PycharmProjects/python_box/lib')
from engine import Engine
e = Engine()
convs = [[
    {"id": 1},
    {"id": 2},
    {"id": 4}
]]

response = e.run(convs)
print(response)