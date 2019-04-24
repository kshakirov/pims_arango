class Engine:
    def read_converter (self,converter):
        body = converter["id"] + 1
        return body

    def converters(self, ids):
        return list(map(self.read_converter, ids))

    def evaluate(self, convs):
        map(lambda c:print(c),convs)

    def _process(selfs,name, inp, request_id):
        klass = name
        print(klass)

    def process(self,inp, converter_names, request_id):
        list(map(lambda n:self._process(n,inp,request_id),converter_names))


    def _run(self, arg):
        convs = self.converters(arg["ids"])
        self.evaluate(convs)


    def run(self, arguments):
        args = {"result": list(map(self._run, arguments))}
        return args
