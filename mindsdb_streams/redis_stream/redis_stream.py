import json
import walrus

from ..base import BaseStream


class RedisStream(BaseStream):
    def __init__(self, stream, connection_info):
        if isinstance(connection_info, str):
            self.connection_info = json.loads(connection_info)
        else:
            self.connection_info = connection_info
        self.client = walrus.Database(**self.connection_info)
        self.stream = self.client.Stream(stream)

    @staticmethod
    def _decode(redis_data):
        decoded = {}
        for k in redis_data:
            decoded[k.decode('utf8')] = redis_data[k].decode('utf8')
        return decoded

    def read(self):
        for k, when_data in self.stream.read():
            try:
                res = json.loads(when_data[b''])
            except KeyError:
                res = self._decode(when_data)
            yield res
            self.stream.delete(k)

    def write(self, dct):
        self.stream.add({'': json.dumps(dct)})

    def __repr__(self):
        return f"{self.__class__.__name__}: stream={self.stream}, connection={self.connection_info}"
