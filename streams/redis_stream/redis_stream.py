import json
import walrus

from ..base import BaseStream


class RedisStream(BaseStream):
    def __init__(self, stream, **connection_info):
        self.client = walrus.Database(**connection_info)
        self.stream = self.client.Stream(stream)

    @staticmethod
    def _decode(redis_data):
        decoded = {}
        for k in redis_data:
            decoded[k.decode('utf8')] = redis_data[k].decode('utf8')
        return decoded

    def read(self):
        for k, when_data in self.stream.read():
            res = when_data if isinstance(when_data, dict) else json.loads(when_data)
            yield self._decode(res)
            self.stream.delete(k)

    def write(self, dct):
        self.stream.add({'': json.dumps(dct)})
