import os
import json

from ..base import BaseStream


class TestStream(BaseStream):
    def __init__(self, filename):
        self.filename = filename

    def read(self):
        if not os.path.exists(self.filename):
            return
        with open(self.filename, 'r') as f:
            for line in f:
                yield json.loads(line.rstrip())
        with open(self.filename, 'w') as _:
            pass

    def write(self, dct):
        with open(self.filename, 'a') as f:
            f.write(json.dumps(dct))
            f.write('\n')
            f.flush()

    def __del__(self):
        try:
            os.remove(self.filename)
        except Exception:
            pass

    def __repr__(self):
        return f"{self.__class__.__name__}: file={self.filename}"
