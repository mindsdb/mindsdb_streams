import os
import time
import atexit
import unittest
import tempfile
import threading
from subprocess import Popen

import psutil
import requests
import pandas as pd
from mindsdb_streams import TestStream, StreamController


HTTP_API_ROOT = "http://127.0.0.1:47334/api"
DS_NAME = 'test_datasource'
DEFAULT_PREDICTOR = "stream_predictor"
TS_PREDICTOR = "stream_ts_predictor"
DEFAULT_STREAM_NAME = "FOO"
TS_STREAM_NAME = DEFAULT_STREAM_NAME + "_TS"
NO_GROUP_STREAM_NAME = DEFAULT_STREAM_NAME + "_NO_GROUP"


def stop_mindsdb(ppid):
    pprocess = psutil.Process(ppid)
    pids = [x.pid for x in pprocess.children(recursive=True)]
    pids.append(ppid)
    for pid in pids:
        try:
            os.kill(pid, 9)
        # process may be killed by OS due to some reasons in that moment
        except ProcessLookupError:
            pass


def run_mindsdb():
    sp = Popen(['python3', '-m', 'mindsdb'], close_fds=True)
    time.sleep(30)
    atexit.register(stop_mindsdb, sp.pid)


class StreamTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        run_mindsdb()


    def upload_ds(self, name):
        df = pd.DataFrame({
            'group': ["A" for _ in range(100, 210)],
            'order': [x for x in range(100, 210)],
            'x1': [x for x in range(100,210)],
            'x2': [x*2 for x in range(100,210)],
            'y': [x*3 for x in range(100,210)]
        })
        with tempfile.NamedTemporaryFile(mode='w+', newline='', delete=False) as f:
            df.to_csv(f, index=False)
            f.flush()
            url = f'{HTTP_API_ROOT}/datasources/{name}'
            data = {
                "source_type": (None, 'file'),
                "file": (f.name, f, 'text/csv'),
                "source": (None, f.name.split('/')[-1]),
                "name": (None, name)
            }
            res = requests.put(url, files=data)
            res.raise_for_status()

    def train_predictor(self, ds_name, predictor_name):
        params = {
            'data_source_name': ds_name,
            'to_predict': 'y',
            'kwargs': {
                'stop_training_in_x_seconds': 20,
                'join_learn_process': True
            }
        }
        url = f'{HTTP_API_ROOT}/predictors/{predictor_name}'
        res = requests.put(url, json=params)
        res.raise_for_status()

    def train_ts_predictor(self, ds_name, predictor_name, with_gb=True):
        ts_settings = {
            "order_by": ["order"],
            "nr_predictions": 1,
            "use_previous_target": True,
            "window": 10}

        if with_gb:
            ts_settings["group_by"] = ["group"]
        else:
            ts_settings["group_by"] = []
        params = {
            'data_source_name': ds_name,
            'to_predict': 'y',
            'kwargs': {
                'use_gpu': False,
                'join_learn_process': True,
                'ignore_columns': None,
                'timeseries_settings': ts_settings,
            }
        }
        url = f'{HTTP_API_ROOT}/predictors/{predictor_name}'
        res = requests.put(url, json=params)
        res.raise_for_status()

    def test_1_check_test_stream(self):
        print(f"\nExecuting {self._testMethodName}")
        stream = TestStream('test_stream_length')

        self.assertEqual(len(list(stream.read())), 0)

        stream.write({'0': 0})

        self.assertEqual(len(list(stream.read())), 1)

        stream.write({'1': 1})
        stream.write({'2': 2})

        self.assertEqual(len(list(stream.read())), 2)
        self.assertEqual(len(list(stream.read())), 0)

    def test_2_predictions_through_controller(self):
        print(f"\nExecuting {self._testMethodName}")
        self.upload_ds(DS_NAME)
        self.train_predictor(DS_NAME, DEFAULT_PREDICTOR)
        stream_in = TestStream(f'{self._testMethodName}_in')
        stream_out = TestStream(f'{self._testMethodName}_out')
        controller = StreamController(DEFAULT_STREAM_NAME, DEFAULT_PREDICTOR, stream_in, stream_out)
        controller_thread = threading.Thread(target=controller.work, args=())

        time.sleep(1)
        for x in range(1, 3):
            stream_in.write({'x1': x, 'x2': 2*x})

        controller_thread.start()
        time.sleep(2)
        controller.stop_event.set()

        self.assertEqual(len(list(stream_out.read())), 2)

    def test_3_ts_predictions_through_controller(self):
        print(f"\nExecuting {self._testMethodName}")
        self.train_ts_predictor(DS_NAME, TS_PREDICTOR)
        stream_in = TestStream(f'{self._testMethodName}_in')
        stream_out = TestStream(f'{self._testMethodName}_out')
        controller = StreamController(TS_STREAM_NAME, TS_PREDICTOR, stream_in, stream_out)
        controller_thread = threading.Thread(target=controller.work, args=())

        controller_thread.start()
        for x in range(210, 221):
            stream_in.write({'x1': x, 'x2': 2*x, 'order': x, 'group': "A"})


        time.sleep(2)
        controller.stop_event.set()
        self.assertEqual(len(list(stream_out.read())), 2)

    def test_4_ts_predictions_through_controller_no_group(self):
        print(f"\nExecuting {self._testMethodName}")
        PREDICTOR_NAME = TS_PREDICTOR + "_no_group"
        self.train_ts_predictor(DS_NAME, PREDICTOR_NAME, with_gb=False)
        stream_in = TestStream(f'{self._testMethodName}_in')
        stream_out = TestStream(f'{self._testMethodName}_out')
        controller = StreamController(NO_GROUP_STREAM_NAME, PREDICTOR_NAME, stream_in, stream_out)
        controller_thread = threading.Thread(target=controller.work, args=())

        controller_thread.start()
        for x in range(210, 221):
            stream_in.write({'x1': x, 'x2': 2*x, 'order': x})


        time.sleep(2)
        controller.stop_event.set()
        self.assertEqual(len(list(stream_out.read())), 2)


if __name__ == "__main__":
    try:
        unittest.main(failfast=True)
        print('Tests passed!')
    except Exception as e:
        print(f'Tests Failed!\n{e}')
