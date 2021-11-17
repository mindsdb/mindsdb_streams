import os
import time
import traceback
from threading import Event, Thread
from tempfile import NamedTemporaryFile
import requests
import pandas as pd
from .utils import log, Cache

class BaseController:
    def __init__(self, name, predictor, stream_in, stream_out):
        self.name = name
        self.stream_in = stream_in
        self.stream_out = stream_out
        self.predictor = predictor

        try:
            from mindsdb.utilities.config import Config
            config = Config()
            self.mindsdb_url = f"http://{config['api']['http']['host']}:{config['api']['http']['port']}"
        except (ImportError, KeyError):
            self.mindsdb_url = os.getenv("MINDSDB_URL") or "http://127.0.0.1:47334"
        self.company_id = os.environ.get('MINDSDB_COMPANY_ID') or os.getenv('COMPANY_ID') or None
        log.info("%s: environment variables: MINDSDB_URL=%s\tCOMPANY_ID=%s",
                    self.name, self.mindsdb_url, self.company_id)
        self.headers = {'Content-Type': 'application/json'}
        if self.company_id is not None:
            self.headers['company-id'] = self.company_id
        self.mindsdb_api_root = self.mindsdb_url + "/api"
        self.predictors_url = "{}/predictors/".format(self.mindsdb_api_root)
        self.predict_url = "{}{}/predict".format(self.predictors_url, self.predictor)
        self.predictor_url = self.predictors_url + self.predictor

        self.stop_event = Event()

    def _is_predictor_exist(self):
        max_attempt = 3
        cur_attempt = 0
        while cur_attempt < max_attempt:
            try:
                res = requests.get(self.predictor_url, headers=self.headers)
                if res.status_code == requests.status_codes.codes.ok:
                    return True
            except Exception as e:
                log.debug("%s: error getting predictor(%s) info - %s", self.name, self.predictor, e)
            cur_attempt += 1
            time.sleep(1)
        return False


class StreamController(BaseController):
    def __init__(self, name, predictor, stream_in, stream_out, stream_anomaly=None, in_thread=False):
        super().__init__(name, predictor, stream_in, stream_out)
        self.stream_anomaly = stream_anomaly
        log.info("%s: creating controller params: predictor=%s, stream_in=%s, stream_out=%s, stream_anomaly=%s",
                    self.name, self.predictor, self.stream_in, self.stream_out, self.stream_anomaly)
        self.predictors_url = "{}/api/predictors/".format(self.mindsdb_url)
        self.predict_url = "{}{}/predict".format(self.predictors_url, self.predictor)
        self.predictor_url = self.predictors_url + self.predictor
        if not self._is_predictor_exist():
            log.error("%s: unable to get prediction - predictor %s doesn't exists", self.name, self.predictor)
        self.ts_settings = self._get_ts_settings()
        log.info("%s: timeseries settings - %s", self.name, self.ts_settings)

        if in_thread:
            self.thread = Thread(target=StreamController.work, args=(self,))
            self.thread.start()

    def work(self):
        is_timeseries = self.ts_settings.get('is_timeseries', False)
        log.info("%s: is_timeseries - %s", self.name, is_timeseries)
        predict_func = self._make_ts_predictions if is_timeseries else self._make_predictions
        predict_func()

    def _make_predictions(self):
        while not self.stop_event.wait(0.5):
            for data in self.stream_in.read():
                log.debug("%s: received input data - %s", self.name, data)
                try:
                    prediction = self._predict(data)
                    log.debug("%s: get predictions for %s - %s", self.name, data, prediction)
                except Exception as e:
                    log.error("%s: prediction error - %s", self.name, e)
                try:

                    prediction = prediction if isinstance(prediction, list) else [prediction, ]
                    for item in prediction:
                        if self.stream_anomaly is not None and self._is_anomaly(item):
                            self.stream_anomaly.write(item)
                        else:
                            self.stream_out.write(item)
                except Exception as e:
                    log.error("%s: writing error - %s", self.name, e)


    @staticmethod
    def _is_anomaly(res):
        for k in res:
            if k.endswith('_anomaly') and res[k] is not None:
                return True
        return False

    def _make_ts_predictions(self):
        window = self.ts_settings['window']

        order_by = self.ts_settings['order_by']
        order_by = [order_by] if isinstance(order_by, str) else order_by

        group_by = self.ts_settings.get('group_by', None)
        if group_by is None:
            group_by = []
        group_by = [group_by] if isinstance(group_by, str) else group_by

        cache = Cache(f'{self.predictor}_cache')
        while not self.stop_event.wait(0.5):
            for when_data in self.stream_in.read():
                log.debug("%s: received input data - %s", self.name, when_data)
                for ob in order_by:
                    if ob not in when_data:
                        raise Exception(f'when_data doesn\'t contain order_by[{ob}]')

                for gb in group_by:
                    if gb not in when_data:
                        raise Exception(f'when_data doesn\'t contain group_by[{gb}]')

                gb_value = tuple(when_data[gb] for gb in group_by) if group_by else ''

                # because cache doesn't work for tuples
                # (raises Exception: tuple doesn't have "encode" attribute)
                gb_value = str(gb_value)

                with cache:
                    if gb_value not in cache:
                        log.debug("%s: creating cache for gb - %s", self.name, gb_value)
                        cache[gb_value] = []

                    # do this because shelve-cache doesn't support
                    # in-place changing
                    records = cache[gb_value]
                    log.debug("%s: adding to the cache - %s", self.name, when_data)
                    records.append(when_data)
                    cache[gb_value] = records

            with cache:
                for gb_value in cache.keys():
                    if len(cache[gb_value]) >= window:
                        cache[gb_value] = [*sorted(
                            cache[gb_value],
                            # WARNING: assuming wd[ob] is numeric
                            key=lambda wd: tuple(wd[ob] for ob in order_by)
                        )]
                        while len(cache[gb_value]) >= window:
                            log.debug("%s: windows - %s, cache size - %s", self.name, window, len(cache[gb_value]))

                            res_list = self._predict(when_data=cache[gb_value][:window])
                            if self.stream_anomaly is not None and self._is_anomaly(res_list[-1]):
                                log.debug("%s: writing '%s' as prediction result to anomaly stream",
                                            self.name, res_list[-1])
                                self.stream_anomaly.write(res_list[-1])
                            else:
                                log.debug("%s: writing '%s' as prediction result to output stream",
                                            self.name, res_list[-1])
                                self.stream_out.write(res_list[-1])
                            cache[gb_value] = cache[gb_value][1:]

    def _predict(self, when_data):
        params = {"when": when_data, 'format_flag': 'dict'}
        res = requests.post(self.predict_url, json=params, headers=self.headers)
        if res.status_code != requests.status_codes.codes.ok:
            raise Exception(f"unable to get prediction for {when_data}: {res.text}")
        return res.json()

    def _get_ts_settings(self):
        res = requests.get(self.predictor_url, headers=self.headers)
        try:
            ts_settings = res.json()['problem_definition']['timeseries_settings']
        except KeyError as e:
            log.error("%s - api error: unable to get timeseries settings for %s url - %s",
                    self.name, self.predictor_url, e)
            ts_settings = {}
        return ts_settings if res.status_code == requests.status_codes.codes.ok else {}


class StreamLearningController(BaseController):
    def __init__(self, name, predictor, learning_params, learning_threshold, stream_in, stream_out, in_thread=False):
        super().__init__(name, predictor, stream_in, stream_out)
        self.learning_params = learning_params
        self.learning_threshold = learning_threshold
        self.learning_data = []
        self.training_ds_name = "{}_training_ds_{}".format(self.predictor,
                                                           time.strftime("%Y-%m-%d_%H-%M-%S",
                                                                         time.gmtime()))
        log.info("%s: learning controller params: predictor=%s, learning_params=%s, learning_threshold=%s, stream_in=%s, stream_out=%s",
                    self.name, self.predictor, self.learning_params, self.learning_threshold, self.stream_in, self.stream_out)

        # for consistency only
        self.stop_event = Event()

        if in_thread:
            self.thread = Thread(target=StreamLearningController.work, args=(self,))
            self.thread.start()

    def work(self):
        self._learn_model()

    def _upload_ds(self, df):
        with NamedTemporaryFile(mode='w+', newline='', delete=False) as f:
            df.to_csv(f, index=False)
            f.flush()
            url = f'{self.mindsdb_api_root}/datasources/{self.training_ds_name}'
            data = {
                "source_type": (None, 'file'),
                "file": (f.name, f, 'text/csv'),
                "source": (None, f.name.split('/')[-1]),
                "name": (None, self.training_ds_name)
            }
            res = requests.put(url, files=data)
            res.raise_for_status()

    def _collect_training_data(self):
        threshold = time.time() + self.learning_threshold
        while time.time() < threshold:
            self.learning_data.extend(self.stream_in.read())
            time.sleep(0.2)
        return pd.DataFrame.from_records(self.learning_data)

    def _cleanup(self):
        delete_url = self.mindsdb_api_root + "/streams/" + self.name
        res = requests.delete(delete_url)
        log.debug("%s: delete '%s' - code: %s, text: %s", self.name, delete_url, res.status_code, res.text)

    def _learn_model(self):
        msg = {"action": "training", "predictor": self.predictor,
                "status": "", "details": ""}

        try:
            if self._is_predictor_exist():
                datetime_suffix = time.strftime("%Y-%m-%d_%H-%M-%S", time.gmtime())
                predictor_name = f"TMP_{self.predictor}_{datetime_suffix}"
            else:
                predictor_name = self.predictor

            df = self._collect_training_data()
            self._upload_ds(df)
            self.learning_params['data_source_name'] = self.training_ds_name
            if 'kwargs' not in self.learning_params:
                self.learning_params['kwargs'] = {}
            self.learning_params['kwargs']['join_learn_process'] = True
            url = f'{self.mindsdb_api_root}/predictors/{predictor_name}'
            res = requests.put(url, json=self.learning_params)
            res.raise_for_status()

            if predictor_name != self.predictor:
                delete_url = f'{self.mindsdb_api_root}/predictors/{self.predictor}'
                rename_url = f'{self.mindsdb_api_root}/predictors/{predictor_name}/rename?new_name={self.predictor}'
                res = requests.delete(delete_url)
                res.raise_for_status()
                res = requests.get(rename_url)
                res.raise_for_status()
            msg["status"] = "success"
        except Exception:
            msg["status"] = "error"
            msg["details"] = traceback.format_exc()
        self.stream_out.write(msg)

        # Need to delete its own record from db to mark it as outdated
        # for integration, which will delete it from 'active threads' after that (local installation)
        # for pod_manager, which will delete this pod from kubernetes cluster (cloud)
        self._cleanup()
