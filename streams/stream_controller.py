import sys
import os
from threading import Event
import requests
from .utils import log, Cache


class StreamController:
    def __init__(self, predictor, stream_in, stream_out, stream_anomaly=None):
        self.stream_in = stream_in
        self.stream_out = stream_out
        self.stream_anomaly = stream_anomaly
        self.predictor = predictor
        log.info("creating controller params: predictor=%s, stream_in=%s, stream_out=%s, stream_anomaly=%s",
                    self.predictor, self.stream_in, self.stream_out, self.stream_anomaly)
        self.mindsdb_url = os.getenv("MINDSDB_URL") or "http://127.0.0.1:47334"
        self.company_id = os.getenv('COMPANY_ID') or None
        log.info("environment variables: MINDSDB_URL=%s\tCOMPANY_ID=%s",
                    self.mindsdb_url, self.company_id)
        self.headers = {'Content-Type': 'application/json'}
        if self.company_id is not None:
            self.headers['company-id'] = self.company_id
        self.predictors_url = "{}/api/predictors/".format(self.mindsdb_url)
        self.predict_url = "{}{}/predict".format(self.predictors_url, self.predictor)
        self.predictor_url = self.predictors_url + self.predictor
        if not self._is_predictor_exist():
            log.error("unable to get prediction - predictor %s doesn't exists", self.predictor)
            sys.exit(1)
        self.ts_settings = self._get_ts_settings()
        self.stop_event = Event()

    def work(self):
        is_timeseries = self.ts_settings.get('is_timeseries', False)
        log.info(f"is_timeseries: {is_timeseries}")
        predict_func = self._make_ts_predictions if is_timeseries else self._make_predictions
        predict_func()

    def _make_predictions(self):
        while not self.stop_event.wait(0.5):
            for data in self.stream_in.read():
                log.debug("received input data: %s", data)
                try:
                    prediction = self._predict(data)
                    log.debug("get predictions for %s: %s", data, prediction)
                except Exception as e:
                    log.error("prediction error: %s", e)
                try:

                    prediction = prediction if isinstance(prediction, list) else [prediction, ]
                    for item in prediction:
                        if self.stream_anomaly is not None and self._is_anomaly(item):
                            self.stream_anomaly.write(item)
                        else:
                            self.stream_out.write(item)
                except Exception as e:
                    log.error("writing error: %s", e)


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
        group_by = [group_by] if isinstance(group_by, str) else group_by

        cache = Cache(f'{self.predictor}_cache')
        while not self.stop_event.wait(0.5):
            for when_data in self.stream_in.read():
                log.debug("received input data: %s", when_data)
                for ob in order_by:
                    if ob not in when_data:
                        raise Exception(f'when_data doesn\'t contain order_by[{ob}]')

                for gb in group_by:
                    if gb not in when_data:
                        raise Exception(f'when_data doesn\'t contain group_by[{gb}]')

                gb_value = tuple(when_data[gb] for gb in group_by) if group_by is not None else ''

                # because cache doesn't work for tuples
                # (raises Exception: tuple doesn't have "encode" attribute)
                gb_value = str(gb_value)

                with cache:
                    if gb_value not in cache:
                        cache[gb_value] = []

                    # do this because shelve-cache doesn't support
                    # in-place changing
                    records = cache[gb_value]
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

                            res_list = self._predict(when_data=cache[gb_value][:window])
                            if self.stream_anomaly is not None and self._is_anomaly(res_list[-1]):
                                log.debug("writing %s as prediction result to anomaly stream",
                                            res_list[-1])
                                self.stream_anomaly.write(res_list[-1])
                            else:
                                log.debug("writing %s as prediction result to output stream",
                                            res_list[-1])
                                self.stream_out.write(res_list[-1])
                            cache[gb_value] = cache[gb_value][1:]

    def _predict(self, when_data):
        params = {"when": when_data, 'format_flag': 'dict'}
        res = requests.post(self.predict_url, json=params, headers=self.headers)
        if res.status_code != requests.status_codes.codes.ok:
            raise Exception(f"unable to get prediction for {when_data}: {res.text}")
        return res.json()

    def _is_predictor_exist(self):
        res = requests.get(self.predictor_url, headers=self.headers)
        if res.status_code != requests.status_codes.codes.ok:
            return False
        return True

    def _get_ts_settings(self):
        res = requests.get(self.predictor_url, headers=self.headers)
        try:
            ts_settings = res.json()['problem_definition']['timeseries_settings']
        except KeyError as e:
            log.error("api error: unable to get timeseries settings for %s url - %s", self.predictor_url, e)
            ts_settings = {}
        return ts_settings if res.status_code == requests.status_codes.codes.ok else {}
