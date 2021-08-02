import sys
import os
import requests
from .utils import log, Cache


class StreamController:
    def __init__(self, predictor, stream_in, stream_out):
        self.stream_in = stream_in
        self.stream_out = stream_out
        self.mindsdb_url = os.getenv("MINDSDB_URL") or "http://127.0.0.1:47334"
        self.company_id = os.getenv('COMPANY_ID')
        self.predictor = predictor
        self.predictors_url = "{}/api/predictors/".format(self.mindsdb_url)
        self.predict_url = "{}{}/predict".format(self.predictors_url, self.predictor)
        self.predictor_url = self.predictors_url + self.predictor
        if not self._is_predictor_exist():
            log.error("unable to get prediction - predictor %s doesn't exists", self.predictor)
            sys.exit(1)
        self.ts_settings = self._get_ts_settings()

    def work(self):
        is_timeseries = self.ts_settings.get('is_timeseries', False)
        predict_func = self._make_ts_predictions if is_timeseries else self._make_predictions
        predict_func()

    def _make_predictions(self):
        for data in self.stream_in.read():
            log.info("received input data: %s", data)
            try:
                prediction = self._predict(data)
                log.info("get predictions for %s: %s", data, prediction)
            except Exception as e:
                log.error("prediction error: %s", e)
            try:
                self.stream_out.write(prediction)
            except Exception as e:
                log.error("writing error: %s", e)


    def _make_ts_predictions(self):
        window = self.ts_settings['window']

        order_by = self.ts_settings['order_by']
        order_by = [order_by] if isinstance(order_by, str) else order_by

        group_by = self.ts_settings.get('group_by', None)
        group_by = [group_by] if isinstance(group_by, str) else group_by

        cache = Cache(f'{self.predictor}_cache')
        if group_by is None:

            if '' not in cache:
                cache[''] = []

            while True:
                with cache:
                    for when_data in self.stream_in.read():
                        for ob in order_by:
                            if ob not in when_data:
                                raise Exception(f'when_data doesn\'t contain order_by[{ob}]')

                        records = cache['']
                        records.append(when_data)
                        cache[''] = records

                    if len(cache['']) >= window:
                        cache[''] = [*sorted(
                            cache[''],
                            # WARNING: assuming wd[ob] is numeric
                            key=lambda wd: tuple(wd[ob] for ob in order_by)
                        )]
                        res_list = self._predict(when_data=cache[''][-window:])
                        self.stream_out.write(res_list[-1])
                        cache[''] = cache[''][1 - window:]
        else:

            while True:
                for when_data in self.stream_in.read():
                    for ob in order_by:
                        if ob not in when_data:
                            raise Exception(f'when_data doesn\'t contain order_by[{ob}]')

                    for gb in group_by:
                        if gb not in when_data:
                            raise Exception(f'when_data doesn\'t contain group_by[{gb}]')

                    gb_value = tuple(when_data[gb] for gb in group_by)

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
                            res_list = self._predict(when_data=cache[''][-window:])
                            self.stream_out.write(res_list[-1])
                            cache[gb_value] = cache[gb_value][1 - window:]

    def _predict(self, when_data):
        params = {"when": when_data}
        res = requests.post(self.predict_url, json=params)
        if res.status_code != requests.status_codes.codes.ok:
            raise Exception(f"unable to get prediction for {when_data}: {res.text}")
        return res.json()

    def _is_predictor_exist(self):
        res = requests.get(self.predictor_url)
        if res.status_code != requests.status_codes.codes.ok:
            return False
        return True

    def _get_ts_settings(self):
        res = requests.get(self.predictor_url)
        return res.json()['timeseries'] if res.status_code == requests.status_codes.codes.ok else {}
