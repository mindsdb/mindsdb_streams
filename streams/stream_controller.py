import os
import requests
from .utils import log


class StreamController:
    def __init__(self, predictor, stream_in, stream_out):
        self.stream_in = stream_in
        self.stream_out = stream_out
        self.mindsdb_url = os.getenv("MINDSDB_URL") or "http://127.0.0.1:47334"
        self.company_id = os.getenv('COMPANY_ID')
        self.predictor = predictor
        self.predictors_url = "{}/api/predictors/".format(self.mindsdb_url)
        self.predict_url = "{}{}/predict".format(self.predictors_url, self.predictor)

    def work(self):
        while True:
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

    def _predict(self, when_data):
        params = {"when": when_data}
        res = requests.post(self.predict_url, json=params)
        if res.status_code != requests.status_codes.codes.ok:
            raise Exception(f"unable to get prediction for {when_data}: {res.text}")
        return res.json()

    def _is_predictor_exist(self):
        res = requests.get(self.predictors_url)
        if res.status_code != requests.status_codes.codes.ok:
            raise Exception(f"unable to get predictors list: {res.text}")
        predictors = [x["name"] for x in res.json()]
        return self.predictor in predictors
