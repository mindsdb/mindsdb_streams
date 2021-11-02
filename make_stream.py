import sys
import os
import json
import argparse
from mindsdb_streams import KafkaStream, RedisStream, StreamController, StreamLearningController


parser = argparse.ArgumentParser()
parser.add_argument('connection_info', type=str, help="json string with connection info")
parser.add_argument('predictor', type=str, help="predictor model name")
parser.add_argument('input_stream', type=str)
parser.add_argument('output_stream', type=str)
parser.add_argument('anomaly_stream', type=str)
parser.add_argument('type', type=str.lower, choices=['kafka', 'redis'])
parser.add_argument('learning_params', type=str,
        help="json string with model learning params")
parser.add_argument('learning_threshold', type=int,
        help="timeout for collecting training data")


if __name__ == '__main__':
    for i, v in enumerate(sys.argv):
        print(f"{i}:\t{v}")
    sys.stdout.flush()
    stream_name = os.getenv("STREAM_NAME", "Foo")
    args = parser.parse_args()
    connection_info = json.loads(args.connection_info)

    stream_class = RedisStream if args.type == 'redis' else KafkaStream
    stream_out = stream_class(args.output_stream, connection_info)
    stream_in = stream_class(args.input_stream, connection_info)
    stream_anomaly = stream_class(args.anomaly_stream, connection_info) if args.anomaly_stream not in ('', None, 'None', 'none') else None
    if args.learning_params and args.learning_threshold:
        controller = StreamLearningController(stream_name,
                                              args.predictor,
                                              args.learning_params,
                                              args.learning_threshold,
                                              stream_in,
                                              stream_out)
    else:

        controller = StreamController(stream_name,
                                      args.predictor,
                                      stream_in,
                                      stream_out,
                                      stream_anomaly)

    print(f"Created '{controller.__class__.__name__}' controller, stream name - {stream_name}")
    controller.work()
