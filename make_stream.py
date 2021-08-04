import sys
import json
import argparse
from streams import KafkaStream, RedisStream, StreamController


parser = argparse.ArgumentParser()
parser.add_argument('--connection_info', type=str, help="json string with connection info", required=True)
parser.add_argument('--predictor', type=str, help="predictor model name", required=True)
parser.add_argument('--input_stream', type=str, required=True)
parser.add_argument('--output_stream', type=str, required=True)
parser.add_argument('--anomaly_stream', type=str, default=None)
parser.add_argument('--type', type=str.lower, choices=['kafka', 'redis'], required=True)


if __name__ == '__main__':
    for i, v in enumerate(sys.argv):
        print(f"{i}:\t{v}")
    args = parser.parse_args()
    connection_info = json.loads(args.connection_info)

    stream_class = RedisStream if args.type == 'redis' else KafkaStream
    stream_out = stream_class(args.output_stream, **connection_info)
    stream_in = stream_class(args.input_stream, **connection_info)
    stream_anomaly = stream_class(args.anomaly_stream, **connection_info) if args.anomaly_stream else None
    controller = StreamController(args.predictor, stream_in, stream_out, stream_anomaly)

    controller.work()
