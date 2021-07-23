import sys
import json
import argparse
from streams import KafkaStream, RedisStream, StreamController


parser = argparse.ArgumentParser()
parser.add_argument('connection_info', type=str, help="json string with connection info")
parser.add_argument('predictor', type=str, help="predictor model name")
parser.add_argument('input_channel', type=str)
parser.add_argument('output_channel', type=str)
parser.add_argument('type', type=str.lower, choices=['kafka', 'redis'])


if __name__ == '__main__':
    for i, v in enumerate(sys.argv):
        print(f"{i}:\t{v}")
    args = parser.parse_args()
    connection_info = json.loads(args.connection_info)

    stream_class = RedisStream if args.type == 'redis' else KafkaStream
    stream_out = stream_class(args.output_channel, **connection_info)
    stream_in = stream_class(args.input_channel, **connection_info)
    controller = StreamController(args.predictor, stream_in, stream_out)

    controller.work()
