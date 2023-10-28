#!/usr/bin/env python3
import argparse
import brotli


def run(args):
    with open(args.rawfile, "rb") as fp:
        data = fp.read()
    for length in range(args.min_uncomp_len, args.max_uncomp_len + 1):
        compressed = brotli.compress(data[:length])
        print(f"len(compress(data[{length}])) = {len(compressed)}")


def make_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument("rawfile")
    parser.add_argument("min_uncomp_len", type=int)
    parser.add_argument("max_uncomp_len", type=int)
    return parser


if __name__ == "__main__":
    run(make_parser().parse_args())
