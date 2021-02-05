# !/usr/bin/env python
# -*- coding:utf-8 -*-
import argparse
from pymilvusdm.main import execute


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--yaml',
        help='The path of yaml.',
        required=True,
        type=str)
    args = parser.parse_args()
    execute(args.yaml)
