#!/usr/bin/env python

import argparse
from pathlib import Path


def main():
    parser = argparse.ArgumentParser(description="A script mocking parts of icon in a form of a shell script.")
    parser.add_argument("file", nargs="+", type=str, help="The files to analyse.")
    args = parser.parse_args()
    Path("analysis").write_text(f"analysis for file {args.file}")


if __name__ == "__main__":
    main()
