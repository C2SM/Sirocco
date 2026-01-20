#!/usr/bin/env python3

import argparse
import time
from pathlib import Path


def main():
    parser = argparse.ArgumentParser(description="A script mocking parts of icon in a form of a shell script.")
    parser.add_argument("file", nargs="+", type=str, help="The files to analyse.")
    args = parser.parse_args()

    # Sleep to simulate computation time and allow job monitoring to catch the job
    time.sleep(5)

    Path("analysis").write_text(f"analysis for file {args.file}")


if __name__ == "__main__":
    main()
