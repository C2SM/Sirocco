#!/usr/bin/env python

import sys


def main():
    # Main script execution continues here
    print("Cleaning")
    for path in sys.argv[1:]:
        print("    " + path)


if __name__ == "__main__":
    main()
