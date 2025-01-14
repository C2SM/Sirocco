#!/usr/bin/env python
"""usage: icon.py [-h] 

A script mocking parts of icon in a form of a shell script

options:
  -h, --help           show this help message and exit
"""

import argparse
from pathlib import Path

LOG_FILE = Path("icon.log")


def log(text: str):
    print(text)
    with LOG_FILE.open("a") as f:
        f.write(text)


def main():
    # TODO add some checks if file are present
    
    #paths = ["simple_icon_run_atm_2d", "simple_icon_run_atm_3d_pl"]
    #for path in paths:
    #    Path(path).mkdir()

    files = ["NAMELIST_ICON_output_atm", "finish.status",
             "simple_icon_run_atm_2d/placeholder.nc",
             "simple_icon_run_atm_3d_pl/placeholder.nc"]
    for file in files:
        output = Path(file)
        output.write_text("")
    #├── _aiidasubmit.sh
    #├── _scheduler-stderr.txt
    #├── _scheduler-stdout.txt
    #├── output_schedule.txt
    #├── simple_icon_run_atm_2d
    #│   └── placeholder.nc
    #└── simple_icon_run_atm_3d_pl
    #    └── placeholder.nc

if __name__ == "__main__":
    main()
