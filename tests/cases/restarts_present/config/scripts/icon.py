#!/usr/bin/env python
"""usage: icon.py [-h] 

A script mocking parts of icon in a form of a shell script

options:
  -h, --help           show this help message and exit
"""

from pathlib import Path

LOG_FILE = Path("icon.log")


def log(text: str):
    print(text)
    with LOG_FILE.open("a") as f:
        f.write(text)


def main():
    # TODO add some checks if file are present for tests
    dirs = ["simple_icon_run_atm_2d", "simple_icon_run_atm_3d_pl", "multifile_restart_atm.mfr", "multifile_restart_atm_20000101T030000Z.mfr"]
    for dir_ in dirs:
        Path(dir_).mkdir(exist_ok=True)
    
    files = ["NAMELIST_ICON_output_atm",
             "simple_icon_run_atm_2d/placeholder.nc",
             "simple_icon_run_atm_3d_pl/placeholder.nc",
             "multifile_restart_atm.mfr/placeholder.nc",
             "multifile_restart_atm_20000101T030000Z.mfr/placeholder.nc"]

    for file in files:
        output = Path(file).absolute()
        output.write_text("")
        log(f"Written {file}\n")

    file = "finish.status"
    output = Path(f"{file}")
    output.write_text("RESTART") 
    log(f"Written {file}\n")

if __name__ == "__main__":
    main()
