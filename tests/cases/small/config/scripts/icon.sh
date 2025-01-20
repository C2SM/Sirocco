#!/bin/bash

# Usage: icon.sh [--init [INIT]] [--restart [RESTART]] [--forcing [FORCING]] [namelist]
# A script mocking parts of icon in a form of a shell script.

LOG_FILE="icon.log"

log() {
    local text="$1"
    echo "$text"
    echo "$text" >> "$LOG_FILE"
}

main() {
    local init_file=""
    local restart_file=""
    local forcing_file=""
    local namelist=""

    # Parse arguments
    while [[ $# -gt 0 ]]; do
        case "$1" in
            --init)
                if [[ $# -gt 1 && ! "$2" == --* ]]; then
                    init_file="$2"
                    shift
                else
                    init_file=""
                fi
                ;;
            --restart)
                if [[ $# -gt 1 && ! "$2" == --* ]]; then
                    restart_file="$2"
                    shift
                else
                    restart_file=""
                fi
                ;;
            --forcing)
                if [[ $# -gt 1 && ! "$2" == --* ]]; then
                    forcing_file="$2"
                    shift
                else
                    forcing_file=""
                fi
                ;;
            -h|--help)
                echo "Usage: icon.sh [--init [INIT]] [--restart [RESTART]] [--forcing [FORCING]] [namelist]"
                echo "A script mocking parts of icon in a form of a shell script."
                exit 0
                ;;
            *)
                namelist="$1"
                ;;
        esac
        shift
    done

    > "icon_output"  # Create or clear icon_output

    if [[ -n "$restart_file" ]]; then
        if [[ -n "$init_file" ]]; then
            echo "Cannot use '--init' and '--restart' option at the same time."
            exit 1
        fi
        if [[ ! -f "$restart_file" ]]; then
            echo "The icon restart file '$restart_file' was not found."
            exit 1
        fi
        log "Restarting from file '$restart_file'."
    elif [[ -n "$init_file" ]]; then
        if [[ ! -f "$init_file" ]]; then
            echo "The icon init file '$init_file' was not found."
            exit 1
        fi
        log "Starting from init file '$init_file'."
    else
        echo "Please provide a restart or init file with the corresponding option."
        exit 1
    fi

    if [[ -n "$namelist" ]]; then
        log "Namelist $namelist provided. Continue with it."
    else
        log "No namelist provided. Continue with default one."
    fi

    # Main script execution continues here
    log "Script finished running calculations."

    echo "" > "restart"  # Create or clear restart file
}

main "$@"
