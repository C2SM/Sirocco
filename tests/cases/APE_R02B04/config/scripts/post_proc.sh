#!/usr/bin/bash -l

for stream in $@; do
    echo "post processing stream: ${stream}"
    echo "======================"
    echo ""
    for ofile in "${stream}"/*; do
        ncdump -h "${ofile}"
    done
done
