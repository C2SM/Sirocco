#!/bin/bash
exec > _scheduler-stdout.txt
exec 2> _scheduler-stderr.txt


'/usr/bin/bash' < 'aiida.in' > 'aiida.out'
