#!/usr/bin/env python
import sys

# input comes from STDIN (standard input)
for line in sys.stdin:
        # remove leading and trailing whitespace
        line = line.strip().split()
        print '%s\t%s' % (','.join(line[:2], ','.join(line[2:])

