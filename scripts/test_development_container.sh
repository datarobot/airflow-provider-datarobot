#!/bin/bash
ERRORS=$(docker exec -it astro-dev_a87185-scheduler-1 cat logs/scheduler/latest/hospital_readmissions_example.py.log | grep "Error")

echo $ERRORS
if [ "$ERRORS" == "" ]; then
  echo "no errors"
  exit 0
else
  echo "error"
  exit 1
fi;
