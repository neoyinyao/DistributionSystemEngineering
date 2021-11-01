#!/bin/bash
#for i in {1..10}; do
#    go test -run Speed3A -race > "3A-$i"
#    if [[ "$?" -ne 0 ]]; then
#      echo "3A-$i failed"
#    fi
##    else
##      rm "3A-$i"
#
#done
for i in {1..10}; do
    go test  -race > "$i"
    if [[ "$?" -ne 0 ]]; then
      echo "$i failed"
    else
      rm "$i"
    fi
done
