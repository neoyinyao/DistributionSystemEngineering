#!/bin/bash

for i in {1..10}; do
    echo "$i"
    go test > "$i"
    if [[ "$?" -ne 0 ]]; then
      echo "$i failed"
    fi
done