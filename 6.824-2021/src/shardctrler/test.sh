#!/bin/bash
for i in {1..10}; do
  go test -race > "$i"
  if [[ "$?" -ne 0 ]]; then
    echo "$i failed"
  else
    rm "$i"
  fi
done