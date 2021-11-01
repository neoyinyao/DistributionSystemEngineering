#!/bin/bash
let succeed=0
for i in `seq 1 30`
do
    go test > $i
    echo $i
    if [ `cat ${i}|grep FAIL |wc -l` -eq 1 ];
    then
        echo FAIL
    else
        succeed=$((succeed++))
        echo SUCCEED
    fi
done
echo $succeed
#go test -run InitialElection2A > out
#if [ `grep -c "PASS" out|wc -l` -eq 1 ];
#then
#  echo succeed
#else
#  echo fail
#fi