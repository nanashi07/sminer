#!/bin/bash -e

function get_name()
{
    #echo $1 | sed -e 's/.\+\.\(.\+\).json/\1/g'
    echo $1 | sed -r 's/.+\.(.+)\.json/\1/g' # mac
}
export -f get_name

PATTERN=$1

if [[ "$PATTERN" == "" ]];
then
  find tmp/json -type f | sort | xargs -n1 -t bash -c 'RUST_BACKTRACE=1 ./target/release/sminer replay $1 | tee replay/replay.`get_name $1`.`date +%s`.log' _
else
  find tmp/json -type f | grep $PATTERN | sort | xargs -n1 -t bash -c 'RUST_BACKTRACE=1 ./target/release/sminer replay $1 | tee replay/replay.`get_name $1`.`date +%s`.log' _
fi



