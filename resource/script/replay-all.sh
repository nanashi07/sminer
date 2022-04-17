#!/bin/bash -e

function get_name()
{
    echo $1 | sed -r 's/.+\.(.+)\.json/\1/g' # mac
}
export -f get_name

PATTERN=$1

mkdir -p replay
./target/release/sminer annotate

if [[ "$PATTERN" == "" ]];
then
  find tmp/json -type f | grep ticker | sort | xargs -n1 bash -c 'RUST_BACKTRACE=full ./target/release/sminer replay $1 | tee replay/replay.`get_name $1`.`date +%s`.log' _
else
  find tmp/json -type f | grep ticker | grep $PATTERN | sort | xargs -n1 bash -c 'RUST_BACKTRACE=full ./target/release/sminer replay $1 | tee replay/replay.`get_name $1`.`date +%s`.log' _
fi



