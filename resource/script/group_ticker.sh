#!/bin/bash -e

SYMBOLS='TQQQ|SQQQ SOXL|SOXS SPXL|SPXS LABU|LABD TNA|TZA YINN|YANG UDOW|SDOW'
FILES=$@

for SYMBOL in $SYMBOLS
do
  for FILE in $FILES
  do
    if [[ -f "$FILE" ]];
    then
      echo splitting $SYMBOL from $FILE
      mkdir -p json/$FILE
      SYMBOL_PATTERN=`echo $SYMBOL | sed 's/|/-/g'`
      grep -E $SYMBOL $FILE > json/$FILE/split.$FILE.$SYMBOL_PATTERN.json
    fi
  done
done