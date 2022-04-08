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
      FILENAME=`basename $FILE`
      mkdir -p $FILENAME
      SYMBOL_PATTERN=`echo $SYMBOL | sed 's/|/-/g'`
      grep -E $SYMBOL $FILE > $FILENAME/group.$FILENAME.$SYMBOL_PATTERN.json
    fi
  done
done