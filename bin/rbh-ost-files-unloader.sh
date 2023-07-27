#!/bin/bash

FILE_CLASSES=("very_huge_file"
              "huge_file"
              "very_large_file"
              "large_file"
              "ext_standard_file"
              "standard_file"
              "small_file"
              "very_small_file"
              "tiny_file"
              "empty_file")

COUNT_RUNNING_UNLOADS=30

if [ $# -ne 3 ]; then

    echo "Usage:                       "
    echo "                             "
    echo "    rbh_ost_files_unloader.sh"
    echo "        OST_MIN_INDEX        "
    echo "        OST_MAX_INDEX        "
    echo "        UNLOAD_DIR           "

    exit 1
fi

OST_MIN_INDEX=$1
OST_MAX_INDEX=$2
UNLOAD_DIR=$3

if [ $OST_MIN_INDEX -gt $OST_MAX_INDEX ]; then
    echo "OST_MAX_INDEX must be greater than OST_MIN_INDEX!" >/dev/stderr
    exit 2
fi

j=0

for FILE_CLASS in "${FILE_CLASSES[@]}" ; do

    for (( i=$OST_MIN_INDEX; i<=$OST_MAX_INDEX; i++ ))
    do
        printf "Unloading file class: ${FILE_CLASS} for OST-IDX: ${i}\n"
        time rbh-report --dump-ost ${i} --filter-class=${FILE_CLASS} --csv > ${UNLOAD_DIR}/${FILE_CLASS}_ost${i}.unl &
        ((j=j+1))

        if [ $j -ne 0 ] && [ $(($j % $COUNT_RUNNING_UNLOADS)) -eq 0 ]; then
            echo "Waiting for unloads to finish at iteration: $j"
            wait
        fi
    done

done
