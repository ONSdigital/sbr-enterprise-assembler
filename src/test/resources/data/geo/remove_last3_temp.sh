#!/bin/bash
input="test-dataset.csv"
output="test-dataset_short.txt"
IFS=','
x=0;
while read -a line; do
	pcode=${line[0]}
    rgn=${line[1]}
   	if [[ $x != 0 ]] ; then
    	pcode=${pcode%????}
   	fi
	echo "${pcode},$rgn"
    let "x++"
done < "$input"