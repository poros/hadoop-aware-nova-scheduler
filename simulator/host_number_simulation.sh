#!/bin/bash
> data
file_input='input_hosts'
for i in `seq 10 10 1000`
do
    echo $i | tee -a data
    python generate_input.py $file_input $i 128 2 100:8 20:8
    python simulator_symmetric_no_optimal.py $file_input -s | tee -a data
done