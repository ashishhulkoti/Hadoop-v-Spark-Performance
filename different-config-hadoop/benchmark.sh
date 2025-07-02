#!/bin/bash

mkdir build && cd build
cmake ..
make
cd ..

touch time.txt
for i in 1 4 16
do
	for j in 1 4 16
	do
		for k in 1 4 16
		do
			./hashgen -t $i -o $j -i $k -f data.bin -m 128 -s 1024 -d false
		done
	done
done
mv time.txt time1.txt


touch time.txt
for i in 1 4 16
do
        for j in 1 4 16
        do
                for k in 1 4 16
                do
                        ./hashgen -t $i -o $j -i $k -f data.bin -m 1024 -s 65536 -d false
                done
        done
done
mv time.txt time64.txt

touch time.txt
for i in 8192 32768
do
	./hashgen -t 16 -o 16 -i 16 -f data.bin -m $i -s 65536 -d false
done
mv time.txt time64_d_ram.txt

python3 Analysis.py

rm -rf build