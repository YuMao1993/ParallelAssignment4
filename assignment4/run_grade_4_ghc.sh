#!/bin/bash
port=$(python scripts/port-for-user.py)

./run_ghc.sh 4 tests/grading_wisdom.txt $port
./run_ghc.sh 4 tests/grading_compareprimes.txt $port
./run_ghc.sh 4 tests/grading_tellmenow.txt $port
./run_ghc.sh 4 tests/grading_nonuniform1.txt $port
./run_ghc.sh 4 tests/grading_nonuniform2.txt $port
./run_ghc.sh 4 tests/grading_nonuniform3.txt $port
./run_ghc.sh 4 tests/grading_uniform1.txt $port