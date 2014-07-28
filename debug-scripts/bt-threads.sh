#!/bin/bash

for line in `pgrep test_`
do
  printf "\n=====\n$line\n=====\n"
  sudo gdb --pid $line -batch -x bt-threads.gdb
done
