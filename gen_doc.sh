#!/bin/bash

maturin develop --release || exit
pdoc -o ./docs/ -d google speedict
