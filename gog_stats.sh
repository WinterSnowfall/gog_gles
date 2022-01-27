#!/bin/bash

cd scripts

python3 gog_plot_gen.py -t
python3 gog_plot_gen.py -d
#only relevant if you set the correct CUTOFF_DATE
#python3 gog_plot_gen.py -i

cd ..

