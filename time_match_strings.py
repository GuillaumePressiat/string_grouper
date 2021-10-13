import pandas as pd
import numpy as np
from string_grouper import match_strings
import random
import time
import os

# mem_limit = '1G'
# procgov = r'C:\Users\heamu\Source\Repos\process-governor\ProcessGovernor\bin\x64\Debug\procgov.exe'
# os.popen(f'{procgov} -r -m {mem_limit} -p {os.getpid()}')
# time.sleep(1)
progress = 0
do_print = True
companies = pd.read_csv('data/sec__edgar_company_info.csv')
x0 = 10000
Nx = 10000
dNx = 1000
Nx2 = 500000
dNx2 = 50000
y0 = 10000
Ny = 10000
dNy = 10000
ns = 10
# X = np.append(np.arange(dNx, Nx + 1, dNx), np.arange(dNx2 + dNx2, Nx2 + 1, dNx2))
X = np.arange(x0, Nx + 1, dNx)
Y = np.arange(y0, Ny + 1, dNy)
means = np.full((len(X), len(Y)), 0)
for s in range(ns):
    dgrid = []
    i = 1
    _ = print('[', flush=True, end='') if do_print else None
    for x in X:
        left_df = companies['Company Name'].iloc[random.sample(range(len(companies)), k = x)]
        if i > 1:
            _ = print(', ', flush=True) if do_print else None
        dseries = []
        stdseries = []
        _ = print('[', flush=True, end='') if do_print else None
        j = 1
        for y in Y:
            if j > 1:
                _ = print(', ', flush=True, end='') if do_print else None
            right_df = companies['Company Name'].iloc[random.sample(range(len(companies)), k = y)]
            t0 = time.time()
            _ = match_strings(right_df, left_df, n_blocks=(1, 1))
            t1 = time.time()
            dseries += [(t1 - t0)/60]
            progress += 1.0/(ns*len(X)*len(Y))
            # print(f'Progress {progress:.1%}', end='\x1b[1K\r')
            _ = print(f'{dseries[-1]}', flush=True, end='') if do_print else None
            # _ = print('.', flush=True, end='') if not do_print else None
            j += 1
        _ = print(']', flush=True, end='') if do_print else None
        dgrid += [dseries]
        i += 1
        # _ = print(f'{i}/{len(X)}', flush=True) if not do_print else None
    _ = print(']', flush=True) if do_print else None
    means = (np.asarray(dgrid) + s*means)/(s + 1)
    with open(f'runtime_means_x_{x0}-{Nx}_y_{y0}-{Ny}.npy', 'wb') as f:
        np.save(f, means)
        np.save(f, X)
        np.save(f, Y)
    #send_me_mail()
