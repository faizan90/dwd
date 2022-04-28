'''
@author: Faizan-Uni-Stuttgart

Feb 10, 2022

9:58:32 AM

'''
import os
import sys
import time
import timeit
import traceback as tb
from pathlib import Path

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt; plt.ioff()

from spinterps.misc import get_dist_mat

DEBUG_FLAG = False


def main():

    main_dir = Path(r'P:\Downloads\pcp.obs.SP7')
    os.chdir(main_dir)

    in_data_file = Path(
        r'P:\Downloads\pcp.obs.SP7\dfs__resampled\hourly_sp7_rr_stns__RRD_RTsum.pkl')

    in_crds_file = Path(
        r'P:\Downloads\pcp.obs.SP7\crds\hourly_sp7_rr_stns\hourly_rr_epsg31467.csv')

    out_dir = Path(r'ppt_hourly_1995_2021_ts__close_stns_drop')

    sep = ';'

    beg_time = '1995-01-01'
    end_time = '2021-12-31'

    time_fmt = '%Y-%m-%d'

    min_dist_threshold = 100

    plot_close_stns_ts_flag = True
    #==========================================================================

    out_dir.mkdir(exist_ok=True)

    if in_data_file.suffix == '.csv':
        data_df = pd.read_csv(in_data_file, sep=sep, index_col=0)
        data_df.index = pd.to_datetime(data_df.index, format=time_fmt)

    elif in_data_file.suffix == '.pkl':
        data_df = pd.read_pickle(in_data_file)

    else:
        raise NotImplementedError(
            f'Unknown file extension: {in_data_file.suffix}!')

    print('data_df shape - original:', data_df.shape)

    data_df = data_df.loc[beg_time:end_time]

    print('data_df shape - after time subsetting:', data_df.shape)

    data_df.dropna(axis=1, how='all', inplace=True)

    print('data_df shape - after dropping NaN columns:', data_df.shape)

    data_df.columns = [str(col) for col in data_df.columns]

    crds_df = pd.read_csv(in_crds_file, sep=sep, index_col=0)

    crds_df = crds_df.loc[:, ['X', 'Y', 'Z']]

    crds_df.index = [str(idx) for idx in crds_df.index]

    print('crds_df shape - original:', crds_df.shape)

    cmn_stns = data_df.columns.intersection(crds_df.index)

    print('cmn_stns shape - data_df and crds_df intersection:', cmn_stns.shape)

    data_df = data_df[cmn_stns]
    crds_df = crds_df.loc[cmn_stns]

    dist_mat = get_dist_mat(crds_df['X'].values, crds_df['Y'].values)

    dist_mat += np.tril(np.full(dist_mat.shape, np.inf), k=0)

    le_thresh_idxs = np.where(dist_mat <= min_dist_threshold)
    #==========================================================================

    if plot_close_stns_ts_flag:
        plt.figure(figsize=(10, 2))

    close_stn_pairs = []
    print('Close station pairs:')
    for i in range(le_thresh_idxs[0].shape[0]):

        close_pair_a, close_pair_b = (
            cmn_stns[le_thresh_idxs[0][i]], cmn_stns[le_thresh_idxs[1][i]])

        close_stn_pairs.append((close_pair_a, close_pair_b))

        print(close_stn_pairs[-1])

        if plot_close_stns_ts_flag:

            data_a = data_df[close_pair_a].values.copy()
            data_a_nan_idxs = np.isnan(data_a)
            data_a[data_a_nan_idxs] = 0.0
            data_a[~data_a_nan_idxs] = 1.0

            data_b = data_df[close_pair_b].values.copy()
            data_b_nan_idxs = np.isnan(data_b)
            data_b[data_b_nan_idxs] = 0.0
            data_b[~data_b_nan_idxs] = 1.0

            plt.plot(
                data_df.index,
                data_a,
                alpha=0.75,
                c='r',
                label=close_pair_a)

            plt.plot(
                data_df.index,
                data_b,
                alpha=0.75,
                c='b',
                label=close_pair_b)

            plt.xlabel('Time')
            plt.ylabel('State')

            plt.grid()

            plt.gca().set_axisbelow(True)

            plt.legend()

            plt.draw()

            plt.yticks([0, 1], ['Not available', 'Available'])

            file_name = (
                f'states_{close_pair_a}_{close_pair_b}_before.png')

            plt.savefig(f'{out_dir / file_name}', bbox_inches='tight')

            plt.clf()

    #==========================================================================

    drop_stns = []
    counts_df = data_df.count(axis=0)
    print('Close station pairs and their counts:')
    for close_pair_a, close_pair_b in close_stn_pairs:
        print(
            close_pair_a,
            counts_df.loc[close_pair_a],
            close_pair_b,
            counts_df.loc[close_pair_b])

        if any([close_pair_a in drop_stns, close_pair_b in drop_stns]):
            continue

        pair_finite_idxs = (
            np.isfinite(data_df[close_pair_a].values) &
            np.isfinite(data_df[close_pair_b].values))

        if counts_df.loc[close_pair_a] < counts_df.loc[close_pair_b]:
            drop_stns.append(close_pair_a)

            data_df.loc[pair_finite_idxs, close_pair_a] = np.nan

        else:
            drop_stns.append(close_pair_b)

            data_df.loc[pair_finite_idxs, close_pair_b] = np.nan

    print('Stations to drop and their counts:')
    for drop_stn in drop_stns:
        print(drop_stn, counts_df.loc[drop_stn])
    #==========================================================================
    for i in range(le_thresh_idxs[0].shape[0]):

        close_pair_a, close_pair_b = (
            cmn_stns[le_thresh_idxs[0][i]], cmn_stns[le_thresh_idxs[1][i]])

        if plot_close_stns_ts_flag:
            data_a = data_df[close_pair_a].values.copy()
            data_a_nan_idxs = np.isnan(data_a)
            data_a[data_a_nan_idxs] = 0.0
            data_a[~data_a_nan_idxs] = 1.0

            data_b = data_df[close_pair_b].values.copy()
            data_b_nan_idxs = np.isnan(data_b)
            data_b[data_b_nan_idxs] = 0.0
            data_b[~data_b_nan_idxs] = 1.0

            plt.plot(
                data_df.index,
                data_a,
                alpha=0.75,
                c='r',
                label=close_pair_a)

            plt.plot(
                data_df.index,
                data_b,
                alpha=0.75,
                c='b',
                label=close_pair_b)

            plt.xlabel('Time')
            plt.ylabel('State')

            plt.grid()

            plt.gca().set_axisbelow(True)

            plt.legend()

            plt.draw()

            plt.yticks([0, 1], ['Not available', 'Available'])

            file_name = (
                f'states_{close_pair_a}_{close_pair_b}_after.png')

            plt.savefig(f'{out_dir / file_name}', bbox_inches='tight')

            plt.clf()

    plt.close()
    #==========================================================================

    data_df.dropna(axis=1, how='all', inplace=True)
    crds_df = crds_df.loc[data_df.columns]

    print('data_df shape - final:', data_df.shape)
    print('crds_df shape - final:', crds_df.shape)

    crds_df.to_csv(
        f'{out_dir / in_crds_file.stem}_close_dropped.csv', sep=sep)

    if in_data_file.suffix == '.csv':
        data_df.to_csv(
            f'{out_dir / in_data_file.stem}_close_dropped.csv', sep=sep)

    elif in_data_file.suffix == '.pkl':
        data_df.to_pickle(
            f'{out_dir / in_data_file.stem}_close_dropped.pkl')

    else:
        raise NotImplementedError(
            f'Unknown file extension: {in_data_file.suffix}!')

    return


if __name__ == '__main__':
    print('#### Started on %s ####\n' % time.asctime())
    START = timeit.default_timer()

    #==========================================================================
    # When in post_mortem:
    # 1. "where" to show the stack,
    # 2. "up" move the stack up to an older frame,
    # 3. "down" move the stack down to a newer frame, and
    # 4. "interact" start an interactive interpreter.
    #==========================================================================

    if DEBUG_FLAG:
        try:
            main()

        except:
            pre_stack = tb.format_stack()[:-1]

            err_tb = list(tb.TracebackException(*sys.exc_info()).format())

            lines = [err_tb[0]] + pre_stack + err_tb[2:]

            for line in lines:
                print(line, file=sys.stderr, end='')

            import pdb
            pdb.post_mortem()
    else:
        main()

    STOP = timeit.default_timer()
    print(('\n#### Done with everything on %s.\nTotal run time was'
           ' about %0.4f seconds ####' % (time.asctime(), STOP - START)))
