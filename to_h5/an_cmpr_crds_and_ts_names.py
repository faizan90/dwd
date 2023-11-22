# -*- coding: utf-8 -*-

'''
@author: Faizan-TU Munich

Sep 21, 2023

10:19:59 AM

Keywords:

'''
import os
import sys
import time
import timeit
import traceback as tb
from pathlib import Path

import h5py
import parse
import numpy as np
import pandas as pd

DEBUG_FLAG = False


def main():

    main_dir = Path(r'U:\dwd_meteo\hourly')
    os.chdir(main_dir)

    in_crds_file = Path(r'crds/utm32n/hourly_ppt_epsg32632.csv')

    sep = ';'

    data_dirs = [
        Path(r'hdf5__all_dss\annual_ppt')]

    data_name_patts = [
        # 'TG_Y{year:4d}.h5'
        'P_Y{year:4d}.h5'
        # 'F_Y{year:4d}.h5'
        # 'T_Y{year:4d}.h5'
        ]
    #==========================================================================

    in_crds_df = pd.read_csv(
        in_crds_file, sep=sep, index_col=0, engine='python')[['X', 'Y', 'Z']]

    in_crds_x_fnt = np.isfinite(in_crds_df['X'].values)
    in_crds_y_fnt = np.isfinite(in_crds_df['Y'].values)

    in_crds_fnt = in_crds_x_fnt & in_crds_y_fnt

    in_crds_df = in_crds_df.loc[in_crds_fnt].copy()

    # Remove duplicate, the user can also implement proper selection
    # because a change station location means a new time series normally.
    keep_crds_stns_steps = ~in_crds_df.index.duplicated(keep='last')
    in_crds_df = in_crds_df.loc[keep_crds_stns_steps]

    in_crds_df.sort_index(inplace=True)

    crds_stns = set(in_crds_df.index)

    data_files = []
    for data_dir in data_dirs:
        data_files.extend(list(data_dir.glob('./*.h5')))

    n_data_files = len(data_files)
    print(f'Found {n_data_files} files to work with.',)

    tss_stns = get_hdf_tss_names(
        (data_files,
         data_name_patts,))

    print(len(crds_stns), len(tss_stns), len(crds_stns.intersection(tss_stns)))
    return


def get_hdf_tss_names(args):

    (in_h5_paths,
     data_name_patts,
     ) = args

    stns = None
    for in_h5_path in in_h5_paths:
        cont_flag = True
        for data_name_patt in data_name_patts:
            result = parse.search(data_name_patt, in_h5_path.name)

            if result is None:
                continue

            cont_flag = False
            break

        if cont_flag:
            continue

        in_h5_hdl = h5py.File(str(in_h5_path), mode='r', driver=None)

        data_grp = in_h5_hdl['data']

        if stns is None:
            stns = set(data_grp.keys())

        else:
            stns = stns.union(set(data_grp.keys()))

        in_h5_hdl.close()

    return stns


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
