# -*- coding: utf-8 -*-

'''
@author: Faizan-Uni-Stuttgart

24 Feb 2020

16:58:40

'''
import os
import time
import timeit
from pathlib import Path

import pyproj
import pandas as pd

DEBUG_FLAG = False


def main():

    main_dir = Path(r'U:\dwd_meteo\daily\crds')
    os.chdir(main_dir)

    # Original
    if False:
        in_file = Path(r'geo_crds_ff/hourly_ff_geo_crds.csv')

        in_epsg = 4326
        out_epsg = 31467

        out_file = Path(f'gkz3_crds_ff/hourly_ff_epsg{out_epsg}.csv')

    # Neckar gkz3 to utm32n. Change conditional below as well.
    elif False:
        in_file = Path(r'neckar_1hr_tem_data_20km_buff/extracted_gkz3_crds.csv')

        in_epsg = 31467
        out_epsg = 32632

        out_file = Path(f'neckar_1hr_tem_data_20km_buff/hourly_tn_epsg{out_epsg}.csv')

    elif True:
        in_file = Path(r'geo_crds\daily_tx_geo_crds.csv')

        in_epsg = 4326
        out_epsg = 32632

        out_file = Path(f'utm32n/daily_tx_epsg{out_epsg}.csv')

    else:
        raise NotImplementedError

    sep = ';'

    out_float_fmt = '%0.0f'

    # in_cols and out_cols have one to one correspondence.
    # columns that are in input but not mentioned here stay as they are.
    #
    # The order of the first three columns should be longitude, latitude
    # and altitude.

    # Original
    if True:
        in_cols = (
            'GEOGR.LAENGE;GEOGR.BREITE;'
            'STATIONSHOEHE;VON_DATUM;BIS_DATUM').split(';')

        out_cols = ['X', 'Y', 'Z', 'BEG_TIME', 'END_TIME']

    # Neckar gkz3 to utm32n
    elif False:
        in_cols = ['X', 'Y', 'Z']
        out_cols = ['X', 'Y', 'Z']

    else:
        raise NotImplementedError

    #==========================================================================
    assert len(in_cols) == len(out_cols)

    out_file.parents[0].mkdir(exist_ok=True, parents=True)

    print('Going through:', in_file)

    # in_crds_df = pd.read_csv(
    #     in_file,
    #     sep=sep,
    #     engine='python',
    #     # parse_dates=False,
    #     index_col=0,
    #     # dtype=object,
    #     # skipinitialspace=True,
    #     encoding='latin1',
    #     )

    in_crds_df = pd.read_csv(
        in_file,
        sep=sep,
        index_col=0,
        engine='python',
        encoding='latin1')

    print('Input shape:', in_crds_df.shape)

    tfmr = pyproj.Transformer.from_crs(
        f'EPSG:{in_epsg}', f'EPSG:{out_epsg}', always_xy=True)

    out_crds = tfmr.transform(
        in_crds_df[in_cols[0]].values.astype(float),
        in_crds_df[in_cols[1]].values.astype(float))

    in_crds_df[in_cols[0]] = out_crds[0]
    in_crds_df[in_cols[1]] = out_crds[1]

    out_cols_all = []
    for col in in_crds_df.columns:
        if col in in_cols:
            out_col = out_cols[in_cols.index(col)]

        else:
            out_col = col

        out_cols_all.append(out_col)

    in_crds_df.columns = out_cols_all

    in_crds_df.to_csv(
        out_file,
        sep=sep,
        float_format=out_float_fmt,
        encoding='latin1')
    return


if __name__ == '__main__':

    print('#### Started on %s ####\n' % time.asctime())
    START = timeit.default_timer()

    #==========================================================================
    # When in post_mortem:
    # 1. "where" to show the stack
    # 2. "up" move the stack up to an older frame
    # 3. "down" move the stack down to a newer frame
    # 4. "interact" start an interactive interpreter
    #==========================================================================

    if DEBUG_FLAG:
        try:
            main()

        except:
            import pdb
            pdb.post_mortem()

    else:
        main()

    STOP = timeit.default_timer()
    print(('\n#### Done with everything on %s.\nTotal run time was'
           ' about %0.4f seconds ####' % (time.asctime(), STOP - START)))
