# -*- coding: utf-8 -*-

'''
@author: Faizan-TU Munich

Sep 21, 2023

12:32:19 PM

Keywords:

'''
import os
import sys
import time
import timeit
import traceback as tb
from pathlib import Path

import numpy as np
import pandas as pd
from osgeo import gdal

DEBUG_FLAG = False


def main():

    main_dir = Path(r'U:\dwd_meteo\hourly\crds\hourly_upper_neckar_50km_buff')
    os.chdir(main_dir)

    in_crds_files_dir = main_dir

    match_pattern = '*_epsg32632.csv'

    in_elev_path = Path(r'U:\TUM\projects\altoetting\hydmod\srtm_de_mosaic_utm32N_100m.tif')

    out_elev_lab = 'Z_SRTM'
    #==========================================================================

    in_crds_files = list(in_crds_files_dir.glob(match_pattern))

    assert len(in_crds_files)

    elev_ds = gdal.Open(str(in_elev_path.absolute()))

    assert elev_ds is not None, 'Could not read elev raster!'

    elev_gt = elev_ds.GetGeoTransform()

    elev_x_min = elev_gt[0]
    elev_y_max = elev_gt[3]

    cell_width = elev_gt[1]
    cell_height = abs(elev_gt[5])

    elev_band = elev_ds.GetRasterBand(1)

    elev_arr = elev_band.ReadAsArray().astype(float)

    elev_ndv = elev_band.GetNoDataValue()

    for in_crds_file in in_crds_files:

        print('Going through:', in_crds_file)

        ot_crds_df = pd.read_csv(in_crds_file, sep=';', index_col=0)

        ot_crds_df[out_elev_lab] = np.nan

        for stn in ot_crds_df.index:
            stn_x = ot_crds_df.loc[stn, 'X']
            stn_y = ot_crds_df.loc[stn, 'Y']

            stn_col = int((stn_x - elev_x_min) / cell_width)
            stn_row = int((elev_y_max - stn_y) / cell_height)

            elev = elev_arr[stn_row, stn_col]

            if np.isclose(elev_ndv, elev):
                elev = np.nan

            ot_crds_df.loc[stn, out_elev_lab] = elev

        ot_crds_df.to_csv(in_crds_file.absolute(), sep=';')

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
