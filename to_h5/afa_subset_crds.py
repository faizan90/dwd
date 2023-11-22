# -*- coding: utf-8 -*-

'''
@author: Faizan-Uni-Stuttgart

24 Feb 2020

13:21:46

'''
import os
import time
import timeit
from pathlib import Path

import numpy as np
import pandas as pd
from osgeo import ogr

DEBUG_FLAG = False


def main():

    main_dir = Path(r'U:\dwd_meteo\hourly\crds')
    os.chdir(main_dir)

    # NOTE: in_crds_file and subset_shp_file should have the same CRS.
    in_crds_file = Path(r'utm32n\hourly_tem_epsg32632.csv')

    sep = ';'

    subset_shp_file = Path(
        r'P:\Synchronize\IWS\QGIS_Neckar\vector\427_2km.shp')

    subset_shp_fld = 'DN'
    # subset_shp_fld = 'ID_0'
    shp_buff_dist = 50e3

    # If zero than no simplification is calculated.
    simplyify_tol = 0.1

    out_dir = Path(r'hourly_upper_neckar_50km_buff')
    #==========================================================================

    print('Reading inputs...')

    cat_poly = get_merged_poly(subset_shp_file, subset_shp_fld, simplyify_tol)
    cat_buff = cat_poly.Buffer(shp_buff_dist)
    assert cat_buff

    in_crds_df = pd.read_csv(
        in_crds_file,
        sep=sep,
        index_col=0,
        engine='python',
        encoding='latin1')

    in_crds_x_fnt = np.isfinite(in_crds_df['X'].values)
    in_crds_y_fnt = np.isfinite(in_crds_df['Y'].values)

    in_crds_fnt = in_crds_x_fnt & in_crds_y_fnt

    in_crds_df = in_crds_df.loc[in_crds_fnt].copy()

    # Remove duplicate, the user can also implement proper selection
    # because a change station location means a new time series normally.
    keep_crds_stns_steps = ~in_crds_df.index.duplicated(keep='last')
    in_crds_df = in_crds_df.loc[keep_crds_stns_steps]

    in_crds_df.sort_index(inplace=True)

    print('Testing containment...')
    contain_crds = get_stns_in_poly(in_crds_df, cat_buff)

    subset_crds_stns = in_crds_df.index.intersection(contain_crds)

    print(subset_crds_stns.size, 'stations selected in crds_df!')

    print('Writing output...')

    out_dir.mkdir(exist_ok=True)

    in_crds_df.loc[subset_crds_stns].to_csv(
        out_dir / f'{in_crds_file.name}', sep=sep)

    return


def get_stns_in_poly(crds_df, poly):

    contain_stns = []

    for i, stn in enumerate(crds_df.index):

        x, y = crds_df.iloc[i].loc[['X', 'Y']]

        pt = ogr.CreateGeometryFromWkt("POINT (%f %f)" % (float(x), float(y)))

        if pt is None:
            print(f'Station {stn} returned a Null point!')
            continue

        if poly.Contains(pt):
            contain_stns.append(stn)

    return contain_stns


def get_merged_poly(in_shp, field='DN', simplify_tol=0):

    '''Merge all polygons with the same ID in the 'field' (from TauDEM)

    Because sometimes there are some polygons from the same catchment,
    this is problem because there can only one cathcment with one ID,
    it is an artifact of gdal_polygonize.
    '''

    cat_ds = ogr.Open(str(in_shp))
    lyr = cat_ds.GetLayer(0)

    feat_dict = {}
    fid_to_field_dict = {}

    feat = lyr.GetNextFeature()
    while feat:
        fid = feat.GetFID()
        f_val = feat.GetFieldAsString(field)
        feat_dict[fid] = feat.Clone()
        fid_to_field_dict[fid] = f_val
        feat = lyr.GetNextFeature()

    fid_list = []
    for fid in list(fid_to_field_dict.keys()):
        fid_list.append(fid)

    if len(fid_list) > 1:
        cat_feat = feat_dict[fid_list[0]]
        merged_cat = cat_feat.GetGeometryRef().Buffer(0)
        for fid in fid_list[1:]:
            # The buffer with zero seems to fix invalid geoms somehow.
            curr_cat_feat = feat_dict[fid].Clone()
            curr_cat = curr_cat_feat.GetGeometryRef().Buffer(0)

            merged_cat = merged_cat.Union(curr_cat)
    else:
        cat = feat_dict[fid_list[0]].Clone()
        merged_cat = cat.GetGeometryRef().Buffer(0)

    if simplify_tol:
        merged_cat = merged_cat.Simplify(simplify_tol)

    cat_ds.Destroy()
    return merged_cat


if __name__ == '__main__':

    _save_log_ = False
    if _save_log_:
        from datetime import datetime
        from std_logger import StdFileLoggerCtrl

        # save all console activity to out_log_file
        out_log_file = os.path.join(
            r'P:\Synchronize\python_script_logs\\%s_log_%s.log' % (
            os.path.basename(__file__),
            datetime.now().strftime('%Y%m%d%H%M%S')))

        log_link = StdFileLoggerCtrl(out_log_file)

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

    if _save_log_:
        log_link.stop()
