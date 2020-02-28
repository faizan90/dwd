'''
@author: Faizan-Uni-Stuttgart

24 Feb 2020

13:21:46

'''
import os
import time
import timeit
from pathlib import Path

import ogr
import pandas as pd

DEBUG_FLAG = True


def get_stns_in_cat(crds_df, poly):

    contain_stns = []

    for i, stn in enumerate(crds_df.index):

        x, y = crds_df.iloc[i].loc[['X', 'Y']]

        pt = ogr.CreateGeometryFromWkt("POINT (%f %f)" % (float(x), float(y)))

        if poly.Contains(pt):
            contain_stns.append(stn)

    return contain_stns


def main():

    main_dir = Path(r'D:\dwd_meteo\new_data_20200224')
    os.chdir(main_dir)

    in_data_file = Path(r'merged_dfs/hourly_temp.csv')

    in_crds_file = Path(r'crds\gkz3_crds_merged\hourly_temp_crds_gkz3.csv')

    subset_shp_file = Path(
        r'P:\Synchronize\IWS\QGIS_Neckar\raster\taudem_out_spate'
        r'_rockenau\watersheds_all.shp')

    shp_buff_dist = 20000

    out_dir = Path(r'neckar_clim_data_20km_buff_new')

    print('Reading inputs...')

    in_vec = ogr.Open(str(subset_shp_file))
    in_lyr = in_vec.GetLayer(0)
    tot_feat_count = in_lyr.GetFeatureCount()
    assert tot_feat_count == 1, 'Can only have one polygon!'

    cat_feat = (in_lyr.GetFeature(0)).Clone()
    cat_poly = cat_feat.GetGeometryRef()
    cat_buff = cat_poly.Buffer(shp_buff_dist)
    assert cat_buff

    in_data_df = pd.read_csv(in_data_file, sep=';', index_col=0)
    in_crds_df = pd.read_csv(in_crds_file, sep=';', index_col=0)[['X', 'Y']]

    keep_crds_stns_steps = ~in_crds_df.index.duplicated(keep='last')
    in_crds_df = in_crds_df.loc[keep_crds_stns_steps]

    print('Testing containment...')
    contain_crds = get_stns_in_cat(in_crds_df, cat_buff)

    subset_data_stns = in_data_df.columns.intersection(contain_crds)

    subset_crds_stns = in_crds_df.index.intersection(contain_crds)

    print(subset_data_stns.size, 'stations selected in data_df!')
    print(subset_crds_stns.size, 'stations selected in crds_df!')

    common_stns = subset_data_stns.intersection(subset_crds_stns)

    print(
        common_stns.size,
        'stations are common between data_df and crds_df!')

    assert common_stns.size, 'No sstations selected!'

    print('Writing output...')

    out_dir.mkdir(exist_ok=True)

    in_data_df.loc[:, common_stns].to_csv(
        out_dir / f'{in_data_file.name}', sep=';')

    in_crds_df.loc[common_stns].to_csv(
        out_dir / f'{in_crds_file.name}', sep=';')

    return


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
