'''
@author: Faizan-Uni-Stuttgart

24 Feb 2020

10:45:21

'''
import os
import time
import timeit
from pathlib import Path

import numpy as np
import pandas as pd

DEBUG_FLAG = True


def main():

    main_dir = Path(r'P:\dwd_meteo\1_minute\precipitation')
    os.chdir(main_dir)

    # format depends on 02_reformat_extracted_ppt_data.py

    in_dir = Path(r'reformatted\recent')

    # if exists, will be updated with new data
    # if does not exist, then is created
    ref_df_path = Path(f'reformatted_merged/{in_dir.name}.csv')

    beg_time = '2019-01-01-00-00'
    end_time = '2019-04-30-23-59'

    freq = 'min'

    time_fmt = '%Y-%m-%d-%H-%M'

    sep = ';'

    if ref_df_path.exists():
        print('Reading existing ref_df...')

        ref_df = pd.read_csv(ref_df_path, sep=sep, index_col=0)

        ref_df.index = pd.to_datetime(ref_df.index, format=time_fmt)

        ref_index = ref_df.index

        print('Done reading existing ref_df!')

    else:
        print('Creating ref_df...')

        ref_index = pd.date_range(
            pd.to_datetime(beg_time, format=time_fmt),
            pd.to_datetime(end_time, format=time_fmt),
            freq=freq)

        ref_df = pd.DataFrame(index=ref_index, dtype=float)

        ref_df_path.parents[0].mkdir(exist_ok=True)

        print('Done creating ref_df!')

    stn_file_ctr = 0
    for stn_file in in_dir.glob('./*'):

        print('Processing:', stn_file)

        stn_ser = pd.read_csv(stn_file, sep=sep, index_col=0, squeeze=True)

        assert stn_ser.values.ndim == 1, 'More than one column!'

        stn_ser.index = pd.to_datetime(stn_ser.index, format=time_fmt)

        stn_ser = stn_ser.reindex(ref_index)

        stn_no = stn_ser.name.split('_')[0]

        if stn_no in ref_df:

            ref_stn_vals = ref_df[stn_no].values

            ref_stn_nan_idxs = np.isnan(ref_stn_vals)

            dst_stn_not_nan_idxs = ~(np.isnan(stn_ser.values))

            idxs_to_update = ref_stn_nan_idxs & dst_stn_not_nan_idxs

            idxs_to_update_sum = idxs_to_update.sum()

            print(f'Updating {idxs_to_update_sum} values...')

            ref_df.loc[idxs_to_update, stn_no] = stn_ser.values[idxs_to_update]

        else:
            print('Station is new!')

            ref_df.insert(ref_df.shape[1], stn_no, stn_ser.values)

        stn_file_ctr += 1

    print(stn_file_ctr)

    print('Writing final ref_df...')

    ref_df.to_csv(ref_df_path, sep=sep, date_format=time_fmt)

    print('Done writing final ref_df!')

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
