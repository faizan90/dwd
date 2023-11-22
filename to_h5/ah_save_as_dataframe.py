# -*- coding: utf-8 -*-

'''
@author: Faizan-Uni-Stuttgart

Nov 25, 2020

7:35:36 PM

'''
import os
import time
import timeit
from pathlib import Path

import h5py
import pandas as pd

DEBUG_FLAG = False


def main():

    main_dir = Path(r'U:\dwd_meteo\hourly')
    os.chdir(main_dir)

    h5_path = Path(
        r'hdf5__merged_subset/hourly_upper_neckar_50km_buff_ppt_Y2005_2022.h5')

    # Two extensions allowed: .csv and .pkl.
    # csv: text dump, pkl: dataframe as pickle dump.
    # An error otherwise.
    out_df_path = Path(r'dfs__merged_subset') / f'{h5_path.stem}.pkl'

    # In case of .csv format.
    float_fmt = '%0.3f'
    #==========================================================================

    out_df_path.parents[0].mkdir(exist_ok=True, parents=True)

    assert h5_path.exists()

    assert out_df_path.suffix in ('.csv', '.pkl')

    with h5py.File(h5_path, 'r') as h5_hdl:
        data_grp = h5_hdl['data']
        columns = list(data_grp.keys())

        assert columns

        time_idxs = pd.DatetimeIndex(pd.to_datetime(
            h5_hdl['time/time_strs'][:].astype(str), format='%Y%m%d%H%M'))

        assert time_idxs.size

        df = pd.DataFrame(index=time_idxs, columns=columns, dtype=float)

        for column in df:
            df[column] = data_grp[column]

        if out_df_path.suffix == '.csv':
            df.to_csv(out_df_path, sep=';', float_format=float_fmt)

        elif out_df_path.suffix == '.pkl':
            df.to_pickle(out_df_path)

        else:
            raise NotImplementedError(f'Unknown suffix: {out_df_path.suffix}!')

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
