'''
@author: Faizan-Uni-Stuttgart

24 Feb 2020

20:54:18

'''
import os
import time
import timeit
from pathlib import Path

import numpy as np
import pandas as pd

DEBUG_FLAG = True


def main():

    main_dir = Path(r'D:\dwd_meteo\new_data_20200224\crds')
    os.chdir(main_dir)

    crds_files = (
        Path(r'gkz3_crds\hist_hourly_precip_gkz3.csv'),
        Path(r'gkz3_crds\pres_hourly_precip_gkz3.csv'),)

    out_file = Path(r'gkz3_crds_merged\hourly_precip_crds_gkz3.csv')

    # This flag performs some cleaning as well.
    # As of this writing, pandas was introducing whitespace in some cases.
    # I think it is a bug in pandas.
    remove_duplicate_records_flag = True

    sep = ';'

    crds_dfs = []
    for crds_file in crds_files:
        crds_df = pd.read_csv(
            crds_file,
            sep=sep,
            index_col=0,
            dtype=object,
            skipinitialspace=True)

        crds_dfs.append(crds_df)

    merge_df = pd.concat(crds_dfs)

    if remove_duplicate_records_flag:
        unq_idxs = list(set(merge_df.index))

        tmpy_df = pd.DataFrame(columns=merge_df.columns, dtype=object)

        new_idxs = []
        new_recs = []

        n_cols = merge_df.shape[1]

        for unq_idx in unq_idxs:

            curr_recs = np.atleast_2d(merge_df.loc[unq_idx].values)
            n_recs = curr_recs.shape[0]
            curr_recs_rav = curr_recs.ravel()

            stripped_vals = []
            for val in curr_recs_rav:
                try:
                    stripped_vals.append(val.strip())

                except:
                    stripped_vals.append(val)

            stripped_vals = np.array(
                stripped_vals, dtype=object).reshape(-1, n_cols)

            hashes = []
            for i in range(n_recs):
                hashes.append(hash(str(stripped_vals[i])))

            prcssd_hashes = []
            for i in range(n_recs):

                if hashes[i] in prcssd_hashes:
                    continue

                new_idxs.append(unq_idx)
                new_recs.append(curr_recs[i])

                prcssd_hashes.append(hashes[i])

        tmpy_df = pd.DataFrame(
            index=new_idxs,
            data=new_recs,
            columns=merge_df.columns,
            dtype=object)

        print('old shape:', merge_df.shape)

        print('new shape:', tmpy_df.shape)

        merge_df = tmpy_df

    merge_df.to_csv(out_file, sep=sep)

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
