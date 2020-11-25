'''
@author: Faizan-Uni-Stuttgart

24 Feb 2020

09:01:59

'''
import os
import time
import timeit
from pathlib import Path

import numpy as np
import pandas as pd
from multiprocessing import Pool

DEBUG_FLAG = True

DATA_TXT_PREF = 'produkt*'


def ret_mp_idxs(n_vals, n_cpus):

    assert n_vals > 0

    idxs = np.linspace(
        0, n_vals, min(n_vals + 1, n_cpus + 1), endpoint=True, dtype=np.int64)

    idxs = np.unique(idxs)

    assert idxs.shape[0]

    if idxs.shape[0] == 1:
        idxs = np.concatenate((np.array([0]), idxs))

    assert (idxs[0] == 0) & (idxs[-1] == n_vals), idxs
    return idxs


def reformat_and_save(args):

    (stn_dirs,
     seps,
     data_cols,
     time_cols,
     stn_cols,
     out_data_col_pref,
     time_fmts,
     out_dir,
     out_sep,
     nan_vals,
     out_time_fmt,
     ) = args

    dir_ctr = 0
    file_ctr = 0

    for stn_dir in stn_dirs:
        dir_ctr += 1

        stn_file_ctr = 0
        for stn_file in stn_dir.glob(f'./{DATA_TXT_PREF}'):
            file_ctr += 1
            stn_file_ctr += 1

            sep_flag = False
            for sep in seps:
                raw_df = pd.read_csv(stn_file, sep=sep, dtype=object)

                if raw_df.shape[1]:
                    sep_flag = True
                    break

            assert sep_flag, 'Could not read with the given separators!'

            assert raw_df.shape[0], 'No data!'
            assert raw_df.shape[1] >= 3, 'Not enough columns!'

            raw_df.columns = [
                str(col).strip().upper() for col in raw_df.columns]

            raw_df_cols = raw_df.columns

            data_col = check_and_get_valid_column(
                raw_df_cols, data_cols, 'data')

            time_col = check_and_get_valid_column(
                raw_df_cols, time_cols, 'time')

            stn_col = check_and_get_valid_column(
                raw_df_cols, stn_cols, 'stn')

            stn_id = raw_df.loc[0, stn_col]

            take_steps = raw_df.loc[:, stn_col].values == stn_id

            stn_id = int(stn_id.strip())

            out_df = pd.DataFrame(
                index=raw_df.loc[:, time_col].values,
                data=raw_df[data_col].values.astype(float),
                columns=[f'{out_data_col_pref}{stn_id}'],
                dtype=float)

            assert out_df.shape[1] == 1, 'More than one data_col!'

            out_df = out_df.loc[take_steps, :]

            time_fmt_flag = False
            for time_fmt in time_fmts:
                try:
                    time_vals = pd.to_datetime(out_df.index, format=time_fmt)

                    time_fmt_flag = True

                except:
                    pass

            assert time_fmt_flag, 'None of the time_fmts worked!'

            out_df.index = time_vals

            stn_exist_ctr = 0
            while True:
                out_path = (
                    out_dir /
                    f'{out_data_col_pref}{stn_id}_{stn_exist_ctr:03d}.csv')

                if out_path.exists():
                    stn_exist_ctr += 1

                else:
                    break

            out_df.replace(nan_vals, float('nan'), inplace=True)

            out_df.to_csv(
                out_path,
                sep=out_sep,
                date_format=out_time_fmt,
                header=f'time{out_sep}{out_data_col_pref}{stn_id}')

            print('Saved:', out_path)

        assert stn_file_ctr > 0, 'No file present in this directory!'

    return file_ctr, dir_ctr


def check_and_get_valid_column(raw_df_cols, chk_cols, label):

    chk_col_flags = [chk_col in raw_df_cols for chk_col in chk_cols]

    assert any(chk_col_flags), f'No {label}_col in raw_df_cols!'

    assert sum(chk_col_flags) == 1, (
        f'More than one {label}_col in raw_df_cols!')

    ret_col = chk_cols[chk_col_flags.index(True)]

    return ret_col


def main():

    main_dir = Path(r'T:\1_minute\precipitation')
    os.chdir(main_dir)

    # DATA_TXT_PREF might need changing.

    in_dir = Path(r'extracted')

    # all columns are stripped of white spaces, and are capitalized

    # one of these should be in the file

#     # Precip
    data_cols = ['RS_01', 'R1', 'NIEDERSCHLAGSHOEHE']
    out_data_col_pref = 'P'

    # Temp
#     data_cols = ['LUFTTEMPERATUR', 'TT_TU']
#     out_data_col_pref = 'T'

    match_patt = '1minutenwerte_nieder_*'

    time_cols = ['MESS_DATUM']
    stn_cols = ['STATIONS_ID']

    seps = [';']

    time_fmts = ['%Y%m%d%H%M']

    nan_vals = [-999]

    out_ext = 'csv'

    out_time_fmt = '%Y-%m-%d-%H-%M'

    out_sep = ';'

    out_dir = Path(f'reformatted/{in_dir.name}')

    del_out_dir_contents_flag = False

    n_cpus = 7

    if out_dir.exists() and del_out_dir_contents_flag:
        for stn_file in out_dir.glob(f'./{out_data_col_pref}*.{out_ext}'):
            os.remove(stn_file)

    else:
        out_dir.mkdir(exist_ok=True)

    all_stn_dirs = list(in_dir.glob(f'./{match_patt}'))

    assert all_stn_dirs, 'No directories selected!'

    if n_cpus == 1:
        ctrs = reformat_and_save(
            (all_stn_dirs,
             seps,
             data_cols,
             time_cols,
             stn_cols,
             out_data_col_pref,
             time_fmts,
             out_dir,
             out_sep,
             nan_vals,
             out_time_fmt,
             ))

    else:
        mp_idxs = ret_mp_idxs(len(all_stn_dirs), n_cpus)

        args_gen = ((
         all_stn_dirs[mp_idxs[i]:mp_idxs[i + 1]],
         seps,
         data_cols,
         time_cols,
         stn_cols,
         out_data_col_pref,
         time_fmts,
         out_dir,
         out_sep,
         nan_vals,
         out_time_fmt,
         )
        for i in range(n_cpus))

        mp_pool = Pool(n_cpus)

        ctrs = mp_pool.map(reformat_and_save, args_gen)

        mp_pool.close()

        mp_pool.join()

    ctrs = np.atleast_2d(ctrs)

    ctrs_sum = ctrs.sum(axis=0)

    print('files, dirs:', ctrs_sum)

    assert ctrs_sum[0] >= ctrs_sum[1]

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
