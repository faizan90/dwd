'''
@author: Faizan-Uni-Stuttgart

24 Feb 2020

09:01:59

'''
import os
import time
import timeit
from pathlib import Path
from io import StringIO

import numpy as np
import pandas as pd

DEBUG_FLAG = False


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
     x_data_cols,
     y_data_cols,
     z_data_cols,
     beg_time_cols,
     end_time_cols,
     stn_id_cols,
     stn_name_cols,
     out_data_col_pref,
     file_pref_patts,
     time_fmts,
     out_sep,
     nan_vals,
     out_time_fmt,
     ) = args

    dir_ctr = 0
    file_ctr = 0

    header_string = (
        f'STN_ID{out_sep}'
        f'STN_NAME{out_sep}'
        f'GEOGR.LAENGE{out_sep}'
        f'GEOGR.BREITE{out_sep}'
        f'STATIONSHOEHE{out_sep}'
        f'VON_DATUM{out_sep}'
        f'BIS_DATUM\n'
        )

    csv_stream = StringIO()
    csv_stream.write(header_string)

    for stn_dir in stn_dirs:
        dir_ctr += 1

        stn_files = []
        for file_pref_patt in file_pref_patts:
            stn_files.extend(list(stn_dir.glob(f'./{file_pref_patt}')))

        stn_file_ctr = 0
        for stn_file in stn_files:
            file_ctr += 1
            stn_file_ctr += 1

            sep_flag = False
            for sep in seps:
                raw_df = pd.read_csv(
                    stn_file,
                    sep=sep,
                    dtype=object,
                    engine='python',
                    skipinitialspace=True,
                    encoding='ISO-8859-1')

                if raw_df.shape[1]:
                    sep_flag = True
                    break

            assert sep_flag, 'Could not read with the given separators!'

            assert raw_df.shape[0], f'No data for: {stn_file}'
            assert raw_df.shape[1] >= 7, 'Not enough columns!'

            raw_df.columns = [
                str(col).strip().upper() for col in raw_df.columns]

            raw_df_cols = raw_df.columns

            x_data_col = check_and_get_valid_column(
                raw_df_cols, x_data_cols, 'x_data')

            x_data_col = check_and_get_valid_column(
                raw_df_cols, x_data_cols, 'x_data')

            y_data_col = check_and_get_valid_column(
                raw_df_cols, y_data_cols, 'y_data')

            z_data_col = check_and_get_valid_column(
                raw_df_cols, z_data_cols, 'z_data')

            beg_time_col = check_and_get_valid_column(
                raw_df_cols, beg_time_cols, 'beg_time')

            end_time_col = check_and_get_valid_column(
                raw_df_cols, end_time_cols, 'end_time')

            stn_id_col = check_and_get_valid_column(
                raw_df_cols, stn_id_cols, 'stn_id')

            stn_name_col = check_and_get_valid_column(
                raw_df_cols, stn_name_cols, 'stn_name')

            stn_id = raw_df.loc[0, stn_id_col]

            take_steps = raw_df.loc[:, stn_id_col].values == stn_id

            stn_id = int(stn_id.strip())

            out_df = raw_df.loc[:,
                [stn_id_col,
                 stn_name_col,
                 x_data_col,
                 y_data_col,
                 z_data_col,
                 beg_time_col,
                 end_time_col,
                ]]

            assert out_df.shape[1] == 7, 'More than one data_col!'

            out_df = out_df.loc[take_steps,:]

            out_df.index = [f'{out_data_col_pref}{stn_id}'] * out_df.shape[0]

            out_df.drop([stn_id_col], axis=1, inplace=True)

            time_fmt_flag = False
            for time_fmt in time_fmts:
                try:
                    out_df[beg_time_col] = pd.to_datetime(
                        out_df[beg_time_col], format=time_fmt)

                    out_df[end_time_col] = pd.to_datetime(
                        out_df[end_time_col], format=time_fmt)

                    time_fmt_flag = True

                except:
                    pass

            assert time_fmt_flag, 'None of the time_fmts worked!'

            out_df.replace(nan_vals, float('nan'), inplace=True)

            out_df.to_csv(
                csv_stream,
                sep=out_sep,
                date_format=out_time_fmt,
                header=None,
                line_terminator='\n',
                encoding='utf-8')

            print('Saved:', stn_id)

        assert stn_file_ctr > 0, 'No file present in this directory!'

    return file_ctr, dir_ctr, csv_stream


def check_and_get_valid_column(raw_df_cols, chk_cols, label):

    chk_col_flags = [chk_col in raw_df_cols for chk_col in chk_cols]

    assert any(chk_col_flags), f'No {label}_col in raw_df_cols!'

    assert sum(chk_col_flags) == 1, (
        f'More than one {label}_col in raw_df_cols!')

    ret_col = chk_cols[chk_col_flags.index(True)]

    return ret_col


def main():

    main_dir = Path(r'P:\dwd_meteo\daily')
    os.chdir(main_dir)

    in_dir = Path(r'txt__raw_dwd_data')

    out_data_col_pref = 'P'

    # all columns are stripped of white spaces, and are capitalized

    # one of these should be in the file
    x_data_cols = ['GEOGR.LAENGE']
    y_data_cols = ['GEOGR.BREITE']
    z_data_cols = ['STATIONSHOEHE']

    beg_time_cols = ['VON_DATUM']
    end_time_cols = ['BIS_DATUM']

    stn_id_cols = ['STATIONS_ID']
    stn_name_cols = ['STATIONSNAME']

    file_pref_patts = ['Metadaten_Geographie*.txt', 'Stationsmetadaten_*.txt']

    # Can go in to dirs by having a slash.
    dir_name_patts = ['*_met/tageswerte_[0-9]*', '*precip/tageswerte_RR_[0-9]*']

    seps = [';']

    time_fmts = ['%Y%m%d']

    nan_vals = [-999]

    out_ext = 'csv'

    out_time_fmt = '%Y-%m-%d'

    out_sep = ';'

    out_dir = Path(f'crds/geo_crds_ppt')
    out_name = f'daily_ppt_geo_crds.{out_ext}'

    out_dir.mkdir(exist_ok=True, parents=True)

    all_stn_dirs = []
    for dir_name_patt in dir_name_patts:
        all_stn_dirs.extend(list(in_dir.glob(f'./{dir_name_patt}')))

    assert all_stn_dirs, 'No directories selected!'

    file_ctr, dir_ctr, csv_stream = reformat_and_save(
            (all_stn_dirs,
             seps,
             x_data_cols,
             y_data_cols,
             z_data_cols,
             beg_time_cols,
             end_time_cols,
             stn_id_cols,
             stn_name_cols,
             out_data_col_pref,
             file_pref_patts,
             time_fmts,
             out_sep,
             nan_vals,
             out_time_fmt,
             ))

    csv_stream.seek(0)
    with open(out_dir / out_name, 'w') as hdl:
        hdl.write(csv_stream.getvalue())

#     pd.read_csv(csv_stream, sep=out_sep, index_col=0, skipinitialspace=True).to_csv(
#         out_dir / f'{in_dir.name}_geo_crds_test.{out_ext}',
#         sep=out_sep,
#         date_format=out_time_fmt)

    print('file_ctr, dir_ctr:', file_ctr, dir_ctr)
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
