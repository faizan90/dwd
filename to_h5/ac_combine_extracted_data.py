'''
@author: Faizan-Uni-Stuttgart

Nov 23, 2020

6:27:54 PM

'''
import os
import time
import timeit
import random
from pathlib import Path
from calendar import monthrange
from multiprocessing import Pool, Manager, Lock

import h5py
import numpy as np
import pandas as pd
from netCDF4 import date2num

DEBUG_FLAG = True

MSGS_FLAG = True

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


def check_and_get_valid_column(raw_df_cols, chk_cols, label):

    chk_col_flags = [chk_col in raw_df_cols for chk_col in chk_cols]

    assert any(chk_col_flags), f'No {label}_col in raw_df_cols!'

    assert sum(chk_col_flags) == 1, (
        f'More than one {label}_col in raw_df_cols!')

    ret_col = chk_cols[chk_col_flags.index(True)]

    return ret_col


def get_nc_units_and_times(resolution, year, beg_month, end_month):

    end_n_days = monthrange(year, end_month)[1]

    beg_time = f'{year:04d}-{beg_month:02d}-01 00:00:00'
    end_time = f'{year:04d}-{end_month:02d}-{end_n_days} 23:59:59'

    if resolution in ('days', 'hours', 'minutes', 'seconds'):
        pass

    else:
        raise NotImplementedError(f'Invalid resolution: {resolution}!')

    nc_units = f'{resolution} since {beg_time}'

    return nc_units, beg_time, end_time


def get_time_intervals(raw_df_cols, time_cols, time_fmts, raw_df, out_freq):

    assert all([time_col in raw_df_cols for time_col in time_cols])

    time_intervals = []
    time_fmt_flag = False

    for time_fmt in time_fmts:
        try:
            for time_col in time_cols:
                time_intervals.append(
                    pd.to_datetime(raw_df[time_col].values, format=time_fmt))

                raw_df.loc[:, time_col] = time_intervals[-1]

            time_fmt_flag = True
            break

        except:
            pass

    assert time_fmt_flag, 'None of the time_fmts worked!'

    if out_freq in ('D', 'H', 'min', 'T'):
        time_reindex = pd.date_range(
            time_intervals[0][+0],
            time_intervals[1][-1],
            freq=out_freq)

    else:
        raise NotImplementedError(f'Not configured for out_freq:{out_freq}!')

    return time_intervals, time_reindex


def updt_df_index(out_df, time_fmts):

    time_fmt_flag = False
    for time_fmt in time_fmts:
        try:
            time_vals = pd.to_datetime(out_df.index, format=time_fmt)

            time_fmt_flag = True
            break

        except:
            pass

    assert time_fmt_flag, 'None of the time_fmts worked!'

    out_df.index = time_vals
    return


def updt_intervals(time_intervals, out_df, stn_id_str):

    time_delta_nbools = ~(
        time_intervals[1] - time_intervals[0]).values.astype(bool)

    nan_nbools = ~np.isfinite(out_df.loc[time_intervals[1]].values)

    for i in range(len(time_intervals[0])):
        if time_delta_nbools[i] or nan_nbools[i]:
            continue

        interval_val = out_df.loc[time_intervals[1][i]][0]

        out_df.loc[
            time_intervals[0][i]:time_intervals[1][i], stn_id_str][:] = (
                interval_val)
    return


def validate_h5_file(
        h5_hdl,
        beg_time,
        end_time,
        nc_calendar,
        nc_units,
        out_freq):

    n_steps = 0

    if 'time' in h5_hdl:
        assert h5_hdl['time/time'][+0] == int(date2num(
            pd.to_datetime(beg_time, format='%Y-%m-%d %H:%M:%S'),
            nc_units,
            nc_calendar))

        assert h5_hdl['time/time'][-1] == int(date2num(
            pd.to_datetime(end_time, format='%Y-%m-%d %H:%M:%S'),
            nc_units,
            nc_calendar))

        assert 'time/time_strs' in h5_hdl

        n_steps = h5_hdl['time/time'].shape[0]

    else:
        time_grp = h5_hdl.create_group('time')

        str_fmt = '%Y%m%d%H%M'

        time_grp.attrs['units'] = nc_units
        time_grp.attrs['calendar'] = nc_calendar
        time_grp.attrs['str_fmt'] = str_fmt

        dates_times = pd.date_range(beg_time, end_time, freq=out_freq)

        n_steps = dates_times.shape[0]

        dates_times_strs = dates_times.strftime(str_fmt)

        h5_str_dt = h5py.special_dtype(vlen=str)

        time_strs_ds = time_grp.create_dataset(
            'time_strs', (dates_times.shape[0],), dtype=h5_str_dt)

        time_strs_ds[:] = dates_times_strs

        del dates_times_strs

        dates_times_nums = date2num(
            dates_times.to_pydatetime(), nc_units, nc_calendar)

        time_nums_ds = time_grp.create_dataset(
            'time', (dates_times.shape[0],), dtype=np.float64)

        time_nums_ds[:] = dates_times_nums

        del dates_times_nums, dates_times

    return n_steps


def write_updt_h5_file(
        out_h5_path,
        beg_time,
        end_time,
        nc_calendar,
        nc_units,
        out_freq,
        out_df,
        sel_idxs,
        stn_id_str):

    h5_hdl = h5py.File(str(out_h5_path), mode='a', driver=None)

    n_steps = validate_h5_file(
        h5_hdl,
        beg_time,
        end_time,
        nc_calendar,
        nc_units,
        out_freq)

    assert n_steps > 0
    assert n_steps >= sel_idxs.sum()

    date2nums_idxs = date2num(
        out_df.index[sel_idxs].to_pydatetime(),
        nc_units,
        nc_calendar)

    if 'data' in h5_hdl:
        data_grp = h5_hdl['data']

    else:
        data_grp = h5_hdl.create_group('data')

    if stn_id_str in data_grp:
        stn_ds = data_grp[stn_id_str]

    else:
        stn_ds = data_grp.create_dataset(stn_id_str, (n_steps,), np.float64)

        stn_ds[:] = np.nan

    stn_ds[date2nums_idxs] = out_df.iloc[sel_idxs, 0].values

    h5_hdl.close()
    return


def write_stn_to_h5(
        out_dir,
        out_name,
        lock,
        beg_time,
        end_time,
        nc_calendar,
        nc_units,
        out_freq,
        out_df,
        sel_idxs,
        stn_id_str):

    out_h5_path = out_dir / f'{out_name}.h5'
    out_temp_path = out_dir / f'{out_name}.h5t'

    with lock:
        while True:
            if out_temp_path.exists():
                time.sleep(0.1)

            else:
                break

        temp_hdl = open(out_temp_path, 'w')

    write_updt_h5_file(
        out_h5_path,
        beg_time,
        end_time,
        nc_calendar,
        nc_units,
        out_freq,
        out_df,
        sel_idxs,
        stn_id_str)

    temp_hdl.close()
    os.remove(out_temp_path)
    return


def write_updt_year_months(
        out_df,
        sep_basis,
        resolution,
        out_data_col_pref,
        out_dir,
        lock,
        nc_calendar,
        out_freq,
        stn_id_str):

    years = np.unique(out_df.index.year)

    if sep_basis == 'months':
        months = np.unique(out_df.index.month)

        for year in years:
            for month in months:
                (nc_units,
                 beg_time,
                 end_time) = get_nc_units_and_times(
                     resolution, year, month, month)

                out_name = (f'{out_data_col_pref}_Y{year}M{month:02d}')

                sel_idxs = (
                    (out_df.index.year == year) &
                    (out_df.index.month == month))

                write_stn_to_h5(
                    out_dir,
                    out_name,
                    lock,
                    beg_time,
                    end_time,
                    nc_calendar,
                    nc_units,
                    out_freq,
                    out_df,
                    sel_idxs,
                    stn_id_str)

    elif sep_basis == 'years':
        for year in years:
            (nc_units,
             beg_time,
             end_time) = get_nc_units_and_times(resolution, year, 1, 12)

            out_name = f'{out_data_col_pref}_Y{year}'

            sel_idxs = (out_df.index.year == year)

            write_stn_to_h5(
                out_dir,
                out_name,
                lock,
                beg_time,
                end_time,
                nc_calendar,
                nc_units,
                out_freq,
                out_df,
                sel_idxs,
                stn_id_str)

    else:
        raise NotImplementedError(f'Invalid sep_basis: {sep_basis}!')

    return


def reformat_and_save(args):

    (stn_dirs,
     seps,
     data_cols,
     time_cols,
     stn_cols,
     out_data_col_pref,
     time_fmts,
     out_dir,
     nan_vals,
     nc_calendar,
     out_freq,
     lock,
     interval_vals_flag,
     sep_basis,
     ) = args

    # Resolution is for num2date.
    if out_freq in ('min', 'T'):
        resolution = 'minutes'

    elif out_freq == 'H':
        resolution = 'hours'

    elif out_freq == 'D':
        resolution = 'days'

    else:
        raise NotImplementedError(
            f'Not configured for frequency: {out_freq}!')

    if interval_vals_flag:
        assert len(time_cols) == 2

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
                raw_df = pd.read_csv(stn_file, sep=sep, dtype=object, nrows=3)

                if raw_df.shape[1]:
                    raw_df = pd.read_csv(stn_file, sep=sep, dtype=object)
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

            stn_col = check_and_get_valid_column(
                raw_df_cols, stn_cols, 'stn')

            stn_id = raw_df.loc[0, stn_col]

            take_steps = raw_df.loc[:, stn_col].values == stn_id

            stn_id = int(stn_id.strip())

            stn_id_str = f'{out_data_col_pref}{stn_id}'

            if interval_vals_flag:
                assert all([time_col in raw_df_cols for time_col in time_cols])

                time_col = time_cols[1]

            else:
                time_col = check_and_get_valid_column(
                    raw_df_cols, time_cols, 'time')

            # Why did I use a DataFrame?
            out_df = pd.DataFrame(
                index=raw_df.loc[:, time_col].values,
                data=raw_df[data_col].values.astype(float),
                columns=[stn_id_str],
                dtype=float)

            assert out_df.shape[1] == 1, 'More than one data_col!'

            out_df = out_df.loc[take_steps, :]

            out_df.replace(nan_vals, float('nan'), inplace=True)

            n_count = out_df.count()[0]

            if MSGS_FLAG:
                print(f'Reading: {stn_file}, with {n_count} steps.')

            if not n_count:
                continue

            if interval_vals_flag:
                time_intervals, time_reindex = get_time_intervals(
                    raw_df_cols,
                    time_cols,
                    time_fmts,
                    raw_df,
                    out_freq)

            else:
                time_intervals = None
                time_reindex = None

            if not isinstance(out_df.index, pd.DatetimeIndex):
                updt_df_index(out_df, time_fmts)

            out_df = out_df.iloc[~out_df.index.duplicated('first'), :]

            if time_reindex is not None:
                out_df = out_df.reindex(time_reindex)
                updt_intervals(time_intervals, out_df, stn_id_str)

            # dropna is important, to avoid over-writing existing values
            # with nans.
            out_df.dropna(axis=0, how='any', inplace=True)

            assert np.all(np.isfinite(out_df.values))

            write_updt_year_months(
                out_df,
                sep_basis,
                resolution,
                out_data_col_pref,
                out_dir,
                lock,
                nc_calendar,
                out_freq,
                stn_id_str)

        assert stn_file_ctr > 0, 'No file present in this directory!'

    return file_ctr, dir_ctr


def main():

    main_dir = Path(r'P:\dwd_meteo\1_minute\precipitation')
    os.chdir(main_dir)

    # DATA_TXT_PREF might need changing based on dataset.

    in_dir = Path(r'extracted')

    # NOTE: all columns are stripped of white spaces around them, and are
    # capitalized before search in the input files.

    # One of these should be in the file.

    # Precip
    data_cols = ['RS_01', 'R1', 'NIEDERSCHLAGSHOEHE']
    out_data_col_pref = 'P'

    # Temp
#     data_cols = ['LUFTTEMPERATUR', 'TT_TU']
#     out_data_col_pref = 'T'

    match_patt = '*/1minutenwerte_nieder_*'

    # If interval_flag then, len(time_cols) == 2.
    # First label in time_cols is for the time at which the reading began.
    # Second label in time_cols is for the time at which the reading ended.
    # Check the files, if they have the begin and end columns.
    # Values within a given interval, get the same value as the interval
    # end time. Please check if this should be the case. Otherwise, change code
    # accordingly.
    interval_vals_flag = True

    time_cols = ['MESS_DATUM_BEGINN', 'MESS_DATUM_ENDE']
    stn_cols = ['STATIONS_ID']

    seps = [';']

    time_fmts = ['%Y%m%d%H%M']

    nan_vals = [-999]

    nc_calendar = 'gregorian'

    # Can be days, hours, minutes only.
    out_freq = 'min'

    # Can be months or years. Both are used in search in ag_subset_h5_data
    sep_basis = 'months'

    out_dir = Path(f'reformatted_binary/historical/monthly')

    n_cpus = 8

    out_dir.mkdir(exist_ok=True, parents=True)

    all_stn_dirs = list(in_dir.glob(f'./{match_patt}'))

    random.shuffle(all_stn_dirs)

    assert all_stn_dirs, 'No directories selected!'

    if n_cpus == 1:
        lock = Lock()

        ctrs = reformat_and_save(
            (all_stn_dirs,
             seps,
             data_cols,
             time_cols,
             stn_cols,
             out_data_col_pref,
             time_fmts,
             out_dir,
             nan_vals,
             nc_calendar,
             out_freq,
             lock,
             interval_vals_flag,
             sep_basis,
             ))

    else:
        mp_idxs = ret_mp_idxs(len(all_stn_dirs), n_cpus)

        lock = Manager().Lock()

        args_gen = ((
         all_stn_dirs[mp_idxs[thrd_idx]:mp_idxs[thrd_idx + 1]],
         seps,
         data_cols,
         time_cols,
         stn_cols,
         out_data_col_pref,
         time_fmts,
         out_dir,
         nan_vals,
         nc_calendar,
         out_freq,
         lock,
         interval_vals_flag,
         sep_basis,
         )
        for thrd_idx in range(n_cpus))

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
