'''
@author: Faizan-Uni-Stuttgart

Nov 25, 2020

11:41:14 AM

'''
import os
import time
import timeit
from pathlib import Path
from multiprocessing import Pool, Manager, Lock

import h5py
import parse
import numpy as np
import pandas as pd
from netCDF4 import date2num, num2date

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


def get_h5_units_calendar_freq(h5_hdl):

    time_grp = h5_hdl['time']

    nc_units = time_grp.attrs['units']
    nc_calendar = time_grp.attrs['calendar']

    resolution = nc_units.split(' ')[0]

    if resolution == 'days':
        freq = 'D'

    elif resolution == 'hours':
        freq = 'H'

    elif resolution == 'minutes':
        freq = 'min'

    else:
        raise NotImplementedError('Unknown resolution: {resolution}!')

    return nc_units, nc_calendar, freq


def get_h5_dates_times(h5_hdl, nc_units, nc_calendar):

    date_times = pd.DatetimeIndex(
        num2date(h5_hdl['time/time'][:], nc_units, nc_calendar))

    return date_times


def initiate_output(in_h5_hdl, beg_time, end_time, out_h5_path):

    nc_units, nc_calendar, freq = get_h5_units_calendar_freq(in_h5_hdl)

    h5_hdl = h5py.File(str(out_h5_path), mode='w', driver=None)

    time_grp = h5_hdl.create_group('time')

    time_grp.attrs['units'] = nc_units
    time_grp.attrs['calendar'] = nc_calendar

    dates_times = pd.date_range(beg_time, end_time, freq=freq)

    n_steps = dates_times.shape[0]

    dates_times_strs = dates_times.strftime('%Y%m%d%H%M')

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

    del dates_times_nums

    h5_hdl.create_group('data')

    h5_hdl.close()

    return n_steps, dates_times


def updt_output(
        out_h5_path,
        common_stns,
        in_sel_idxs,
        out_sel_idxs,
        n_steps,
        in_h5_hdl):

    with h5py.File(str(out_h5_path), mode='a', driver=None) as out_h5_hdl:

        data_grp = out_h5_hdl['data']

        for common_stn in common_stns:
            if common_stn not in data_grp:
                stn_ds = data_grp.create_dataset(
                    common_stn, (n_steps,), np.float64)

                stn_ds[:] = np.nan

            else:
                stn_ds = data_grp[common_stn]

            stn_ds[out_sel_idxs] = in_h5_hdl[f'data/{common_stn}'][in_sel_idxs]
    return


def get_sel_idxs(dts_1, dts_2):

    td = pd.Timedelta('1s')
    min_t = dts_1[0].to_pydatetime()

    idts_1 = (dts_1 - min_t) // td
    idts_2 = (dts_2 - min_t) // td

    sel_idxs_1 = np.isin(idts_1, idts_2)
    sel_idxs_2 = np.isin(idts_2, idts_1)

    return sel_idxs_1, sel_idxs_2


def subset_data(args):

    (in_h5_paths,
     data_name_patts,
     subset_stns,
     out_h5_path,
     lock,
     beg_time,
     end_time,
     ) = args

    for in_h5_path in in_h5_paths:
        cont_flag = True
        for data_name_patt in data_name_patts:
            result = parse.search(data_name_patt, in_h5_path.name)

            if result is None:
                continue

            cont_flag = False
            break

        if cont_flag:
            continue

        in_h5_hdl = h5py.File(str(in_h5_path), mode='r', driver=None)

        data_grp = in_h5_hdl['data']

        common_stns = list(set(subset_stns) & set(data_grp.keys()))

        if not common_stns:
            continue

        tem_file = out_h5_path.with_suffix('.h5t')

        if not out_h5_path.exists():
            with lock:
                open(tem_file, 'w')

                n_steps, reindex_dates_times = initiate_output(
                     in_h5_hdl, beg_time, end_time, out_h5_path)

                os.remove(tem_file)

        else:
            while True:
                if tem_file.exists():
                    time.sleep(0.1)

                else:
                    break

            with h5py.File(
                str(out_h5_path), mode='r', driver=None) as out_h5_hdl:

                (out_nc_units,
                 out_nc_calendar,
                 _) = get_h5_units_calendar_freq(out_h5_hdl)

                reindex_dates_times = get_h5_dates_times(
                    out_h5_hdl, out_nc_units, out_nc_calendar)

                n_steps = reindex_dates_times.size

        assert n_steps > 0

        (in_nc_units,
         in_nc_calendar,
         _) = get_h5_units_calendar_freq(in_h5_hdl)

        in_dates_times = get_h5_dates_times(
            in_h5_hdl, in_nc_units, in_nc_calendar)

        # Rounding is important here.
        in_dates_times = in_dates_times.round('S')

        in_sel_idxs, out_sel_idxs = get_sel_idxs(
            in_dates_times, reindex_dates_times)

        n_sel_idxs = in_sel_idxs.sum()
        if n_sel_idxs:
            print(f'Found records (n={n_sel_idxs}) in:', in_h5_path)

            with lock:
                updt_output(
                    out_h5_path,
                    common_stns,
                    in_sel_idxs,
                    out_sel_idxs,
                    n_steps,
                    in_h5_hdl)

        in_h5_hdl.close()

    return


def main():

    main_dir = Path(r'P:\dwd_meteo\1_minute\precipitation')
    os.chdir(main_dir)

    data_dirs = [
        Path(r'reformatted_binary\historical\monthly')]

    data_name_patts = [
        'P_Y{year:4d}M{month:2d}.h5',
        'P_Y{year:4d}.h5']

    # Assuming that it is the output of af_subset_crds.py
    crds_file = Path(
        r'crds\neckar_1min_ppt_data_20km_buff\metadata_ppt_gkz3_crds.csv')

    sep = ';'

    n_cpus = 8

    # Should correspond to the resolution of the input data.
    # Seconds is the rounding resolution.
    beg_time = '2017-01-01 00:00:00'
    end_time = '2017-12-31 23:59:59'

    # The units and calendar are taken from whatever input file came first.
    # This does not matter as, at the end, the strings are saved anyways.
    out_data_path = Path(
        r'reformatted_merged_h5/neckar_1min_ppt_data_20km_buff_Y2018.h5')

    overwrite_output_flag = True

    if overwrite_output_flag and out_data_path.exists():
        os.remove(out_data_path)

    out_data_path.parents[0].mkdir(exist_ok=True, parents=True)

    crds_df = pd.read_csv(crds_file, sep=sep, index_col=0)

    subset_stns = crds_df.index.tolist()

    data_files = []
    for data_dir in data_dirs:
        data_files.extend(list(data_dir.glob('./*.h5')))

    n_data_files = len(data_files)
    print(f'Found {n_data_files} files to work with.',)

    n_cpus = min(n_cpus, n_data_files)

    assert data_files, 'No files!'

    if n_cpus == 1:
        lock = Lock()

        subset_data((
            data_files,
            data_name_patts,
            subset_stns,
            out_data_path,
            lock,
            beg_time,
            end_time,
            ))

    else:
        mp_idxs = ret_mp_idxs(n_data_files, n_cpus)

        lock = Manager().Lock()

        args_gen = ((
            data_files[mp_idxs[thrd_idx]:mp_idxs[thrd_idx + 1]],
            data_name_patts,
            subset_stns,
            out_data_path,
            lock,
            beg_time,
            end_time,
            )
        for thrd_idx in range(n_cpus))

        mp_pool = Pool(n_cpus)

        mp_pool.map(subset_data, args_gen)

        mp_pool.close()

        mp_pool.join()

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
