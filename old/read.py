'''
@author: Faizan-Uni-Stuttgart

'''
import os
import timeit
import time
from pathlib import Path
from copy import deepcopy

import numpy as np
import pandas as pd


def get_data_and_crds_files(data_dirs, data_file_patts, crd_file_patts):

    '''
    Get data files with corresponding coordinates files
    '''

    tot_data_files_ct = 0
    tot_data_acpt_files_ct = 0

    out_data_files = []
    out_crds_files = []

    vb = True

    for data_file_patt in data_file_patts:
        for data_dir in data_dirs:
            sub_data_dirs = (x for x in data_dir.iterdir() if x.is_dir())

            for sub_data_dir in sub_data_dirs:
                sub_data_files = list(sub_data_dir.glob(data_file_patt))

                if not sub_data_files:

#                     print(
#                         f'No data file for {sub_data_dir.stem}')

                    continue

                if len(sub_data_files) > 1:
                    print(
                        f'Found more than one data file in {sub_data_dir.stem}:',
                        sub_data_files)

                    continue

                tot_data_files_ct += len(sub_data_files)

                sub_crds_files = []
                for crd_file_patt in crd_file_patts:
                    sub_crds_files += list(sub_data_dir.glob(crd_file_patt))

                if not sub_crds_files:
                    print(
                        f'No coordinates file for {sub_data_dir.stem}')

                    continue

                if len(sub_crds_files) > 1:
                    print(
                        f'Found more than one coordinate file in {sub_data_dir.stem}:',
                        sub_crds_files)

                    continue

                out_data_files += sub_data_files
                out_crds_files += sub_crds_files

                tot_data_acpt_files_ct += len(sub_crds_files)

    if vb:
        print(tot_data_files_ct, tot_data_acpt_files_ct)
        print(len(out_data_files), len(out_crds_files))

    assert len(out_data_files) == len(out_crds_files)

    return (out_data_files, out_crds_files)


def get_data_sers_and_crds_dfs(
        data_file,
        crds_file,
        gage_type,
        measure_time_fmt,
        measure_columns,
        measure_column_labels,
        coordinate_time_fmt,
        coordinate_columns,
        coordinate_column_labels,
        missing_values,
        separator,
        ):

    data_sers_and_crds_dfs = (None, None)
    data_read_flag = False
    crds_read_flag = False

    try:
        data_df = pd.read_csv(
            data_file,
            sep=separator,
            skipinitialspace=True,
            na_values=missing_values,
            error_bad_lines=False,
            warn_bad_lines=True,
            squeeze=False)

        out_columns = deepcopy(measure_columns)

        for data_val_column in out_columns[2]:
            if data_val_column in data_df.columns:
                out_columns[2] = data_val_column
                break

        assert isinstance(out_columns[2], str)

        data_df = data_df[out_columns]

        data_stn_id = int(data_df.iloc[0, 0])

        data_df[out_columns[1]] = pd.to_datetime(
            data_df[out_columns[1]], format=measure_time_fmt)

        data_df = pd.DataFrame(
            index=data_df[out_columns[1]].values,
            data=data_df[out_columns[2]].values,
            columns=[f'{gage_type}{data_stn_id:07d}'])

        data_df = data_df.loc[~np.isnat(data_df.index)]

        data_read_flag = True

    except Exception as msg:
        print('\n')

        print(f'Error reading data file: {data_file.stem}')

        print(msg)

    try:
        crds_df = pd.read_csv(
            crds_file,
            sep=separator,
            skipinitialspace=True,
            na_values=missing_values,
            error_bad_lines=False,
            warn_bad_lines=True,
            squeeze=False,
            engine='python',
            dtype=object)

        crds_df = crds_df[coordinate_columns]

        crds_df[coordinate_columns[4]] = pd.to_datetime(
            crds_df[coordinate_columns[4]], format=coordinate_time_fmt)

        crds_df[coordinate_columns[5]] = pd.to_datetime(
            crds_df[coordinate_columns[5]], format=coordinate_time_fmt)

        end_time_idx = crds_df.columns.get_indexer_for(
            [coordinate_columns[5]])[0]

        if np.isnat(crds_df.iloc[-1, end_time_idx].to_datetime64()):
            crds_df.iloc[-1, end_time_idx] = data_df.index[-1]

#         crds_df[coordinate_columns[5]] = crds_df[
#             coordinate_columns[5]].astype(int)

        crds_stn_id = int(crds_df.iloc[0, 0])

        crds_df = pd.DataFrame(
            index=[
                f'{gage_type}{crds_stn_id:07d}_{j:02d}'
                for j in range(crds_df.shape[0])],
            data=crds_df.loc[:, coordinate_columns[1:]].values,
            columns=coordinate_column_labels)

        crds_read_flag = True

    except Exception as msg:
        print('\n')

        print(f'Error reading coordinates file: {crds_file.stem}')

        print(msg)

    if data_read_flag and crds_read_flag:
        if data_stn_id != crds_stn_id:
            print(
                f'Error: '
                f'Different data and coords ids: {data_stn_id}, '
                f'{crds_stn_id}')

        else:
            data_sers = []
            for stn_id in crds_df.index:
                beg_time = crds_df.loc[stn_id, coordinate_column_labels[3]]
                end_time = crds_df.loc[stn_id, coordinate_column_labels[4]]

                data_ser = data_df.loc[
                    beg_time:end_time, f'{gage_type}{data_stn_id:07d}']

                data_ser.name = stn_id

                data_sers.append(data_ser)

            data_sers_and_crds_dfs = data_sers, crds_df

    return data_sers_and_crds_dfs


def main():

    main_dir = Path(os.getcwd())
    os.chdir(main_dir)

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
    START = timeit.default_timer()  # to get the runtime of the program

    main()

    STOP = timeit.default_timer()  # Ending time
    print(('\n#### Done with everything on %s.\nTotal run time was'
           ' about %0.4f seconds ####' % (time.asctime(), STOP - START)))

    if _save_log_:
        log_link.stop()
