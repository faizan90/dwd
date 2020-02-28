'''
@author: Faizan-Uni-Stuttgart

'''
import os
import timeit
import time
from pathlib import Path

import numpy as np
import pandas as pd

from dwd import (
    get_data_and_crds_files, get_precip_fmts, get_data_sers_and_crds_dfs)


def main():

    main_dir = Path(r'P:\Synchronize\IWS\DWD')
    os.chdir(main_dir)

    data_dir = Path(r'Q:\extracted')

    beg_time = '1950-01-01'
    end_time = '2017-12-13'

    freq = 'D'
    gage_type = 'PPT'

    out_h5_ds_key = 'ppt_daily'

    out_data_file = Path('ppt_data_daily.h5').absolute()
    out_crds_file = Path('ppt_crds_daily.h5').absolute()

    sub_data_dirs = [
        data_dir / sub_data_dir
        for sub_data_dir in [
#             r'hist_daily_more_precip'
            r'pres_daily_more_precip'
            ]]

    data_file_patts = ['**/produkt*.txt']
    crd_file_patts = ['**/Stationsmetadaten*.txt', '**/*_Geographie_*.txt']

    data_files, crds_files = get_data_and_crds_files(
        sub_data_dirs, data_file_patts, crd_file_patts)

    (measure_time_fmt,
     measure_columns,
     measure_column_labels,
     coordinate_time_fmt,
     coordinate_columns,
     coordinate_column_labels,
     missing_values,
     separator) = get_precip_fmts(freq)

    out_data_df = pd.DataFrame(
        index=pd.date_range(beg_time, end_time, freq='D'),
        dtype=np.float32)

    out_crds_df = pd.DataFrame(
        index=pd.Index([], dtype=object),
        columns=coordinate_column_labels)

    for (data_file, crds_file) in zip(data_files, crds_files):
        data_sers, crds_df = get_data_sers_and_crds_dfs(
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
            )

        for data_ser in data_sers:
            if not data_ser.shape[0]:
                continue

            out_data_df[data_ser.name] = data_ser
            out_crds_df.loc[data_ser.name] = crds_df.loc[data_ser.name]

#         break

    print(out_data_df.shape)
    print(out_crds_df.shape)

    out_data_df.to_hdf(str(out_data_file), key=out_h5_ds_key, mode='a')
    out_crds_df.to_hdf(str(out_crds_file), key=out_h5_ds_key, mode='a')

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
