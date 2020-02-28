'''
@author: Faizan-Uni-Stuttgart

'''
import os
import timeit
import time
from pathlib import Path

# import h5py
# import numpy as np
# import pandas as pd


def main():

    main_dir = Path(r'P:\Synchronize\IWS\DWD')
    os.chdir(main_dir)

#     data_dir = Path(r'Q:\extracted')
#
#     out_data_file = Path('dwd_daily_precip.h5').absolute()
#
#     out_hdl = h5py.File(str(out_data_file), mode='a', driver=None)
#
#     out_data_grp_label = 'data'
#     out_time_grp_label = 'times'
#     out_crds_grp_label = 'crds'
#
#     if out_data_grp_label not in out_hdl:
#         out_data_grp = out_hdl.create_group(out_data_grp_label)
#
#     else:
#         out_data_grp = out_hdl[out_data_grp_label]
#
#     if out_time_grp_label not in out_hdl:
#         out_time_grp = out_hdl.create_group(out_time_grp_label)
#
#     else:
#         out_time_grp = out_hdl[out_time_grp_label]
#
#     if out_crds_grp_label not in out_hdl:
#         out_crds_grp = out_hdl.create_group(out_crds_grp_label)
#
#     else:
#         out_crds_grp = out_hdl[out_crds_grp_label]
#
#     for i, (data_file, crds_file) in enumerate(zip(data_files, crds_files)):
#
#         try:
# #             print('\n')
# #             print(f'Going through {i:05d}: {data_file.stem} and {crds_file.stem}')
#
#             data_df = pd.read_csv(
#                 data_file,
#                 sep=separator,
#                 skipinitialspace=True,
#                 na_values=missing_values,
#                 error_bad_lines=False,
#                 warn_bad_lines=True,
#                 squeeze=False)
#
#             out_columns = deepcopy(measure_columns)
#
#             for data_val_column in out_columns[2]:
#                 if data_val_column in data_df.columns:
#                     out_columns[2] = data_val_column
#                     break
#
#             assert isinstance(out_columns[2], str)
#
#             data_df = data_df[out_columns]
#
#             data_stn_id = int(data_df.iloc[0, 0])
#
#             data_df[out_columns[1]] = pd.to_datetime(
#                 data_df[out_columns[1]], format=measure_time_fmt)
#
#             data_df = pd.DataFrame(
#                 index=data_df[out_columns[1]].values,
#                 data=data_df[out_columns[2]].values,
#                 columns=[f'{stn_name_pref}{data_stn_id:07d}'])
#
#             data_df = data_df.loc[~np.isnat(data_df.index)]
#
#         except Exception as msg:
#             print('\n')
#
#             print(f'Error: {i:05d}, {data_file.stem}')
#
#             print(msg)
#
#             continue
#
#         try:
#             crds_df = pd.read_csv(
#                 crds_file,
#                 sep=separator,
#                 skipinitialspace=True,
#                 na_values=missing_values,
#                 error_bad_lines=False,
#                 warn_bad_lines=True,
#                 squeeze=False,
#                 engine='python',
#                 dtype=object)
#
#             crds_df = crds_df[coordinate_columns]
#
#             end_time_idx = crds_df.columns.get_indexer_for(
#                 [coordinate_columns[5]])[0]
#
#             if np.isnan(float(crds_df.iloc[-1, end_time_idx])):
#                 crds_df.iloc[-1, end_time_idx] = int(
#                     data_df.index[-1].strftime(coordinate_time_fmt))
#
#             crds_df[coordinate_columns[5]] = crds_df[
#                 coordinate_columns[5]].astype(int)
#
#             crds_stn_id = int(crds_df.iloc[0, 0])
#
#             crds_df = pd.DataFrame(
#                 index=[
#                     f'{stn_name_pref}{crds_stn_id:07d}_{j:02d}'
#                     for j in range(crds_df.shape[0])],
#                 data=crds_df.loc[:, coordinate_columns[1:]].values,
#                 columns=coordinate_column_labels)
#
#         except Exception as msg:
#             print('\n')
#
#             print(f'Error: {i:05d}, {crds_file.stem}')
#
#             print(msg)
#
#             continue
#
#         if data_stn_id != crds_stn_id:
#             print(
#                 f'Error: {i:05d}, '
#                 f'Different data and coords ids: {data_stn_id}, '
#                 f'{crds_stn_id}')
#
#             continue

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
