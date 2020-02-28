'''
@author: Faizan-Uni-Stuttgart

'''
import os
import timeit
import time
from pathlib import Path

# import h5py

# def write_data_and_crds_dfs_to_h5(
#         data_df,
#         crds_df,
#         h5_file):
#
#     out_hdl = h5py.File(str(h5_file), mode='a', driver=None)
#
#     out_data_grp_label = 'data'
#     out_time_grp_label = 'time'
#     out_crds_grp_label = 'crds'
#     out_gage_dss_label = 'gage'
#
#     h5_str_dt = h5py.special_dtype(vlen=str)
#
#     for gage in crds_df.index:
#         pass
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
#     if out_gage_dss_label not in out_hdl:
#         out_gage_dss = out_hdl.create_dataset(
#             out_gage_dss_label,
#             )
#
#     else:
#         out_gage_dss = out_hdl[out_gage_dss_label]
#
#     return


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
