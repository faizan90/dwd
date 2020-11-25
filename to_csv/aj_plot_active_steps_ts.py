'''
@author: Faizan-Uni-Stuttgart

24 Feb 2020

21:36:56

'''
import os
import time
import timeit
from pathlib import Path

import pandas as pd
import matplotlib.pyplot as plt

plt.ioff()

DEBUG_FLAG = True


def main():

    main_dir = Path(
        r'E:\dwd_meteo\new_data_20200224\neckar_clim_data_20km_buff_new_with_netatmo')
    os.chdir(main_dir)

    in_data_dir = main_dir
    sep = ';'

    in_ext = '.csv'

    in_date_fmt = '%Y-%m-%d-%H'

    out_dir = main_dir

    fig_size_long = (18, 8)

    os.chdir(main_dir)

    out_dir.mkdir(exist_ok=True)

    for data_file in in_data_dir.glob(f'*{in_ext}'):

        print('Going through:', data_file)

        try:

            in_var_name = data_file.stem

            in_var_df = pd.read_csv(
                data_file, sep=sep, index_col=0)

            in_var_df.index = pd.to_datetime(
                in_var_df.index, format=in_date_fmt)

            avail_nrst_stns_ser = in_var_df.count(axis=1)

            assert avail_nrst_stns_ser.sum() > 0, 'in_var_df is empty!'

            plt.figure(figsize=fig_size_long)
            plt.plot(avail_nrst_stns_ser.index,
                     avail_nrst_stns_ser.values,
                     alpha=0.8)

            plt.xlabel('Time')
            plt.ylabel('Number of active stations')

            plt.title(in_var_name)

            plt.grid()

            plt.savefig(
                out_dir / f'{in_var_name}.png',
                dpi=300,
                bbox_inches='tight')

            plt.close()

        except Exception as msg:
            print(msg)

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
