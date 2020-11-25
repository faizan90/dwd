'''
@author: Faizan-Uni-Stuttgart

24 Feb 2020

09:01:59

'''
import os
import time
import timeit
from pathlib import Path

DEBUG_FLAG = True


def main():

    main_dir = Path(r'P:\dwd_meteo')
    os.chdir(main_dir)

    in_ppt_dir = Path(r'extracted/hist_hourly_temp')

    unq_cols = set([])

    dir_ctr = 0
    file_ctr = 0

    for stn_dir in in_ppt_dir.glob('./stundenwerte*'):
        dir_ctr += 1

        for stn_file in stn_dir.glob('./produkt*'):
            file_ctr += 1

            with open(stn_file, 'r') as stn_hdl:
                line = stn_hdl.readline()

                cols = set(line.split(';'))

                unq_cols |= cols

        assert file_ctr == dir_ctr

    print(dir_ctr, file_ctr)

    print(unq_cols)

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
