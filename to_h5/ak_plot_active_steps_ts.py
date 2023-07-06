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

DEBUG_FLAG = False


def main():

    main_dir = Path(r'P:\dwd_meteo\daily')

    os.chdir(main_dir)

    # .csv and .pkl allowed only.
    in_df_path = Path(
        r'dfs__merged_subset\daily_upper_neckar_50km_buff_tx_Y2005_2022.pkl')

    sep = ';'
    time_fmt = '%Y-%m-%d %H:%M:%S'

    out_dir = Path('figs__active_time')

    fig_size_long = (18, 8)
    dpi = 200
    xlabel = 'Time (day)'
    ylabel = 'Number of active stations (-)'
    #==========================================================================

    out_dir.mkdir(exist_ok=True, parents=True)

    if in_df_path.suffix == '.csv':
        df = pd.read_csv(in_df_path, sep=sep, index_col=0)
        df.index = pd.to_datetime(df.index, format=time_fmt)

    elif in_df_path.suffix == '.pkl':
        df = pd.read_pickle(in_df_path)

    else:
        raise NotImplementedError(f'Unknown extension: {in_df_path.suffix}!')

    avail_nrst_stns_ser = df.count(axis=1)

    assert avail_nrst_stns_ser.sum() > 0, 'df is empty!'

    plt.figure(figsize=fig_size_long)

    plt.plot(
        avail_nrst_stns_ser.index.values,
        avail_nrst_stns_ser.values,
        alpha=0.8)

    plt.xlabel(xlabel)
    plt.ylabel(ylabel)

    plt.grid()
    plt.gca().set_axisbelow(True)

    plt.savefig(
        out_dir / f'{in_df_path.stem}.png', dpi=dpi, bbox_inches='tight')

    plt.close()
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
