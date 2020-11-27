'''
@author: Faizan-Uni-Stuttgart

Nov 25, 2020

7:48:49 PM

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

    main_dir = Path(r'P:\dwd_meteo\hourly')
    os.chdir(main_dir)

    # .csv and .pkl allowed as extensions.
    in_df_path = Path('dfs__merged_subset/neckar_1hr_tem_data_20km_buff_Y2005_2020.pkl')

    # in case of .csv
    sep = ';'
    time_fmt = '%Y-%m-%d %H:%M:%S'

    # Plot params
    fig_size = (10, 5)
    dpi = 200

    xlabel = 'Time (hour)'
    ylabel = 'Temperature (C)'

    # Outputs' directory
    out_dir = Path('figs__ts/ts_figs__tem_1hr_neckar')

    out_dir.mkdir(exist_ok=True, parents=True)

    if in_df_path.suffix == '.csv':
        df = pd.read_csv(in_df_path, sep=sep, index_col=0)
        df.index = pd.to_datetime(df.index, format=time_fmt)

    elif in_df_path.suffix == '.pkl':
        df = pd.read_pickle(in_df_path)

    else:
        raise NotImplementedError(f'Unknown extension: {in_df_path.suffix}!')

    for column in df:
        print(f'Plotting column: {column}...')
        plt.figure(figsize=fig_size)

        plt.plot(df.index, df[column], alpha=0.7)

        plt.xlabel(xlabel)
        plt.ylabel(ylabel)

        plt.grid()
        plt.gca().set_axisbelow(True)

        out_fig_name = f'{in_df_path.stem}_{column}.png'

        plt.savefig(str(out_dir / out_fig_name), bbox_inches='tight', dpi=dpi)

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
