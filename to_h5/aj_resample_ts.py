'''
@author: Faizan-Uni-Stuttgart

Nov 26, 2020

11:21:54 AM

'''
import os
import time
import timeit
from pathlib import Path

import pandas as pd

DEBUG_FLAG = True


def main():

    main_dir = Path(
        r'P:\dwd_meteo\hourly')

    os.chdir(main_dir)

    # .csv and .pkl allowed.
    in_df_path = Path(r'merged_dfs/rheinlandpfalz_1hr_ppt_data_20km_buff_Y2009_2020.pkl')

    sep = ';'
    time_fmt = '%Y-%m-%d %H:%M:%S'
    float_fmt = '%0.3f'

    # Can be .pkl or .csv.
    out_fmt = '.pkl'

    out_dir = Path(r'resampled_dfs')

    # min_counts correspond to the resolutions. Each resolution when
    # being resampled show have a min-count to get a non Na value.
    # This is because resample sum does not have a skipna flag.
    resample_ress = ['D']
    min_counts = [24]
    resample_types = ['mean', 'min', 'max']

    assert out_fmt in ('.csv', '.pkl')

    assert len(resample_ress) == len(min_counts)

    out_dir.mkdir(exist_ok=True, parents=True)

    if in_df_path.suffix == 'csv':
        in_df = pd.read_csv(in_df_path, sep=sep, index_col=0)
        in_df.index = pd.to_datetime(in_df.index, format=time_fmt)

    elif in_df_path.suffix == '.pkl':
        in_df = pd.read_pickle(in_df_path)

    else:
        raise NotImplementedError(
            f'Unknown file extension: {in_df_path.suffix}!')

    for resample_res, min_count in zip(resample_ress, min_counts):

        counts_df = in_df.resample(resample_res).count().astype(float)
        counts_df[counts_df < min_count] = float('nan')

        for resample_type in resample_types:

            resample_df = getattr(in_df.resample(resample_res), resample_type)()

            resample_df *= counts_df

            # Another, very slow, way of doing this.
#             resample_df = in_df.resample(resample_res).agg(
#                 getattr(pd.Series, resample_type), skipna=False)

            out_name = (
                f'{in_df_path.stem}__'
                f'RR{resample_res}_RT{resample_type}{out_fmt}')

            out_path = out_dir / out_name

            if out_fmt == '.csv':
                resample_df.to_csv(
                    out_path, time_format=time_fmt, float_format=float_fmt)

            elif out_fmt == '.pkl':
                resample_df.to_pickle(out_path)

            else:
                raise NotImplementedError(
                    f'Unknown file extension: {out_fmt}!')

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
