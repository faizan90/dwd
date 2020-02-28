'''
@author: Faizan-Uni-Stuttgart

'''
import os
import timeit
import time
from pathlib import Path


def get_daily_precip_fmts():

    measure_time_fmt = '%Y%m%d'

    measure_columns = [
        'STATIONS_ID', 'MESS_DATUM', ['RS', 'NIEDERSCHLAGSHOEHE']]

    measure_column_labels = ['prec_daily']

    assert (len(measure_columns) - 2) == len(measure_column_labels)

    coordinate_time_fmt = measure_time_fmt

    coordinate_columns = [
        'Stations_id',
        'Stationshoehe',
        'Geogr.Breite',
        'Geogr.Laenge',
        'von_datum',
        'bis_datum',
        'Stationsname'
        ]

    coordinate_column_labels = [
        'Z',  # in meters
        'Y',  # in degrees
        'X',  # in degrees
        'beg_time',
        'end_time',
        'stn_name']

    assert len(coordinate_columns) == (len(coordinate_column_labels) + 1)

    missing_values = [-999, -9999, -999.99, -9999.99]

    separator = ';'

    return (
        measure_time_fmt,
        measure_columns,
        measure_column_labels,
        coordinate_time_fmt,
        coordinate_columns,
        coordinate_column_labels,
        missing_values,
        separator)


def get_precip_fmts(freq):

    fmts = None

    if freq == 'D':
        fmts = get_daily_precip_fmts()

    else:
        raise NotImplementedError(f'Unknown frequency: {freq}!')

    return fmts


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
