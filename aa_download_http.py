'''
@author: Faizan-Uni-Stuttgart

24 Feb 2020

14:13:16

'''
import os
import time
import timeit
from pathlib import Path

import requests
from bs4 import BeautifulSoup
from pathos.threading import ThreadPool

DEBUG_FLAG = False


def main():

    main_dir = Path(r'P:\dwd_meteo')
    os.chdir(main_dir)

    out_dir = Path(r'zipped_DWD_data')

    main_site = r'https://opendata.dwd.de'

    out_dir_names = [
    #     'hist_daily_met',
    #     'pres_daily_met',
    #
    #     'hist_daily_more_precip',
    #     'pres_daily_more_precip',
    #
    #     'hist_daily_soil_temp',
    #     'pres_daily_soil_temp',
    #
    #     'daily_solar',
    #
    #     'hist_hourly_precip',
    #     'pres_hourly_precip',
    #
    #     'hist_hourly_temp',
    #     'pres_hourly_temp',
    #
    #     'hist_hourly_cloud_type',
    #     'pres_hourly_cloud_type',
    #
    #     'hist_hourly_cloudiness',
    #     'pres_hourly_cloudiness',
    #
    #     'hist_hourly_pressure',
    #     'pres_hourly_pressure',
    #
    #     'hist_hourly_soil_temp',
    #     'pres_hourly_soil_temp',
    #
    #     'hourly_solar',
    #
    #     'hist_hourly_sun',
    #     'pres_hourly_sun',
    #
    #     'hist_hourly_visib',
    #     'pres_hourly_visib',

        'hist_hourly_wind',
        'pres_hourly_wind',
        ]

    sub_links = [
    #     r'/climate_environment/CDC/observations_germany/climate/daily/kl/historical/',
    #     r'/climate_environment/CDC/observations_germany/climate/daily/kl/recent/',
    #
    #     r'/climate_environment/CDC/observations_germany/climate/daily/more_precip/historical/',
    #     r'/climate_environment/CDC/observations_germany/climate/daily/more_precip/recent/',
    #
    #     r'/climate_environment/CDC/observations_germany/climate/daily/soil_temperature/historical/',
    #     r'/climate_environment/CDC/observations_germany/climate/daily/soil_temperature/recent/',
    #
    #     r'/climate_environment/CDC/observations_germany/climate/daily/solar/',
    #
    #     r'/climate_environment/CDC/observations_germany/climate/hourly/precipitation/historical/',
    #     r'/climate_environment/CDC/observations_germany/climate/hourly/precipitation/recent/',
    #
    #     r'/climate_environment/CDC/observations_germany/climate/hourly/air_temperature/historical/',
    #     r'/climate_environment/CDC/observations_germany/climate/hourly/air_temperature/recent/',
    #
    #     r'/climate_environment/CDC/observations_germany/climate/hourly/cloud_type/historical/',
    #     r'/climate_environment/CDC/observations_germany/climate/hourly/cloud_type/recent/',
    #
    #     r'/climate_environment/CDC/observations_germany/climate/hourly/cloudiness/historical/',
    #     r'/climate_environment/CDC/observations_germany/climate/hourly/cloudiness/recent/',
    #
    #     r'/climate_environment/CDC/observations_germany/climate/hourly/pressure/historical/',
    #     r'/climate_environment/CDC/observations_germany/climate/hourly/pressure/recent/',
    #
    #     r'/climate_environment/CDC/observations_germany/climate/hourly/soil_temperature/historical/',
    #     r'/climate_environment/CDC/observations_germany/climate/hourly/soil_temperature/recent/',
    #
    #     r'/climate_environment/CDC/observations_germany/climate/hourly/solar/',
    #
    #     r'/climate_environment/CDC/observations_germany/climate/hourly/sun/historical/',
    #     r'/climate_environment/CDC/observations_germany/climate/hourly/sun/recent/',
    #
    #     r'/climate_environment/CDC/observations_germany/climate/hourly/visibility/historical/',
    #     r'/climate_environment/CDC/observations_germany/climate/hourly/visibility/recent/',

        r'/climate_environment/CDC/observations_germany/climate/hourly/wind/historical/',
        r'/climate_environment/CDC/observations_germany/climate/hourly/wind/recent/'
        ]

    assert len(out_dir_names) == len(sub_links)

    out_dir.mkdir(exist_ok=True)

    n_threads = len(out_dir_names)

    if n_threads == 1:
        for i in range(len(out_dir_names)):
            download_data(
                main_site + sub_links[i],
                out_dir / out_dir_names[i]
                )

    else:
        thread_pool = ThreadPool(nodes=n_threads)

        thread_pool.map(
            download_data,
            [main_site + sub_link for sub_link in sub_links],
            [out_dir / out_dir_name for out_dir_name in out_dir_names],
            )

    return


def find_files(url):
    soup = BeautifulSoup(requests.get(url).text, features='html.parser')

    hrefs = []
    for a in soup.find_all('a'):
        hrefs.append(a['href'])

    return hrefs


def download_data(site, target_dir):

    target_dir.mkdir(exist_ok=True)

    print('\n\n\nConnecting to:', site)

    all_files = find_files(site)

    assert all_files, 'No files selected!'

    for raw_file in all_files:
        out_path = target_dir / raw_file

        # _, ext = (out_path.name).rsplit('.', 1)

        if not (out_path.exists()):
            time.sleep(0.1)

            req_cont = requests.get(
                site + raw_file,
                allow_redirects=True)

            open(out_path, 'wb').write(req_cont.content)

            print('Downloaded:', out_path)

        else:
            print('Skipped:', out_path)

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
