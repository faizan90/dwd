'''
@author: Faizan-Uni-Stuttgart

24 Feb 2020

14:13:16

Keywords: DWD download

'''
import os
import time
import timeit
from pathlib import Path

import py7zr
import requests
from bs4 import BeautifulSoup
from pathos.threading import ThreadPool

DEBUG_FLAG = True


def main():

    main_dir = Path(r'P:/dwd_meteo')
    os.chdir(main_dir)

    out_dir = Path(r'zipped_DWD_data')

    main_site = r'https://opendata.dwd.de'

    # NOTE: For all "pres" data, do not specify the zip file location as it has no time info in the directory name.
    # data_vars format: (directory where to save the files in out_dir, link of the data directory in main_site, locations of the 7zip file that hold the old downloaded files. Can be None if it does not exist.)
    data_vars = (
        # ('hist_daily_met',
        #  r'/climate_environment/CDC/observations_germany/climate/daily/kl/historical/',
        #  (r'daily/txt__raw_dwd_data/hist_daily_met.7z',)),
        # ('pres_daily_met',
        #  r'/climate_environment/CDC/observations_germany/climate/daily/kl/recent/',
        #  (None,)),

        # ('hist_daily_more_precip',
        #  r'/climate_environment/CDC/observations_germany/climate/daily/more_precip/historical/',
        #  (r'daily/txt__raw_dwd_data/hist_daily_more_precip.7z',)),
        # ('pres_daily_more_precip',
        #  r'/climate_environment/CDC/observations_germany/climate/daily/more_precip/recent/',
        #  (None,)),

        # ('hist_daily_soil_temp',
        #  r'/climate_environment/CDC/observations_germany/climate/daily/soil_temperature/historical/',
        #  None),
        # ('pres_daily_soil_temp',
        #  r'/climate_environment/CDC/observations_germany/climate/daily/soil_temperature/recent/',
        #  None),
        #
        # ('daily_solar',
        #  r'/climate_environment/CDC/observations_germany/climate/daily/solar/',
        #  None),

        ('hist_hourly_precip',
         r'/climate_environment/CDC/observations_germany/climate/hourly/precipitation/historical/',
         (r'hourly/txt__raw_dwd_data/hist_hourly_precip.7z',)),
        ('pres_hourly_precip',
         r'/climate_environment/CDC/observations_germany/climate/hourly/precipitation/recent/',
         (None,)),

        ('hist_hourly_temp',
         r'/climate_environment/CDC/observations_germany/climate/hourly/air_temperature/historical/',
         (r'hourly/txt__raw_dwd_data/hist_hourly_temp.7z',)),
        ('pres_hourly_temp',
         r'/climate_environment/CDC/observations_germany/climate/hourly/air_temperature/recent/',
         (None,)),
        #
        # ('hist_hourly_cloud_type',
        #  r'/climate_environment/CDC/observations_germany/climate/hourly/cloud_type/historical/',
        #  None),
        # ('pres_hourly_cloud_type',
        #  r'/climate_environment/CDC/observations_germany/climate/hourly/cloud_type/recent/',
        #  None),
        #
        # ('hist_hourly_cloudiness',
        #  r'/climate_environment/CDC/observations_germany/climate/hourly/cloudiness/historical/',
        #  None),
        # ('pres_hourly_cloudiness',
        #  r'/climate_environment/CDC/observations_germany/climate/hourly/cloudiness/recent/',
        #  None),
        #
        # ('hist_hourly_pressure',
        #  r'/climate_environment/CDC/observations_germany/climate/hourly/pressure/historical/',
        #  None),
        # ('pres_hourly_pressure',
        #  r'/climate_environment/CDC/observations_germany/climate/hourly/pressure/recent/',
        #  None),
        #
        # ('hist_hourly_soil_temp',
        #  r'/climate_environment/CDC/observations_germany/climate/hourly/soil_temperature/historical/',
        #  None),
        # ('pres_hourly_soil_temp',
        #  r'/climate_environment/CDC/observations_germany/climate/hourly/soil_temperature/recent/',
        #  None),
        #
        # ('hourly_solar',
        #  r'/climate_environment/CDC/observations_germany/climate/hourly/solar/',
        #  None),
        #
        # ('hist_hourly_sun',
        #  r'/climate_environment/CDC/observations_germany/climate/hourly/sun/historical/',
        #  None),
        # ('pres_hourly_sun',
        #  r'/climate_environment/CDC/observations_germany/climate/hourly/sun/recent/',
        #  None),
        #
        # ('hist_hourly_visib',
        #  r'/climate_environment/CDC/observations_germany/climate/hourly/visibility/historical/',
        #  None),
        # ('pres_hourly_visib',
        #  r'/climate_environment/CDC/observations_germany/climate/hourly/visibility/recent/',
        #  None),
        #
        # ('hist_hourly_wind',
        #  r'/climate_environment/CDC/observations_germany/climate/hourly/wind/historical/',
        #  r'hourly/txt__raw_dwd_data/hist_hourly_wind.7z'),
        # ('pres_hourly_wind',
        #  r'/climate_environment/CDC/observations_germany/climate/hourly/wind/recent/',
        #  None),
        )

    assert all([len(data_var) == 3 for data_var in data_vars])

    out_dir.mkdir(exist_ok=True, parents=True)

    n_threads = len(data_vars)

    mt_args = (
        (main_site, out_dir, data_var) for data_var in data_vars)

    if n_threads == 1:
        for mt_arg in mt_args:
            download_data(mt_arg)

    else:
        thread_pool = ThreadPool(nodes=n_threads)
        thread_pool.map(download_data, mt_args)

        thread_pool.close()
        thread_pool.join()

    return


def download_data(mt_args):

    main_site, out_dir, (sub_dir, sub_site, zip_locs) = mt_args

    out_dir = Path(out_dir)

    assert isinstance(zip_locs, (list, tuple)), type(zip_locs)

    target_dir = out_dir / sub_dir

    site = main_site + sub_site

    target_dir.mkdir(exist_ok=True)

    print('Connecting to:', site)

    all_files = find_files(site)

    assert all_files, 'No files selected!'

    print(len(all_files), 'to go through...')

    stn_dirs = set()
    for zip_loc in zip_locs:
        if zip_loc is not None:

            zip_loc = Path(zip_loc)

            with py7zr.SevenZipFile(zip_loc) as zip_hdl:
                for name in zip_hdl.getnames():

                    stn_dir_cmps = name.split('/')

                    if len(stn_dir_cmps) < 3:
                        continue

                    stn_dir = stn_dir_cmps[1]

                    if '.' in stn_dir:
                        continue

                    stn_dirs.add(stn_dir)

    for raw_file in all_files:
        out_path = target_dir / raw_file

        # In case the download broke or something before.
        if not (out_path.exists()):

            if raw_file.rsplit('.', 1)[0] in stn_dirs:
                continue

            time.sleep(0.1)

            req_cont = requests.get(
                site + raw_file,
                allow_redirects=True)

            open(out_path, 'wb').write(req_cont.content)

            print('Downloaded:', out_path)

        else:
            print('Skipped:', out_path)

    return


def find_files(url):

    soup = BeautifulSoup(requests.get(url).text, features='html.parser')

    hrefs = []
    for a in soup.find_all('a'):
        hrefs.append(a['href'])

    return hrefs


if __name__ == '__main__':

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
