'''
@author: Faizan-Uni-Stuttgart

'''
import os
import timeit
import time
from pathlib import Path

import numpy as np
import netcdf4 as nc
from netcdftime import utime, datetime as nc_dt


def add_month(date, months_to_add):

    """
    Finds the next month from date.

    :param netcdftime.datetime date: Accepts datetime or phony datetime
        from ``netCDF4.num2date``.
    :param int months_to_add: The number of months to add to the date
    :returns: The final date
    :rtype: *netcdftime.datetime*
    """

    years_to_add = int((
        date.month +
        months_to_add -
        np.mod(date.month + months_to_add - 1, 12) - 1) / 12)

    new_month = int(np.mod(date.month + months_to_add - 1, 12)) + 1

    new_year = date.year + years_to_add

    date_next = nc_dt(
        year=new_year,
        month=new_month,
        day=date.day,
        hour=date.hour,
        minute=date.minute,
        second=date.second)
    return date_next


def add_year(date, years_to_add):

    """
    Finds the next year from date.

    :param netcdftime.datetime date: Accepts datetime or phony datetime
        from ``netCDF4.num2date``.
    :param int years_to_add: The number of years to add to the date
    :returns: The final date
    :rtype: *netcdftime.datetime*
    """

    new_year = date.year + years_to_add

    date_next = nc_dt(
        year=new_year,
        month=date.month,
        day=date.day,
        hour=date.hour,
        minute=date.minute,
        second=date.second)
    return date_next


def num2date(num_axis, units, calendar):

    """
    A wrapper from ``nc.num2date`` able to handle "years since" and
        "months since" units.

    If time units are not "years since" or "months since", calls
    usual ``netcdftime.num2date``.

    :param numpy.array num_axis: The numerical time axis following units
    :param str units: The proper time units
    :param str calendar: The NetCDF calendar attribute
    :returns: The corresponding date axis
    :rtype: *array*
    """

    res = None
    if not units.split(' ')[0] in ['years', 'months']:
        res = nc.num2date(num_axis, units=units, calendar=calendar)

    else:
        units_as_days = 'days ' + ' '.join(units.split(' ')[1:])

        start_date = nc.num2date(0.0, units=units_as_days, calendar=calendar)

        num_axis_mod = np.atleast_1d(np.array(num_axis))

        if units.split(' ')[0] == 'years':
            max_years = np.floor(np.max(num_axis_mod)) + 1
            min_years = np.ceil(np.min(num_axis_mod)) - 1

            years_axis = np.array([
                add_year(start_date, years_to_add)
                for years_to_add in np.arange(min_years, max_years + 2)])

            cdftime = utime(units_as_days, calendar=calendar)
            years_axis_as_days = cdftime.date2num(years_axis)

            yind = np.vectorize(np.int)(np.floor(num_axis_mod))

            num_axis_mod_days = (
                years_axis_as_days[yind - int(min_years)] +
                (num_axis_mod - yind) *
                np.diff(years_axis_as_days)[yind - int(min_years)])

            res = nc.num2date(
                num_axis_mod_days, units=units_as_days, calendar=calendar)

        elif units.split(' ')[0] == 'months':
            max_months = np.floor(np.max(num_axis_mod)) + 1
            min_months = np.ceil(np.min(num_axis_mod)) - 1

            months_axis = np.array([
                add_month(start_date, months_to_add)
                for months_to_add in np.arange(min_months, max_months + 12)])

            cdftime = utime(units_as_days, calendar=calendar)
            months_axis_as_days = cdftime.date2num(months_axis)

            mind = np.vectorize(np.int)(np.floor(num_axis_mod))

            num_axis_mod_days = (
                months_axis_as_days[mind - int(min_months)] +
                (num_axis_mod - mind) *
                np.diff(months_axis_as_days)[mind - int(min_months)])

            res = nc.num2date(
                num_axis_mod_days, units=units_as_days, calendar=calendar)

        else:
            raise ValueError(units.split(' ')[0])

    assert res is not None
    return res


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
