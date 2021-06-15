'''
@author: Faizan-Uni-Stuttgart

Jun 11, 2021

12:12:51 PM

'''
import os
import sys
import time
import timeit
import traceback as tb
from pathlib import Path
from itertools import combinations

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

plt.ioff()

DEBUG_FLAG = False


def show_close_stns_auto(crds_df, min_dist_thresh, vb=False):

    n_pts = crds_df.shape[0]
    stns = crds_df.index

    crds = crds_df.values

    cmn_idxs = []
    for i in range(n_pts - 1):
        crds_i = crds[i]

        dists = (((crds_i - crds[i + 1:,:]) ** 2).sum(axis=1)) ** 0.5

        ld_idxs = np.where(dists <= min_dist_thresh)[0]

        n_ld_idxs = ld_idxs.size

        if not n_ld_idxs:
            continue

        for j in range(ld_idxs.size):
            if vb:
                print(
                    f'Stations "{stns[i]}" and "{stns[ld_idxs[j] + i + 1]}" '
                    f'are too close to eachother ({dists[ld_idxs[j]]})!')

            cmn_idxs.append((i, ld_idxs[j] + i + 1))

#         for j in range(i + 1, n_pts):
#             dist = (((crds_i - crds[j]) ** 2).sum()) ** 0.5
#
#             if dist <= min_dist_thresh:
#                 print(
#                     f'Stations "{stns[i]}" and "{stns[j]}" are too close '
#                     f'to eachother ({dist})!')
#
#                 cmn_idxs.append((i, j))

    cmn_idxs = np.array(cmn_idxs)
    return cmn_idxs


def show_close_stns_cross(crds_df_a, crds_df_b, min_dist_thresh, vb):

    stns_a = crds_df_a.index
    crds_a = crds_df_a.values

    stns_b = crds_df_b.index
    crds_b = crds_df_b.values

    cmn_idxs = []
    for i in range(crds_df_a.shape[0]):
        crds_i = crds_a[i]

        dists = (((crds_i - crds_b) ** 2).sum(axis=1)) ** 0.5

        ld_idxs = np.where(dists <= min_dist_thresh)[0]

        n_ld_idxs = ld_idxs.size

        if not n_ld_idxs:
            continue

        for j in range(ld_idxs.size):
            if vb:
                print(
                    f'Stations "{stns_a[i]}" and "{stns_b[ld_idxs[j]]}" '
                    f'are too close to eachother ({dists[ld_idxs[j]]})!')

            cmn_idxs.append((i, ld_idxs[j]))

    cmn_idxs = np.array(cmn_idxs)
    return cmn_idxs


def plot_scatter(ts_a, ts_b, lab_a, lab_b, out_dir, pref):

    fig_size = (7, 7)
    plt.figure(figsize=fig_size)

    plt.scatter(ts_a, ts_b, alpha=0.7)

    plt.grid()
    plt.gca().set_axisbelow(True)

    plt.xlabel(lab_a)
    plt.ylabel(lab_b)

    xy_min = min(np.nanmin(ts_a), np.nanmin(ts_b))
    xy_max = max(np.nanmax(ts_a), np.nanmax(ts_b))

    plt.xlim(xy_min, xy_max)
    plt.ylim(xy_min, xy_max)

    plt.gca().set_aspect('equal')

    plt.savefig(
        str(out_dir / f'{pref}{lab_a}_{lab_b}.png'),
        bbox_inches='tight')

    plt.close()
    return


def get_data_and_crds(
        in_data_df_paths,
        in_crds_paths,
        sep,
        dist_thresh_auto,
        out_dir,
        in_data_labs,
        beg_time,
        end_time,
        plot_too_near_flag,
        take_auto_nebor_flag):

    n_dss = len(in_data_df_paths)

    assert n_dss > 1, 'Number of files to read should be greater than 1!'

    in_data_dfs = []
    in_crds_dfs = []
    for i in range(n_dss):
        print(f'Dataset {i + 1} out of {n_dss}...')

        #======================================================================
        # Data.

        print(f'Data file: {in_data_df_paths[i]}')

        data_df = pd.read_pickle(in_data_df_paths[i])

        assert isinstance(data_df, pd.DataFrame), 'Input file not a DataFrame!'
        assert np.product(data_df.shape), 'Not usable records in input file!'

        print('Data shape:', data_df.shape)

        data_df = data_df.loc[beg_time:end_time]

        data_df.columns = [str(col) for col in data_df.columns]

        #======================================================================
        # Coordinates.

        print(f'Coordinates file: {in_crds_paths[i]}')

        crds_df = pd.read_csv(in_crds_paths[i], sep=sep, index_col=0)

        print('Coordinates shape:', crds_df.shape)

        assert all([lab in crds_df.columns for lab in ['X', 'Y']]), (
            'Columns X and Y not present in the coordinates dataframe!')

        crds_df.index = [str(col) for col in crds_df.index]

        #======================================================================
        # Common station checks and update.

        cmn_crds = data_df.columns.intersection(crds_df.index)

        print('Common coordinates:', cmn_crds.size)

        assert cmn_crds.size, (
            'crds_df and data_df have not common columns!')

        assert np.all(np.isfinite(crds_df[['X', 'Y']].values)), (
            'Invalid coordinates in crds_df!')

        data_df = data_df.loc[:, cmn_crds]
        crds_df = crds_df.loc[cmn_crds,:]

        #======================================================================
        # Check for duplicate stations and minimum distances for the auto case.

        duplicate_stn_idxs = crds_df.index.duplicated()

        assert not duplicate_stn_idxs.sum(), 'Duplicate stations in crds_df!'

        print('Computing station too close to each other in the auto-case...')

        close_stns_bt = timeit.default_timer()

        too_close_idxs_auto = show_close_stns_auto(
            crds_df[['X', 'Y']], dist_thresh_auto, False)

        close_stns_et = timeit.default_timer()

        print(
            f'Close station computation took '
            f'{close_stns_et - close_stns_bt:0.3f} seconds.')

        print('\n')

        if too_close_idxs_auto.size:

            drop_stns = []
            for j in range(too_close_idxs_auto.shape[0]):
                stn_a, stn_b = crds_df.index[too_close_idxs_auto[j]].values

                stn_a_ts = data_df.loc[:, stn_a].values
                stn_b_ts = data_df.loc[:, stn_b].values

                stn_a_valid_val_idxs = np.isfinite(stn_a_ts)
                stn_b_valid_val_idxs = np.isfinite(stn_b_ts)

                n_stn_a_valid_val_idxs = stn_a_valid_val_idxs.sum()
                n_stn_b_valid_val_idxs = stn_b_valid_val_idxs.sum()

                a_valid_b_invalid_idxs = (
                    stn_a_valid_val_idxs & (~stn_b_valid_val_idxs))

                a_invalid_b_valid_idxs = (
                    (~stn_a_valid_val_idxs) & stn_b_valid_val_idxs)

                cmn_tsteps_idxs = stn_a_valid_val_idxs & stn_b_valid_val_idxs

                n_cmn_tsteps = cmn_tsteps_idxs.sum()

                if n_cmn_tsteps:
                    same_vals_flag = np.all(np.isclose(
                        stn_a_ts[cmn_tsteps_idxs],
                        stn_b_ts[cmn_tsteps_idxs]))

                else:
                    same_vals_flag = False

                if (n_stn_a_valid_val_idxs >= n_stn_b_valid_val_idxs):

                    print(
                        f'{stn_a} has a longer time series than {stn_b} '
                        f'(a: {n_stn_a_valid_val_idxs}, '
                        f'b: {n_stn_b_valid_val_idxs}). '
                        f'Dropping {stn_b}...')

                    drop_stns.append(stn_b)

#                     if same_vals_flag:
#                         print(
#                             'Both stns have similar values at the common '
#                             'steps.')

                    if take_auto_nebor_flag and a_invalid_b_valid_idxs.sum():
                        data_df.loc[a_invalid_b_valid_idxs, stn_a] = (
                            stn_b_ts[a_invalid_b_valid_idxs])

                else:
                    print(
                        f'{stn_b} has a longer time series than {stn_a} '
                        f'(b: {n_stn_b_valid_val_idxs}, '
                        f'a: {n_stn_a_valid_val_idxs}). '
                        f'Dropping {stn_a}...')

                    drop_stns.append(stn_a)

#                     if same_vals_flag:
#                         print(
#                             'Both stns have similar values at the common '
#                             'steps.')

                    if take_auto_nebor_flag and a_valid_b_invalid_idxs.sum():
                        data_df.loc[a_valid_b_invalid_idxs, stn_b] = (
                            stn_a_ts[a_valid_b_invalid_idxs])

                if (plot_too_near_flag and
                    n_cmn_tsteps and
                    (not same_vals_flag)):

                    plot_scatter(
                        stn_a_ts[cmn_tsteps_idxs],
                        stn_b_ts[cmn_tsteps_idxs],
                        f'{in_data_labs[i]}{stn_a}',
                        f'{in_data_labs[i]}{stn_b}',
                        out_dir,
                        'auto_')

            data_df.drop(columns=drop_stns, inplace=True)
            crds_df.drop(index=drop_stns, inplace=True)

            print('\n')

            print(
                'Data shape after dropping nearby stations:',
                data_df.shape)

            print(
                'Coordinates shape after dropping nearby stations:',
                crds_df.shape)

        else:
            print('No stations that are too close.')

        #======================================================================
        # assert not show_close_stns_auto(
        #     crds_df[['X', 'Y']], dist_thresh_auto).shape[0], (
        #         'Stations that are too close still remain!')
        #======================================================================

        #======================================================================
        # Accept.

        in_data_dfs.append(data_df)
        in_crds_dfs.append(crds_df)

        print('\n')

    assert all([in_data_dfs[i].shape[0] == in_data_dfs[0].shape[0]
                for i in range(1, n_dss)]), (
                    'Unequal number of time steps in input data frames!')

    assert all([np.all(in_data_dfs[i].index == in_data_dfs[0].index)
                for i in range(1, n_dss)]), (
                    'Time steps to do have a 1-1 correspondence for input '
                    'data frames.')

    return in_data_dfs, in_crds_dfs


def drop_too_near_stns_cross(
        in_data_dfs,
        in_crds_dfs,
        dist_thresh_cross,
        out_dir,
        in_data_labs,
        plot_too_near_flag,
        take_cross_nebor_flag):

    print('\n\n')

    combs = combinations(np.arange(0, len(in_data_dfs))[::-1], 2)
    for comb in combs:
        print(
            f'Computing stations too close to each other in the '
            f'cross-case {comb}...')

        close_stns_bt = timeit.default_timer()

        too_close_idxs_cross = show_close_stns_cross(
            in_crds_dfs[comb[0]][['X', 'Y']],
            in_crds_dfs[comb[1]][['X', 'Y']],
            dist_thresh_cross,
            False)

        print('Pairs too close to each other:', too_close_idxs_cross.shape[0])

        close_stns_et = timeit.default_timer()

        print(
            f'Close station computation took '
            f'{close_stns_et - close_stns_bt:0.3f} seconds.')

        if too_close_idxs_cross.size:
            drop_stns_a = []
            drop_stns_b = []

            print('\n')

            for i in range(too_close_idxs_cross.shape[0]):
                stn_a = in_crds_dfs[comb[0]].index[too_close_idxs_cross[i, 0]]
                stn_b = in_crds_dfs[comb[1]].index[too_close_idxs_cross[i, 1]]

                stn_a_ts = in_data_dfs[comb[0]].loc[:, stn_a].values
                stn_b_ts = in_data_dfs[comb[1]].loc[:, stn_b].values

                stn_a_valid_val_idxs = np.isfinite(stn_a_ts)
                stn_b_valid_val_idxs = np.isfinite(stn_b_ts)

                n_stn_a_valid_val_idxs = stn_a_valid_val_idxs.sum()
                n_stn_b_valid_val_idxs = stn_b_valid_val_idxs.sum()

                a_valid_b_invalid_idxs = (
                    stn_a_valid_val_idxs & (~stn_b_valid_val_idxs))

                a_invalid_b_valid_idxs = (
                    (~stn_a_valid_val_idxs) & stn_b_valid_val_idxs)

                cmn_tsteps_idxs = stn_a_valid_val_idxs & stn_b_valid_val_idxs

                n_cmn_tsteps = cmn_tsteps_idxs.sum()

                if n_cmn_tsteps:
                    same_vals_flag = np.all(np.isclose(
                        stn_a_ts[cmn_tsteps_idxs],
                        stn_b_ts[cmn_tsteps_idxs]))

                else:
                    same_vals_flag = False

                if (n_stn_a_valid_val_idxs >= n_stn_b_valid_val_idxs):

#                     print(
#                         f'{stn_a} has a longer time series than {stn_b} '
#                         f'(a: {n_stn_a_valid_val_idxs}, '
#                         f'b: {n_stn_b_valid_val_idxs}). '
#                         f'Dropping {stn_b}...')

                    drop_stns_b.append(stn_b)

#                     if same_vals_flag:
#                         print(
#                             'Both stns have similar values at the common '
#                             'steps.')

                    if take_cross_nebor_flag and a_invalid_b_valid_idxs.sum():
                        in_data_dfs[comb[0]].loc[
                            a_invalid_b_valid_idxs, stn_a] = stn_b_ts[
                                a_invalid_b_valid_idxs]

                else:
#                     print(
#                         f'{stn_b} has a longer time series than {stn_a} '
#                         f'(b: {n_stn_b_valid_val_idxs}, '
#                         f'a: {n_stn_a_valid_val_idxs}). '
#                         f'Dropping {stn_a}...')

                    drop_stns_a.append(stn_a)

#                     if same_vals_flag:
#                         print(
#                             'Both stns have similar values at the common '
#                             'steps.')

                    if take_cross_nebor_flag and a_valid_b_invalid_idxs.sum():
                        in_data_dfs[comb[1]].loc[
                            a_valid_b_invalid_idxs, stn_b] = stn_a_ts[
                                a_valid_b_invalid_idxs]

                if (plot_too_near_flag and
                    n_cmn_tsteps and
                    (not same_vals_flag)):

                    plot_scatter(
                        stn_a_ts[cmn_tsteps_idxs],
                        stn_b_ts[cmn_tsteps_idxs],
                        f'{in_data_labs[comb[0]]}{stn_a}',
                        f'{in_data_labs[comb[1]]}{stn_b}',
                        out_dir,
                        'cross_')

            in_data_dfs[comb[0]].drop(columns=drop_stns_a, inplace=True)
            in_crds_dfs[comb[0]].drop(index=drop_stns_a, inplace=True)

            in_data_dfs[comb[1]].drop(columns=drop_stns_b, inplace=True)
            in_crds_dfs[comb[1]].drop(index=drop_stns_b, inplace=True)

            print(len(drop_stns_a), len(drop_stns_b))

#             print('Following stations dropped:')
#             print(drop_stns_a)
#             print(drop_stns_b)

            print('\n')

        else:
            print('No stations that are too close.')

            print('\n')

    return


def merge_dfs_inplace(in_data_dfs, in_crds_dfs, in_data_labs):

    print('\n\n')
    print('Merging data frames and coordinates...')

    ref_data_df = in_data_dfs[0]
    ref_crds_df = in_crds_dfs[0]

    ref_data_df.columns = [
        f'{in_data_labs[0]}{stn}' for stn in ref_data_df.columns]

    ref_crds_df.index = [
        f'{in_data_labs[0]}{stn}' for stn in ref_crds_df.index]

    cmn_crds_cols = ref_crds_df.columns
    for i in range(1, len(in_crds_dfs)):
        cmn_crds_cols = cmn_crds_cols.intersection(in_crds_dfs[i].columns)

    assert cmn_crds_cols.size, 'Huh! No common columns in crds_dfs?'

    ref_crds_df = ref_crds_df.loc[:, cmn_crds_cols]

    print('Reference data shape before merge:', ref_data_df.shape)
    print('Reference crds shape before merge:', ref_crds_df.shape)

    assert not ref_data_df.columns.difference(ref_crds_df.index).size, (
        'data and crds data frames have disjoint stations!')

    for i in range(1, len(in_data_dfs)):
        use_data_df = in_data_dfs[i]
        use_crds_df = in_crds_dfs[i]
        use_label = in_data_labs[i]

        assert not use_data_df.columns.difference(use_crds_df.index).size, (
            'data and crds data frames have disjoint stations!')

        for stn in use_data_df.columns:
            ref_data_df[f'{use_label}{stn}'] = use_data_df[stn]
            ref_crds_df.loc[f'{use_label}{stn}',:] = use_crds_df.loc[
                stn, cmn_crds_cols]

    print('Reference data shape after merge:', ref_data_df.shape)
    print('Reference crds shape after merge:', ref_crds_df.shape)

    return ref_data_df, ref_crds_df


def main():

    main_dir = Path(r'P:\dwd_meteo')
    os.chdir(main_dir)

    # NOTE: Order and length matters.

    # All pandas pickles. Should have the same time steps. Only the station
    # names and values can be differnt.
    # Final merging follows the priority listed here. The first one has
    # the highest priority.

    # Precipitation.
#     in_data_df_paths = (
#         Path(r'daily\dfs__merged_subset\daily_de_ppt_Y1961_2020.pkl'),
#         Path(r'hourly\dfs__resampled\hourly_de_ppt_Y1961_2020__RRD_RTsum.pkl'),
#         Path(r'P:\Synchronize\IWS\Hydrological_Modeling\data\ecad\2021\dfs__merged_subset\daily_outside_de_rr_Y1961_2020.pkl'),
#         )
#
#     # All csvs. Same coordinates systems. Should have the columns X and Y.
#     in_crds_paths = (
#         Path(r'daily\crds\daily_de_buff_100km\daily_ppt_epsg32632.csv'),
#         Path(r'hourly\crds\hourly_de_buff_100km\hourly_ppt_epsg32632.csv'),
#         Path(r'P:\Synchronize\IWS\Hydrological_Modeling\data\ecad\2021\crds\daily_outside_de\stations_epsg32632_rr_buff_1.000000e+05m.csv')
#         )

    # Mean temperature.
    in_data_df_paths = (
        Path(r'daily\dfs__merged_subset\daily_de_tx_Y1961_2020.pkl'),
        Path(r'hourly\dfs__resampled\hourly_de_tem_Y1961_2020__RRD_RTmax.pkl'),
        Path(r'P:\Synchronize\IWS\Hydrological_Modeling\data\ecad\2021\dfs__merged_subset\daily_outside_de_tx_Y1961_2020.pkl'),
        )

    # All csvs. Same coordinates systems. Should have the columns X and Y.
    in_crds_paths = (
        Path(r'daily\crds\daily_de_buff_100km\daily_tx_epsg32632.csv'),
        Path(r'hourly\crds\hourly_de_buff_100km\hourly_tem_epsg32632.csv'),
        Path(r'P:\Synchronize\IWS\Hydrological_Modeling\data\ecad\2021\crds\daily_outside_de\stations_epsg32632_tx_buff_1.000000e+05m.csv')
        )

    in_data_labs = ('DWD_D_', 'DWD_H_', 'ECAD_D_')

    # Minimum distance that neighboring stations should have, to be considered
    # as seperate in the merged dataset. And if there are common time steps
    # at which both stations have values.

    # Minimum distance for stations in the same dataset.
    dist_thresh_auto = 50

    # Minimum distance for stations in different datasets.
    dist_thresh_cross = 50

    # Plot stations time series that are too near to each other.
    plot_too_near_flag = False

    # Take nebors' data where one is nan that are too close in the auto case.
    take_auto_nebor_flag = False

    # Take nebors' data where one is nan that are too close in the cross case.
    take_cross_nebor_flag = True

    # csv delimiter.
    sep = ';'

    # Start and ending time. Standard pandas format.
    beg_time = '1961-01-01 00:00:00'
    end_time = '2020-12-31 23:59:00'

    # Outputs.
    out_dir = Path(
        r'daily_de_buff_100km_tem__merged__daily_hourly_dwd__daily_ecad')

    out_name_suff = f'{in_data_df_paths.stem}__merged'

    #==========================================================================
    assert len(in_data_df_paths) == len(in_crds_paths) == len(in_data_labs)

    assert len(set(in_data_labs)) == len(in_data_labs), (
        'Non-unique_labels for inputs!')

    out_dir.mkdir(exist_ok=True, parents=True)

    #==========================================================================
    # Get inputs after dropping stations that are too close to each other.

    in_data_dfs, in_crds_dfs = get_data_and_crds(
        in_data_df_paths,
        in_crds_paths,
        sep,
        dist_thresh_auto,
        out_dir,
        in_data_labs,
        beg_time,
        end_time,
        plot_too_near_flag,
        take_auto_nebor_flag)

    #==========================================================================
    # Based on cross distances, drop stations that are too close and
    # have shorter time series.

    drop_too_near_stns_cross(
        in_data_dfs,
        in_crds_dfs,
        dist_thresh_cross,
        out_dir,
        in_data_labs,
        plot_too_near_flag,
        take_cross_nebor_flag)

    #==========================================================================
    # Merging.

    merged_data_df, merged_crds_df = merge_dfs_inplace(
        in_data_dfs, in_crds_dfs, in_data_labs)

    #==========================================================================
    # Saving.

    merged_data_df.to_pickle(out_dir / f'{out_name_suff}_data.pkl')
    merged_crds_df.to_csv(out_dir / f'{out_name_suff}_crds.csv', sep=sep)
    return


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
            pre_stack = tb.format_stack()[:-1]

            err_tb = list(tb.TracebackException(*sys.exc_info()).format())

            lines = [err_tb[0]] + pre_stack + err_tb[2:]

            for line in lines:
                print(line, file=sys.stderr, end='')

            import pdb
            pdb.post_mortem()
    else:
        main()

    STOP = timeit.default_timer()
    print(('\n#### Done with everything on %s.\nTotal run time was'
           ' about %0.4f seconds ####' % (time.asctime(), STOP - START)))
