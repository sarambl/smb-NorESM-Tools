# %%
from pathlib import Path

import xarray as xr


import subprocess
import sys
import time

import pandas as pd

"""
Example usage:

extract_latlon_grid OsloAero_intBVOC_f19_f19_mg17_full 2012-01-01 2015-01-01 [40,90] [-180,180] /path/to/output/ /path/to/put/temp/data  '.h1.'


Arguments: case_name from_time to_time lat_limits lon_limits path_output path_temp history_field
"""



path_input_data = Path('/proj/bolinc/users/x_sarbl/noresm_archive/')





def convert_lon_to_360(lon):
    return lon % 360


# %%


def check_nrproc(l_df: pd.DataFrame):
    _b = l_df['process'].isnull()
    return len(l_df[~_b])


def check_stat_proc(l_df):
    """
    Check nr of processes done/running or not running
    :param l_df:
    :return:
    """
    nrunning = l_df['status'][l_df['status'] == 'running'].count()
    ndone = l_df['status'][l_df['status'] == 'done'].count()
    nnrunning = l_df['status'][l_df['status'] == 'not running'].count()
    return nrunning, ndone, nnrunning


# %%
def update_stat_proc(r):
    """
    Update row if process is done
    :param r:
    :return:
    """
    if r['status'] == 'done':
        return 'done'
    if r['status'] == 'not running':
        return 'not running'
    else:
        p = r['process']
        p: subprocess.Popen
        if p.poll() is None:
            # (stdout_data, stderr_data) = p.communicate()
            # print('****')
            # print((stdout_data, stderr_data))
            # print('****')

            return 'running'

        else:
            return 'done'


# %%
def launch_ncks(comms, max_launches=20):
    """
    Launch a number of processes to calculate monthly files for file.
    :param comms: list of commands to run
    :param max_launches: maximum launched subprocesses at a time
    :return:
    """
    if len(comms) == 0:
        return
    # Setup dataframe to keep track of processes

    l_df = pd.DataFrame(index=comms, columns=['process', 'status'])
    l_df['status'] = 'not running'
    l_df['status'] = l_df.apply(update_stat_proc, axis=1)
    check_stat_proc(l_df)
    # pyf = sys.executable  # "/persistent01/miniconda3/envs/env_sec_v2/bin/python3"
    # file = package_base_path / 'bs_fdbck'/'preprocess'/'subproc_station_output.py'
    # while loop:
    # mod_load  ='module load NCO/4.7.9-nsc5 &&'
    notdone = True
    while notdone:
        # Update status
        l_df['status'] = l_df.apply(update_stat_proc, axis=1)
        nrunning, ndone, nnrunning = check_stat_proc(l_df)
        # check if done, if so break
        notdone = len(l_df) != ndone
        if notdone is False:
            break
        # If room for one more process:
        if (nrunning < max_launches) and (nnrunning > 0):
            co = l_df[l_df['status'] == 'not running'].iloc[0].name
            print(co)
            # co.comm

            # Launch subprocess:
            p1 = subprocess.Popen([co], shell=True)
            # put subprocess in dataframe
            l_df.loc[co, 'process'] = p1
            l_df.loc[co, 'status'] = 'running'

        print(l_df)
        time.sleep(5)


# %%

def extract_subset(case_name='OsloAero_intBVOC_f19_f19_mg17_full', from_time='2012-01-01', to_time='2015-01-01',
                   lat_lims=None, lon_lims=None, out_folder=None, tmp_folder=None, history_field='.h1.', max_launch=10):
    """

    :param case_name: Name of the case (simulation)
    :param from_time: From when you want to extract data
    :param to_time: To when you want ot extract data
    :param lat_lims: list, limits you want to impose on latitude
    :param lon_lims: list, limits you want to impose on longitude
    :param out_folder: outfolder for data
    :param tmp_folder: where to put the temporary
    :param history_field:
    :return:
    """
    # %%
    # these are the default and you can change them
    if lat_lims is None:
        lat_lims = [60., 66.]
    if lon_lims is None:
        lon_lims = [22., 30.]
    print(lat_lims)
    print(lon_lims)
    lon_lims = [convert_lon_to_360(lon_lims[0]), convert_lon_to_360(lon_lims[1])]
    # this is where the data will be placed if you do not specify other locations
    if out_folder is None:
        out_folder = Path('') / case_name

    if tmp_folder is None:
        tmp_folder = out_folder / 'tmp'

    if not out_folder.exists():
        out_folder.mkdir(parents=True)
    if not tmp_folder.exists():
        tmp_folder.mkdir(parents=True)
    # This is where the data will be read from
    input_folder = path_input_data / case_name / 'atm' / 'hist'
    # %%
    # Just print to inform the user of what is happening
    print(f'case_name: {case_name} \n from time: {from_time} \n to_time: {to_time} \n'
          f' lat_lims: {lat_lims} \n lon_lims_ {lon_lims} \n '
          f'out_folder: {str(out_folder)} \n tmp_folder: {str(tmp_folder)} \n'
          f'input_folder: {input_folder}'
          )
    # %%

    # %%
    # get list of files with correct history_field (e.g. .h1.)
    p = input_folder.glob(f'**/*{history_field}*')

    files = [x for x in p if x.is_file()]
    files.sort()
    files = pd.Series(files)
    print(files)

    # %%
    # stem of the files (just the file name, not the whole path):
    files_stm = [f.stem for f in files]
    # get the data stamp from the file name:
    files_date = [f.split('.')[-1][:-6] for f in files_stm]
    # convert the date stamp to a datetime format
    files_date = [pd.to_datetime(f, format='%Y-%m-%d') for f in files_date]
    # Convert the from time limit to datetime
    from_time_dt = pd.to_datetime(from_time, format='%Y-%m-%d')
    # Convert the to time limit to datetime
    to_time_dt = pd.to_datetime(to_time, format='%Y-%m-%d')
    # make true/false list for which files satisfy the condition of being
    # before to_time but after from_time:
    st = [from_time_dt <= t <= to_time_dt for t in files_date]
    # select only these files:
    files = files[st]
    # %%
    # Chekc if you can load NCO
    try:
        subprocess.run('module load NCO/4.7.9-nsc5', shell=True)
    except FileNotFoundError:
        print('could not load NCO')
    # list of commands to run in bash:
    comms = []
    # output file names:
    files_out = list()
    fp_o = None
    for f in files:
        print(f)
        f_s = f.stem
        fn_o = f_s + '_tmp_subset.nc'
        fp_o = tmp_folder / fn_o
        files_out.append(fp_o)
        # this is checking if the file already exist and then i did some test on the size to check if it
        # was an empty file or not. This is not a problem unless your program is interrupted
        if fp_o.exists():
            size = fp_o.stat().st_size
            print(size)
            # if size of file is large enough, then skip this file because the file is already finished
            if size > 5e6:
                continue
        # use ncks to extract files:
        co = f'ncks -O -d lon,{lon_lims[0]},{lon_lims[1]} -d lat,{lat_lims[0]},{lat_lims[1]} {f} {fp_o}'
        # -v u10max,v10max
        comms.append(co)
    # %%
    # Launch all the commands.
    launch_ncks(comms, max_launches = max_launch)
    print('done with all files, will now concatinate')
    # %%
    # %%
    # name of the final out file:
    fn_out_final = out_folder / f'{case_name}{history_field}_{from_time}-{to_time}_concat_subs_{lon_lims[0]}' \
                                f'-{lon_lims[1]}_{lat_lims[0]}-{lat_lims[1]}.nc'

    # pattern of files to concat:
    files_str_patt = f'{fp_o.parent}/{case_name}*_tmp_subset.nc'

    # %%
    # concatinate with ncrcat:
    com_concat = f'ncrcat {files_str_patt} {fn_out_final}'
    print(com_concat)
    # run the concatination
    subprocess.run(com_concat, shell=True)


# %%


if __name__ == '__main__':
    extract_subset(*sys.argv[1:])

