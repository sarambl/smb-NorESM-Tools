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

varlist_default = ['PRECC','PRECL','PRECSC','PRECSL','CLOUD','LCLOUD',  'FREQL', 'FREQI', 'FCTL', 'FCTI', 'CLDTOT',]


path_input_data = Path('/proj/bolinc/users/x_sarbl/noresm_archive/')





def convert_lon_to_360(lon):
    return float(lon)% 360
# %%



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

def extract_subset(case_name='OsloAeroSec_intBVOC_f19_f19_mg17_ssp245', from_time='2012-01-01', to_time='2015-01-01',
                   out_base=None,
                   lat_lims=None, lon_lims=None, varlist=None, output_folder=None, history_field='.h1.', max_launch=10):
    """

    :param case_name: Name of the case (simulation)
    :param from_time: From when you want to extract data
    :param to_time: To when you want ot extract data
    :param lat_lims: list, limits you want to impose on latitude
    :param lon_lims: list, limits you want to impose on longitude
    :param out_base: outfolder for data
    :param output_folder: where to put the temporary
    :param history_field:
    :return:
    """
    # %%
    print('HEEEY')
    # these are the default and you can change them
    if lat_lims =='None':
        lat_lims = None
    if lon_lims =='None':
        lon_lims = None
    if varlist is None:
        varlist = varlist_default
    #if lat_lims is None:
    #    lat_lims = [-90., 90.]
    if lat_lims is not None:
        lower_lat = int(lat_lims.split(',')[0][1:])
        upper_lat = int(lat_lims.split(',')[1][:-1])
        lat_lims = [lower_lat, upper_lat]
        lat_lims = [float(lat_lims[0]),float(lat_lims[1])]
        lat_lims_str = '_'.join(lat_lims)
    else:
        lat_lims_str =''

    #if lon_lims is None:
    #    lon_lims = [0., 360.]
    if lon_lims is not None:
        lower_lon = int(lon_lims.split(',')[0][1:])
        upper_lon = int(lon_lims.split(',')[1][:-1])
        lon_lims = [lower_lon, upper_lon]
        lon_lims = [convert_lon_to_360(lon_lims[0]), convert_lon_to_360(lon_lims[1])]
        lon_lims_str = '_'.join(lon_lims)
    else:
        lon_lims_str =''

    print(lat_lims)
    print(lon_lims)

    if lon_lims == [0,360]:
        lon_lims = None
        print('no lon limits applied')
    
    # this is where the data will be placed if you do not specify other locations
    if out_base is None:
        out_base = Path('') / (case_name + '_subset')
    else:
        out_base = Path(out_base)

    if output_folder is None:
        output_folder = out_base /  (case_name + '_subset')
    else:
        output_folder = Path(output_folder)
    print(output_folder)
    if not out_base.exists():
        out_base.mkdir(parents=True)
    if not output_folder.exists():
        output_folder.mkdir(parents=True)
    # This is where the data will be read from
    input_folder = path_input_data / case_name / 'atm' / 'hist'
    # %%
    # Just print to inform the user of what is happening
    print(f'case_name: {case_name} \n from time: {from_time} \n to_time: {to_time} \n'
          f'variable list: {varlist_default}'
          f' lat_lims: {lat_lims} \n lon_lims_ {lon_lims} \n '
          f'out_folder: {str(out_base)} \n tmp_folder: {str(output_folder)} \n'
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
    print(files)
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
        fn_o = f_s + '_tmp_subset.nc' #remove below as well if you remove lon!
        fp_o = output_folder / fn_o
        files_out.append(fp_o)
        # this is checking if the file already exist and then i did some test on the size to check if it
        # was an empty file or not. This is not a problem unless your program is interruptedls
        if fp_o.exists():
            size = fp_o.stat().st_size
            print(size)
            # if size of file is large enough, then skip this file because the file is already finished
            if size > 5e6:
                continue
        # use ncks to extract files:
        if lon_lims == None and lat_lims == None:
            str_varl = ','.join(varlist)
            co = f'ncks -O -v {str_varl} {f} {fp_o}'
        elif lon_lims ==None:
            co = f'ncks -O -v {str_varl} -d lat,{lat_lims[0]},{lat_lims[1]} {f} {fp_o}'
        else:
            co = f'ncks -O -v {str_varl} -d lon,{lon_lims[0]},{lon_lims[1]} -d lat,{lat_lims[0]},{lat_lims[1]} {f} {fp_o}'
        # -v u10max,v10max
        comms.append(co)
    # %%
    files_str_patt = f'{fp_o.parent}/{case_name}*_tmp_subset.nc'
    #print(f'Removing all files in:{files_str_patt} ')
    #comm_rm = f'rm {files_str_patt}'
    #subprocess.run(comm_rm, shell=True)

    # Launch all the commands.
    launch_ncks(comms, max_launches = max_launch)
    print('done with all files, will now concatinate')
    # %%
    # %%
    # name of the final out file:
    fn_out_final = out_base / f'{case_name}{history_field}_{from_time}-{to_time}_concat_subs_' \
                                f'{lon_lims_str}-{lat_lims_str}.nc'

    # pattern of files to concat:


    # %%
    # concatinate with ncrcat:
    #com_concat = f'ncrcat {files_str_patt} {fn_out_final}'
    #print(com_concat)
    # run the concatination
    #subprocess.run(com_concat, shell=True)


# %%


if __name__ == '__main__':
    extract_subset(*sys.argv[1:])
