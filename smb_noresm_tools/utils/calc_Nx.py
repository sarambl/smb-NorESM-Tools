from scipy.stats import lognorm
import numpy as np
import xarray as xr

# %%
sized_varListNorESM = {'NCONC': ['NCONC00', 'NCONC01', 'NCONC02', 'NCONC04', 'NCONC05', 'NCONC06', 'NCONC07', 'NCONC08',
                                 'NCONC09', 'NCONC10', 'NCONC12', 'NCONC14'],
                       'SIGMA': ['SIGMA00', 'SIGMA01', 'SIGMA02', 'SIGMA04', 'SIGMA05', 'SIGMA06', 'SIGMA07', 'SIGMA08',
                                 'SIGMA09', 'SIGMA10', 'SIGMA12', 'SIGMA14'],
                       'NMR': ['NMR00', 'NMR01', 'NMR02', 'NMR04', 'NMR05', 'NMR06', 'NMR07', 'NMR08',
                               'NMR09', 'NMR10', 'NMR12', 'NMR14']}


def add_00_mode(dtset):
    """
    Adds values for mode 0

    NB: MAKE SURE IT's THE RIGHT UNITS
    :param dtset:
    :return:
    """
    if 'NNAT_0' in dtset.data_vars:
        dtset['SIGMA00'] = 1.6  # Kirkevag et al 2018
        dtset['SIGMA00'].attrs['units'] = '-'  # Kirkevag et al 2018
        # NB: Make sure it's the right units!
        dtset['NMR00'] = 62.6  # nm Kirkevag et al 2018
        dtset['NMR00'].attrs['units'] = 'nm'  # nm Kirkevag et al 2018
        dtset['NCONC00'] = dtset['NNAT_0']

    return dtset


def calc_Nd_interval_NorESM(input_ds, fromD, toD=None, varNameN=None):
    """
    Returns data array with concentration of N between diameter fromD and toD.
    Note that the fromD and toD are assumed to be in nm.
    :param input_ds: ds containing all the variables needed
    :param fromD: diameter from in nm
    :param toD: diameter to in nm
    :param varNameN: output name.
    :return:
    """
    if varNameN is None:
        if toD is None:
            varNameN = f'N{fromD:d}'
        else:
            varNameN = f'N{fromD:d}-{toD:d}'
    # %%
    # %%
    dummy_var = sized_varListNorESM['NCONC'][0]
    da_Nd = xr.DataArray(0, coords=input_ds[dummy_var].coords)  # keep dimensions, zero value
    da_Nd.name = varNameN
    # %%

    varsNCONC = sized_varListNorESM['NCONC']
    varsNMR = sized_varListNorESM['NMR']
    varsSIG = sized_varListNorESM['SIGMA']
    # %%
    input_ds = add_00_mode(input_ds)
    # %%
    for varN, varSIG, varNMR in zip(varsNCONC, varsSIG, varsNMR):
        NCONC = input_ds[varN].values  # *10**(-6) #m-3 --> cm-3
        SIGMA = input_ds[varSIG].values  #
        NMD = input_ds[varNMR].values * 2  # radius --> diameter
        if input_ds[varNMR].attrs['units'] == 'm':
            NMD = NMD * 1.e9  # converting from m to nm
        dummy = calc_Nx_modei(NCONC, NMD, SIGMA, fromD, toD)
        # if NMR=0 --> nan values. We set these to zero:

        dummy[NMD == 0] = 0.
        dummy[NCONC == 0] = 0.
        dummy[np.isnan(NCONC)] = np.nan
        da_Nd += dummy

    return da_Nd


def calc_Nx_modei(NCONC, NMD, SIGMA, fromD, toD=None):
    """
    Calculate Nx1-x2 for some mode with NCONC, NMD and SIGMA.
    :param NCONC:
    :param NMD:
    :param SIGMA:
    :param fromD:
    :param toD:
    :return:
    """
    if toD is None:
        dummy = NCONC * (1 - lognorm.cdf(fromD, np.log(SIGMA), scale=NMD))

    else:
        dummy = NCONC * (lognorm.cdf(toD, np.log(SIGMA), scale=NMD) -
                         lognorm.cdf(fromD, np.log(SIGMA), scale=NMD))
    return dummy
