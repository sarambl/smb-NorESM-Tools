import xarray as xr
import numpy as np

"""
This file contains two functions for calculating the number size distribution in 
NorESM (or other log normal parameters for that matter).
There are two alternatives: 
compute_dNdlogD_mod
aand
lognormal_julia

And these do the same.
"""




def compute_dNdlogD_mod(diameter,
                         input_ds,
                         mode_num,
                         ):
    """
    Returns the number distribution in diameters given as input, for
    mode number <mode_num> and for an input_ds which contains
    'NCONC<mode_num>','SIGMA<mode_num>' and 'NMR<mode_num>'.
    :param diameter:
    :param input_ds:
    :param mode_num:
    :return:
    """

    varN = f'NCONC{mode_num:02d}'
    varNMR = f'NMR{mode_num:02d}'
    varNMD = f'NMD{mode_num:02d}'
    varSIG = f'SIGMA{mode_num:02d}'
    dNdlogD_var = f'dN(mode {mode_num:02d})/dlogD'
    input_ds[varNMD] = 2.*input_ds[varNMR]
    size_dtset = xr.Dataset(coords={**input_ds.coords, 'diameter': diameter})
    # varNMR = varListNorESM['NMR'][i]
    NCONC = input_ds[varN]  # [::]*10**(-6) #m-3 --> cm-3
    SIGMA = input_ds[varSIG]  # [::]#*10**6
    NMD = input_ds[varNMD]   # radius --> diameter
    # number:
    size_dtset[dNdlogD_var] = dNdlogD_modal(NCONC, NMD, SIGMA, diameter)
    size_dtset[dNdlogD_var].attrs['units'] = 'cm-3'
    size_dtset[dNdlogD_var].attrs['long_name'] = 'dN/dlogD (mode' + dNdlogD_var[-2:] + ')'
    return size_dtset


def dNdlogD_sec(diameter, SOA, SO4, num, bin_diameter_int):
    """
    Calculate dNdlogD sectional for individual bin
    :param diameter:
    :param SOA:
    :param SO4:
    :param num:
    :param bin_diameter_int:
    :return:
    """
    # SECnr = self.nr_bins
    # binDiam_l = self.bin_diameter_int  # binDiameter_l
    if type(num) is str:
        num = int(num)

    diam_u = bin_diameter_int[num]  # bin upper lim
    diam_l = bin_diameter_int[num - 1]  # bin lower lim

    SOA, dum = xr.broadcast(SOA, diameter)  # .values  # *1e-6
    SO4, dum = xr.broadcast(SO4, diameter)  # .values  # *1e-6
    dNdlogD = (SOA + SO4) / (np.log(diam_u / diam_l))  #
    in_xr = dict(diameter=diameter[(diameter >= diam_l) & (diameter < diam_u)])
    out_da = xr.DataArray(np.zeros_like(dNdlogD.values), coords=dNdlogD.coords)  # dNdlogD*0.#, diameter)
    out_da.loc[in_xr] = dNdlogD.loc[in_xr]
    return out_da



def dNdlogD_modal(NCONC, NMD, SIGMA, diameter):
    """
    computes sizedist.
    :param NCONC:
    :param NMD: in diameter!!
    :param SIGMA:
    :param diameter:
    :return:
    """
    da = NCONC / (np.log10(SIGMA) * np.sqrt(2 * np.pi)) * np.exp(
        -(np.log10(diameter) - np.log10(NMD)) ** 2 / (2 * np.log10(SIGMA) ** 2))
    return da





def lognormal_julia(x, N, mu, sigma):
    """
    Function that defines the lognormal distribution.

    Parameters:
        x        :   (np.array) The particle diameters (in micrometres) at which you wan to evaluate dN/dlogD
        N        :   The number concentration in this particular aerosol mode (MODEL OUTPUT)
        mu       :   The mean modal radius (NB!) (MODEL OUTPUT)
        sigma :   The  standard deviation of this mode (MODEL OUTPUT)

    Returns an array of the same size as x, with dNdlogD values.
    """
    logsigma = np.log10(sigma)
    return N * (1/np.sqrt(2*np.pi)) * (1/logsigma) * np.exp(-np.log10(x/(2*mu))**2 / (2 * logsigma**2))