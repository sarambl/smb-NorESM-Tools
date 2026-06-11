import pandas as pd
import numpy as np
import xarray as xr
path_salsa_bin = 'echam_salsa_info/bins_salsa.csv'

# %%
df_bins_salsa =pd.read_csv(path_salsa_bin, index_col=0)

df_bins_salsa_nm = df_bins_salsa*1e9
df_bins_salsa_nm = df_bins_salsa_nm.rename({
    'Bin Lower Bound (m)':'Bin Lower Bound (nm)',
    'Bin Center Diameter (m)':'Bin Center Diameter (nm)',
    'Bin Upper Bound (m)':'Bin Upper Bound (nm)',
}, axis=1)
#df_bins_salsa_nm

salsa_pbs = list(df_bins_salsa_nm.index)



def get_bin_up_low(pb, unit='nm'):
    if unit=='nm':
        return df_bins_salsa_nm.loc[pb,'Bin Lower Bound (nm)'],df_bins_salsa_nm.loc[pb,'Bin Upper Bound (nm)']
    else:
        return df_bins_salsa.loc[pb,'Bin Lower Bound (m)'],df_bins_salsa.loc[pb,'Bin Upper Bound (m)']

def get_bin_width(pb, unit='nm'):
    if unit=='nm':
        return df_bins_salsa_nm.loc[pb,'Bin Upper Bound (nm)']-df_bins_salsa_nm.loc[pb,'Bin Lower Bound (nm)']
    else:
        return df_bins_salsa.loc[pb,'Bin Upper Bound (m)']-df_bins_salsa.loc[pb,'Bin Lower Bound (m)']

def get_bin_width_log10(pb):
    return np.log10(df_bins_salsa_nm.loc[pb,'Bin Upper Bound (nm)'])-np.log10(df_bins_salsa_nm.loc[pb,'Bin Lower Bound (nm)'])

def get_bin_width_log(pb):
    return np.log(df_bins_salsa_nm.loc[pb,'Bin Upper Bound (nm)'])-np.log(df_bins_salsa_nm.loc[pb,'Bin Lower Bound (nm)'])
# %%


vars_ccn = [f'conccnmode{m}' for m in salsa_pbs]
vars_ddiam = [f'ddrymode{m}' for m in salsa_pbs]

def dN_salsa_mod(ds,pb, diameters):
    _ds = xr.Dataset(coords={'diameter':diameters})

    ll,ul = get_bin_up_low(pb)
    bw_log10 = get_bin_width_log10(pb)
    tf = (ll<_ds['diameter'])&(_ds['diameter']<=ul)
    vnum = f'conccnmode{pb}'
    num = ds[vnum]
    ds[f'dN_{pb}'] = tf*num
    ds[f'dNdlog10D_{pb}'] = ds[f'dN_{pb}']/bw_log10
    return ds



def add_dNdlogD10_salsa(ds, diameters):
    ls_added_dNdlogD = list()
    for pb in salsa_pbs:
        print(pb)
        ds = dN_salsa_mod(ds,pb, diameters)
        ls_added_dNdlogD.append(f'dNdlog10D_{pb}')
        ls_added_dNdlogD.append(f'dNdlog10D_{pb}')
    ds['dNdlog10D'] = ds[ls_added_dNdlogD].to_array().sum('variable')
    return ds
