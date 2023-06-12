import numpy as np
import xarray as xr


def prep_sizedist_vars(ds, var_diam='D', v_dNdlog10D='particle_number_size_distribution_amean'):
    v_log10D = 'log10D'
    ds[v_log10D] = np.log10(ds[var_diam])
    mid_points = (ds[v_log10D].values[0:-1] + ds[v_log10D].values[1:]) / 2
    bottom = ds[v_log10D].values[0] - (mid_points[0] - ds[v_log10D].values[0])
    top = ds[v_log10D].values[-1] + (mid_points[-1] - ds[v_log10D].values[-2])

    d_lims = np.concatenate([np.array([bottom]), mid_points, np.array([top])])

    d_lims = 10 ** d_lims

    ds['bottom'] = xr.DataArray(d_lims[0:-1].transpose(), dims={var_diam: ds[var_diam]})
    ds['top'] = xr.DataArray(d_lims[1:].transpose(), dims={var_diam: ds[var_diam]})

    ds['diam_lims'] = ds[['bottom', 'top']].to_array(dim='limit')
    # compute dlogD:
    dlog10D = (np.log10(ds['diam_lims'].sel(limit='top')) - np.log10(ds['diam_lims'].sel(limit='bottom')))

    ds['dlog10D'] = xr.DataArray(dlog10D, dims={var_diam: ds[var_diam]})

    ds['log10D'] = np.log10(ds[var_diam])
    # compute number of particles in each bin:
    ds['dN'] = ds[v_dNdlog10D] * ds['dlog10D']
    return ds



def calc_Nx_interpolate_first(ds, x=100, var_diam='D', v_dNdlog10D='particle_number_size_distribution_amean'):
    ds = prep_sizedist_vars(ds, var_diam=var_diam, v_dNdlog10D=v_dNdlog10D)
    ds['log10D'] = np.log10(ds[var_diam])
    ds_log10 = ds.swap_dims({var_diam: 'log10D'})

    ds_log10 = ds_log10.interp({'log10D': np.linspace(ds['log10D'].min(), ds['log10D'].max())})
    Nx = ds_log10['dNdlogD'].sel({'log10D': slice(np.log10(x), None)}).integrate(dim='log10D')
    return Nx









def calc_Nx_interpolate_first_bin_wise(ds, x=100, var_diam='D',
                    v_dNdlog10D='particle_number_size_distribution_amean'):
    ds = prep_sizedist_vars(ds, var_diam=var_diam, v_dNdlog10D=v_dNdlog10D)
    arg_gt_x = int(ds[var_diam].where(ds['diam_lims'].sel(limit='bottom') > x).argmin().values)
    # get limits for grid box below
    # In log space...
    d_below = np.log10(ds['diam_lims'].isel(**{var_diam: (arg_gt_x - 1)}).sel(limit='bottom'))
    d_above = np.log10(ds['diam_lims'].isel(**{var_diam: (arg_gt_x - 1)}).sel(limit='top'))
    # fraction of gridbox above limit:
    frac_ab = (d_above - np.log10(x)) / (d_above - d_below)
    # Include the fraction of the bin box above limit:
    add = ds['dN'].isel(**{var_diam: (arg_gt_x - 1)}) * frac_ab
    Nx_orig = ds['dN'].isel(**{var_diam: slice(arg_gt_x, None)}).sum(var_diam) + add
    return Nx_orig


def _compute_default_int(ds, x=100, var_diam='D', v_dNdlog10D='particle_number_size_distribution_amean'):
    ds = prep_sizedist_vars(ds, var_diam=var_diam, v_dNdlog10D=v_dNdlog10D)
    arg_gt_x = int(ds[var_diam].where(ds['diam_lims'].sel(limit='bottom') > x).argmin().values)
    # get limits for grid box below
    # In log space...
    d_below = np.log10(ds['diam_lims'].isel(**{var_diam: (arg_gt_x - 1)}).sel(limit='bottom'))
    d_above = np.log10(ds['diam_lims'].isel(**{var_diam: (arg_gt_x - 1)}).sel(limit='top'))
    # fraction of gridbox above limit:
    frac_ab = (d_above - np.log10(x)) / (d_above - d_below)
    # Include the fraction of the bin box above limit:
    add = ds['dN'].isel(**{var_diam: (arg_gt_x - 1)}) * frac_ab

    # Nx_orig = ds['dN'].isel(**{var_diam:slice(arg_gt_x,None)}).sum(var_diam) + add
    ds['range'] = xr.DataArray(np.arange(len(ds['diameter'])), dims={'diameter': ds['diameter']}, )

    ds_sd = ds.swap_dims({var_diam: 'range'})

    Nx = ds_sd['dN'].integrate(dim='range')
    return Nx + add


def compute_trapez(ds, x=100, var_diam='D', v_dNdlog10D='particle_number_size_distribution_amean'):
    ds = prep_sizedist_vars(ds, var_diam=var_diam, v_dNdlog10D=v_dNdlog10D)
    # get limits for grid box below
    # In log space...

    ds['log10D'] = np.log10(ds[var_diam])
    ds_log10 = ds.swap_dims({var_diam: 'log10D'})
    ds_sel = ds_log10.sel({'log10D': slice(np.log10(x), None)})

    ds_sel['Nx'] = xr.DataArray(np.trapz(ds_sel[v_dNdlog10D].values, ds_sel['log10D'].values, axis=0),
                                dims={'time': ds_sel['time']})

    return ds_sel['Nx']




def _compute_interpolate_except_last(ds, x=100, var_diam='D', v_dNdlog10D='particle_number_size_distribution_amean'):
    ds = prep_sizedist_vars(ds, var_diam=var_diam, v_dNdlog10D=v_dNdlog10D)

    arg_gt_x = int(ds[var_diam].where(ds['diam_lims'].sel(limit='bottom') > x).argmin().values)
    # get limits for grid box below
    # In log space...
    # fraction of gridbox above limit:
    # Include the fraction of the bin box above limit:
    arg_gt_x = int(ds[var_diam].where(ds['diam_lims'].sel(limit='bottom') > x).argmin().values)
    # get limits for grid box below
    # In log space...
    d_below = np.log10(ds['diam_lims'].isel(**{var_diam: (arg_gt_x - 1)}).sel(limit='bottom'))
    d_above = np.log10(ds['diam_lims'].isel(**{var_diam: (arg_gt_x - 1)}).sel(limit='top'))
    # fraction of gridbox above limit:
    frac_ab = (d_above - np.log10(x)) / (d_above - d_below)
    # Include the fraction of the bin box above limit:
    add = ds['dN'].isel(**{var_diam: (arg_gt_x - 1)}) * frac_ab



    ds_sel = ds

    ds_sel['log10D'] = np.log10(ds_sel[var_diam])
    ds_log10 = ds_sel.swap_dims({var_diam: 'log10D'})
    d_int_from = float(ds.isel({var_diam: arg_gt_x}).sel(limit='bottom')['diam_lims'])
    ds_log10 = ds_log10.interp({'log10D': np.linspace(ds_log10['log10D'].min(), ds_log10['log10D'].max())})

    Nx = ds_log10['dNdlogD'].sel(log10D=slice(np.log10(d_int_from), None)).integrate(dim='log10D')
    return Nx+ add


def _integrate_no_interp(ds, x=100, var_diam='D', v_dNdlog10D='particle_number_size_distribution_amean'):
    ds = prep_sizedist_vars(ds, var_diam=var_diam, v_dNdlog10D=v_dNdlog10D)
    ds['log10D'] = np.log10(ds[var_diam])
    ds_log10 = ds.swap_dims({var_diam: 'log10D'})
    Nx = ds_log10['dNdlogD'].sel({'log10D': slice(np.log10(x), None)}).integrate(dim='log10D')
    return Nx
