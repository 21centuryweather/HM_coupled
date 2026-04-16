"""
Edit the configuration values below and run this file in IPython.
"""

import argparse
import xarray as xr
from pathlib import Path
import matplotlib.pyplot as plt
import datetime

############## set up ##############

DEFAULT_ROOT = Path('/scratch/ce10/mjl561/outputs/u-dy159')
DEFAULT_MODEL = 'HM-AU_12_BOM_CLIM_GAL9'
DEFAULT_VARIABLE = 'av_lat_hflx'
DEFAULT_NCDIR = 'SLV1H'
DEFAULT_RUN_LIMIT = None  # limit to first N runs for testing; set to None for no limit

############## functions ##############

def load_variable(root, variable, ncdir='SLV1H', model_filter=None, run_limit=None):
    root_path = Path(root)
    grouped = {}
    run_dirs = sorted(
        path for path in root_path.iterdir()
        if path.is_dir() and path.name[:4].isdigit()
    )
    if run_limit is not None:
        run_dirs = run_dirs[:run_limit]

    for run_dir in run_dirs:
        if model_filter:
            model_dirs = [run_dir / model_filter]
        else:
            model_dirs = sorted(path for path in run_dir.iterdir() if path.is_dir())

        for model_dir in model_dirs:
            if not model_dir.is_dir():
                continue
            model = model_dir.name
            files = sorted(model_dir.glob(f'nc/{ncdir}/{variable}-*.nc'))
            if files:
                grouped.setdefault(model, []).extend(files)

    if not grouped:
        raise FileNotFoundError(
            f'No matching files for {variable} under {root_path}.'
        )

    print('opening datasets...')
    datasets = {}
    for model, model_files in grouped.items():
        ds = xr.open_mfdataset(
            [str(p) for p in model_files],
            combine='by_coords',
            chunks='auto',
            parallel=True,
            decode_timedelta=True,
        )
        if 'time' in ds.coords:
            ds = ds.sortby('time')
        datasets[model] = ds

    print('... done loading datasets.')

    return datasets

def save_model_dataset(root, model, ds, variable, frequency):
    output_dir = Path(root) / model / 'tmp'
    output_dir.mkdir(parents=True, exist_ok=True)
    # ds = ds.chunk({'time': -1, 'latitude': 1, 'longitude': 1})
    encoding = {
        name: {'zlib': True, 'complevel': 1}
        for name in ds.data_vars
    }
    year_months = sorted({str(v) for v in ds['time'].dt.strftime('%Y-%m').values})
    for year_month in year_months:
        print(f'Processing {year_month}...')
        monthly = ds.sel(time=year_month)
        monthly = monthly.chunk({'time': -1, 'latitude': 360, 'longitude': 460})
        chunks = recommend_chunks(monthly, client, chunk_domain='spatial', verbose=True)
        output_path = output_dir / f'{variable}_{frequency}_{year_month}_{model}.nc'
        print(f'Saving {model} {year_month} to {output_path}...')
        monthly.to_netcdf(output_path, encoding=encoding)
    print('... done saving.')


def parse_args():
    parser = argparse.ArgumentParser(
        description='Load and concatenate HM output NetCDF files for a variable.'
    )
    parser.add_argument('--root', type=Path, default=DEFAULT_ROOT, help='Root output directory containing dated run folders.')
    parser.add_argument('--model', default=DEFAULT_MODEL, help='Model directory name to load. Omit to process all models.')
    parser.add_argument('--variable', default=DEFAULT_VARIABLE, help='Variable name to load from nc/<frequency>/<variable>-*.nc files.')
    parser.add_argument('--ncdir', default=DEFAULT_NCDIR, help='NetCDF subdirectory/frequency, for example SLV1H.')
    parser.add_argument('--run-limit', type=int, default=DEFAULT_RUN_LIMIT, help='Optional limit on the number of dated run directories to process.')
    parser.add_argument('--pbs', action='store_true', help='Use PBS mode (auto-detect environment). Omit for interactive mode.')
    return parser.parse_args()

if __name__ == '__main__':
    
    # time the processing
    tic = datetime.datetime.now()

    args = parse_args()
    
    # # sams dask_setup: https://21centuryweather.discourse.group/t/introducing-dask-setup-dask-made-easy-on-gadi/2234
    from dask_setup import setup_dask_client, recommend_chunks, DaskSetupConfig

    if 'client' not in globals():
        client, cluster, temp_dir = setup_dask_client(mode='interactive', workload_type='cpu')

    datasets = load_variable(
        args.root,
        args.variable,
        ncdir=args.ncdir,
        model_filter=None,
        run_limit=args.run_limit,
    )

    # ##### rechunk
    # for model, ds in datasets.items():
    #     datasets[model] = ds.chunk({'time': -1})


    lat, lon = -22.2828, 133.249
    # Extract point values for each model and combine into a single DataArray.
    ref = next(iter(datasets.values()))
    lat_index = ref.get_index('latitude').get_indexer([lat], method='nearest')[0]
    lon_index = ref.get_index('longitude').get_indexer([lon], method='nearest')[0]

    point_series = []
    model_names = []
    for model, ds in datasets.items():
        da = ds[args.variable].isel(latitude=lat_index, longitude=lon_index)
        point_series.append(da)
        model_names.append(model)

    combined_da = xr.concat(point_series, dim='model')
    combined_da = combined_da.assign_coords(model=model_names)

    # groupby hour
    combined_da_hour = combined_da.groupby('time.hour').mean('time')

    # plot
    combined_da_hour.plot.line(x='hour', hue='model')

    toc = datetime.datetime.now()
    print(f'Processing completed in {toc - tic}.')



    # for model, ds in datasets.items():
    #     print(f'Loaded {args.variable} for model {model}:')
    #     print(ds)
    #     save_model_dataset(args.root, model, ds, args.variable, args.ncdir)

    client.close()
    cluster.close()

