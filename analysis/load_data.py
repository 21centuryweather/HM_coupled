"""
Edit the configuration values below and run this file in IPython.
"""

import argparse
import xarray as xr
from pathlib import Path

############## set up ##############

DEFAULT_ROOT = Path('/scratch/ce10/mjl561/outputs/u-dy159')
DEFAULT_MODEL = 'HM-AU_12_BOM_CLIM_GAL9'
DEFAULT_VARIABLE = 'av_lat_hflx'
DEFAULT_NCDIR = 'SLV1H'
DEFAULT_RUN_LIMIT = None # limit to first N runs for testing; set to None for no limit

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
    if not run_dirs:
        raise FileNotFoundError(
            f'No run directories found under {root_path}.'
        )
    print(f'Using runs: {[path.name for path in run_dirs]}')

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

    return datasets

def save_model_dataset(root, model, ds, variable, frequency):
    output_dir = Path(root) / model
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / f'{variable}_{frequency}_concat.nc'
    # ds = ds.chunk({'time': -1, 'latitude': 1, 'longitude': 1})
    encoding = {
        name: {'zlib': True, 'complevel': 1}
        for name in ds.data_vars
    }
    print(f'Saving {model} to {output_path}')
    ds.to_netcdf(output_path, encoding=encoding)


def parse_args():
    parser = argparse.ArgumentParser(
        description='Load and concatenate HM output NetCDF files for a variable.'
    )
    parser.add_argument(
        '--root',
        type=Path,
        default=DEFAULT_ROOT,
        help='Root output directory containing dated run folders.',
    )
    parser.add_argument(
        '--model',
        default=DEFAULT_MODEL,
        help='Model directory name to load. Omit to process all models.',
    )
    parser.add_argument(
        '--variable',
        default=DEFAULT_VARIABLE,
        help='Variable name to load from nc/<frequency>/<variable>-*.nc files.',
    )
    parser.add_argument(
        '--ncdir',
        default=DEFAULT_NCDIR,
        help='NetCDF subdirectory/frequency, for example SLV1H.',
    )
    parser.add_argument(
        '--run-limit',
        type=int,
        default=DEFAULT_RUN_LIMIT,
        help='Optional limit on the number of dated run directories to process.',
    )
    return parser.parse_args()


def main():
    args = parse_args()

    from dask_setup import setup_dask_client

    if 'client' not in globals():
        client, cluster, temp_dir = setup_dask_client(workload_type='io')
    datasets = load_variable(
        args.root,
        args.variable,
        ncdir=args.ncdir,
        model_filter=args.model,
        run_limit=args.run_limit,
    )

    for model, ds in datasets.items():
        print(f'Loaded {args.variable} for model {model}:')
        print(ds)
        save_model_dataset(args.root, model, ds, args.variable, args.ncdir)


if __name__ == '__main__':
    main()

