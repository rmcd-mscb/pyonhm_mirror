import argparse
import logging
import os
import xarray as xr
from pathlib import Path
import zarr
import glob

logger = logging.getLogger(__name__)

def create_ensemble_zarr(base_dir, output_zarr_path):
    """
    Reads a set of NetCDF files from ensemble directories, adds an ensemble dimension,
    and stores the combined dataset as a single Zarr file.

    Parameters:
    - base_dir: str
        The base directory containing date-stamped folders with ensemble subdirectories.
        e.g., "forecast/output/ensembles/<date>"
    - output_zarr_path: str
        The file path for the output Zarr file.
    """
    compressor = zarr.Blosc(cname="zstd", clevel=3, shuffle=2)


    # For each date directory, list ensemble subdirectories sorted numerically
    ensemble_dirs = sorted(
        glob.glob(os.path.join(base_dir, "ensemble_*")),
        key=lambda x: int(os.path.basename(x).split('_')[-1])
    )

    # Create an empty dataset to initialize the Zarr store
    zarr_initialized = False

    for ens_dir in ensemble_dirs:
        # Use xarray to open all NetCDF files in the ensemble directory.
        file_pattern = os.path.join(ens_dir, "*.nc")
        ds = xr.open_mfdataset(file_pattern, combine='by_coords', parallel=True, chunks={})

        # Extract ensemble index from folder name, e.g., ensemble_0 -> 0
        ens_index = int(os.path.basename(ens_dir).split('_')[-1])
        ds = ds.expand_dims(ensemble=[ens_index])
        # Example encoding for variables
        # Define encoding only for the initial write
        encoding = {var: {"compressor": compressor} for var in ds.data_vars}
        if not zarr_initialized:
            # Initialize the Zarr store
            ds.to_zarr(output_zarr_path, mode='w', consolidated=True, encoding=encoding, compute=True)
            zarr_initialized = True
        else:
            # Append to the existing Zarr store
            ds.to_zarr(output_zarr_path, mode='a', consolidated=True, append_dim='ensemble', compute=True)

        print(f"Processed ensemble: {ens_index}")

    print(f"Ensemble Zarr file created at: {output_zarr_path}")

def main():
    parser = argparse.ArgumentParser(description="Convert NetCDF files to Zarr.")
    parser.add_argument("--output-path", required=True, help="Directory containing NetCDF files.")
    parser.add_argument(
        "--mode", required=True, choices=["op", "median", "ensemble"],
        help="Mode of operation: 'op' for operation, 'median' for median computation, 'ensemble' for ensemble processing."
    )
    
    args = parser.parse_args()
    output_path = Path(args.output_path)
    output_file = output_path / f"{output_path.name}.zarr"
    
    try:
        if args.mode == "ensemble":
            logger.info(f"Converting ensemble output in {str(output_path)} to zarr {output_file}")
            create_ensemble_zarr(base_dir=output_path, output_zarr_path=output_file)
        else:
            raise NotImplementedError(f"Mode '{args.mode}' is not implemented yet.")
    except Exception as e:
        print(f"Error during conversion: {e}")
        exit(1)

if __name__ == "__main__":
    main()
