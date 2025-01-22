# # NHM ncf2zarr Image
#
# This image contains the ncf2zarr program.
FROM nhmusgs_base

LABEL maintainer="rmcd@usgs.gov"

# Copy python script into image
COPY --chown=nhm ncf2zarr.py /opt/conda/bin
RUN chmod 744 /opt/conda/bin/ncf2zarr.py
# ## ncf2zarr Installation Location
#
# The ncf2zarr program installs in `/usr/local/bin`.
COPY --chown=nhm ncf2zarr /usr/local/bin
RUN chmod 744 /usr/local/bin/ncf2zarr

# # ## ncf2zarr User Account
# #
# # The ncf2zarr user account is defined in the [base image
# # Dockerfile](https://code.chs.usgs.gov/wma/nhm/docker-images/-/blob/main/base/Dockerfile).
# USER $USER
# WORKDIR /home/$USER

ENTRYPOINT ["/bin/bash", "-c", "/usr/local/bin/ncf2zarr"]
