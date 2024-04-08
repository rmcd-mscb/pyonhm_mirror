# # NHM Base Image
#
# This is the base image used to build the NHM pipeline. The base
# image is not necessary in a production system, and it is not
# necessary to push it to Docker Hub.
#
# The base image for the NHM base image is
# [docker-miniconda](https://hub.docker.com/r/continuumio/miniconda3).
FROM condaforge/mambaforge:23.3.1-1

LABEL maintainer="ashalper@usgs.gov"

# ## Additional Packages
#
# Several additional packages are installed on the base image to
# provide features necessary for building and running the NHM
# pipeline:
#
#    * Secure networking
#        - [ca-certificates](https://packages.debian.org/bullseye/ca-certificates)
#    * APT package management system
#        - [dialog](https://packages.debian.org/bullseye/dialog)
#    * Pipeline debugging
#        - [file](https://packages.debian.org/bullseye/file)
#    * PRMS build prerequisites
#        - [gcc](https://packages.debian.org/bullseye/gcc)
#        - [gfortran](https://packages.debian.org/bullseye/gfortran)
#        - [libhdf5-mpich-dev](https://packages.debian.org/bullseye/libhdf5-mpich-dev)
#        - [libnetcdf-dev](https://packages.debian.org/bullseye/libnetcdf-dev)
#        - [libnetcdff-dev](https://packages.debian.org/bullseye/libnetcdff-dev)
#        - [make](https://packages.debian.org/bullseye/make)
#    * Docker shared volume support
#        - [procps](https://packages.debian.org/bullseye/procps)
#    * Git source download unarchiving
#        - [unzip](https://packages.debian.org/bullseye/unzip)
#    * Vim for development editing
#        - [vim](https://packages.debian.org/bullseye/vim)
#

# Prevent halting due to tzdata, interactive package config. See
# https://serverfault.com/questions/84521/automate-dpkg-reconfigure-tzdata
RUN ln -fs /usr/share/zoneinfo/UTC /etc/localtime

RUN apt-get update --fix-missing && \
    apt-get -q -y install \
	    apt-utils \
	    ca-certificates \
	    dialog \
	    file \
	    gcc \
	    gfortran \
	    libhdf5-mpich-dev \
	    libnetcdf-dev \
	    libnetcdff-dev \
	    make \
	    procps \
	    unzip \
        vim
RUN apt-get autoclean && apt-get purge

# ## DOI SSL Intercept Root Certificate
#
# The [DOI intercept root
# certificate](https://github.com/usgs/best-practices/blob/master/ssl/WorkingWithinSSLIntercept.md)
# is required within .usgs.gov to make secure connections outside the
# subnet.
RUN if wget http://sslhelp.doi.net/docs/DOIRootCA2.cer ; then \
        mkdir -p /usr/local/share/ca-certificates ; \
        mv DOIRootCA2.cer /usr/local/share/ca-certificates/DOIRootCA2.crt ; \
        update-ca-certificates ; \
    fi

RUN export SSL_CERT_FILE=/usr/local/share/ca-certificates/DOIRootCA2.crt
RUN export CURL_CA_BUNDLE="${SSL_CERT_FILE}"
RUN export PIP_CERT="${SSL_CERT_FILE}"
# Add the DOI SSL Intercept Root Certificate
# RUN wget http://sslhelp.doi.net/docs/DOIRootCA2.cer -O /usr/local/share/ca-certificates/DOIRootCA2.crt && \
#     update-ca-certificates

# Set environment variables to use the custom certificate
# ENV SSL_CERT_FILE=/usr/local/share/ca-certificates/DOIRootCA2.crt
# ENV CURL_CA_BUNDLE=${SSL_CERT_FILE}
# ENV PIP_CERT=${SSL_CERT_FILE}
# ENV REQUESTS_CA_BUNDLE=${SSL_CERT_FILE}
# ENV GIT_SSL_CAINFO=${SSL_CERT_FILE}
# RUN mamba config --set ssl_verify /usr/local/share/ca-certificates/DOIRootCA2.crt

# ## Conda Installation
#
# The base image Conda packages are updated to the most recent
# versions available.
# ### Additional Conda Packages
#
# Additional specific versions of Conda packages are required for
# Python support in the gridmet, gridmetetl, gridmets2s, ncf2cbh,
# cbhfiller, out2ncf, and verifier images.
# Combining mamba install and pip install in a single RUN command
RUN mamba update -n base mamba -y && \
    mamba install -n base -c conda-forge -y \
        python \
        pip \
        gdptools=0.2.7 \
        dask \
        bottleneck \
        geopandas \
        nco \
        netcdf4 \
        pandas \
        requests \
        xarray \
        xmltodict \
        pint-xarray && \
    mamba clean -a && \
    /bin/bash -c "source activate base"
    # && pip --trusted-host pypi.org --trusted-host files.pythonhosted.org install -vvv git+https://code.chs.usgs.gov/wma/nhm/gridmetetl.git@check2

# ## Source Code Location
#
# The NHM source code location can be configured by setting the base
# image's `NHM_SOURCE_DIR` environment variable.
ENV NHM_SOURCE_DIR=/usr/local/src

# ## ONHM Runners Version
#
# The version of the ONHM Runners source referenced by the ncf2cbh,
# cbhfiller, and out2ncf images can be declared by setting
# the `VERSION_ONHM_RUNNERS` argument in the base image.
ARG VERSION_ONHM_RUNNERS=0.2.5
ARG VERSION_ONHM_RUNNERS=main
RUN git -c advice.detachedHead=false clone \
    https://code.chs.usgs.gov/wma/nhm/onhm-runners.git \
    --branch $VERSION_ONHM_RUNNERS --depth=1 $NHM_SOURCE_DIR/onhm-runners && \
    cd $NHM_SOURCE_DIR/onhm-runners && \
    rm -rf .git && \
    rm .gitignore README.md .project .pydevproject

# RUN pip --trusted-host pypi.org --trusted-host files.pythonhosted.org install -vvv git+https://code.chs.usgs.gov/wma/nhm/gridmetetl.git@check2
RUN pip --trusted-host pypi.org --trusted-host files.pythonhosted.org install -vvv git+https://github.com/rmcd-mscb/gridmet-etl.git

# ## gridmetetl Output Directory
#
# The gridmetetl output directory is `/nhm/gridmetetl/Output`.
RUN mkdir -p /nhm/gridmetetl/Output

# ## NHM User Account
#
# The NHM user account can be set by the `USER` environment variable
# in the base image.
ENV USER=nhm
RUN useradd -ms /bin/bash $USER
# RUN chown -R $USER /nhm
# RUN chgrp -R $USER /nhm
RUN chown -R nhm:nhm /nhm
RUN chmod -R 755 /nhm
ENV HOMEDIR=/home/$USER

# ## Shared Volume
#
# Pipeline I/O occurs on the `/nhm` shared volume. `/nhm/` is mounted
# on all containers except the gridmet container (which only queries
# the gridMET server, and does not require persistent storage).
VOLUME ["/nhm"]
