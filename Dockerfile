FROM mambaorg/micromamba:1.1.0 as app

# build and run as root users since micromamba image has 'mambauser' set as the $USER
USER root
# set workdir to default for building; set to /data at the end
WORKDIR /

# metadata labels
LABEL base.image="mambaorg/micromamba:1.1.0"
LABEL dockerfile.version="1"
LABEL software="Theiagen Utility Scripts"
LABEL software.version="1"
LABEL description="Conda environment for running Theiagen's utility scripts"
LABEL website="https://github.com/theiagen/utilities"
LABEL license="GNU GPL v3"
LABEL license.url="https://github.com/theiagen/utilities/blob/main/LICENSE"
LABEL maintainer1="Curtis Kapsak"
LABEL maintainer1.email="curtis.kapsak@theiagen.com"

# install dependencies; cleanup apt garbage; make /data directory
RUN apt-get update && apt-get install -y --no-install-recommends \
 wget \
 ca-certificates \
 git \
 procps && \
 apt-get autoclean && rm -rf /var/lib/apt/lists/* && mkdir /data

# copy in theiagen/utilities scripts
COPY scripts/ /utilities/scripts

# copy in reference files
COPY reference_files/ /utilities/reference_files

# copy in conda environment yml file
COPY env.yml /utilities/env.yml

# install things into base conda/mamba environment
RUN micromamba install -n base -f /utilities/env.yml -y && \
 micromamba clean -a -y

# so that mamba/conda env is active when running below commands
ENV ENV_NAME="base"
ARG MAMBA_DOCKERFILE_ACTIVATE=1

# final working directory
WORKDIR /data

# hardcode executables from base conda env into the PATH variable; LC_ALL for singularity compatibility
ENV PATH="${PATH}:/opt/conda/bin/:/utilities/scripts" \
 LC_ALL=C.UTF-8

# new base for testing
FROM app as test

# so that mamba/conda env is active when running below commands
ENV ENV_NAME="base"
ARG MAMBA_DOCKERFILE_ACTIVATE=1

# simple test to see versions
RUN micromamba list
