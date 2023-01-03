# utilities

A collection of general utility scripts

## Docker

This repo includes a Dockerfile, which is used for building a docker image that contains the scripts & files from this repo.

To build the docker image locally (and optionally push the image to quay), run the following:

```bash
# clone the code locally
git clone https://github.com/theiagen/utilities.git
cd utilities

# build the docker image on your local machine, build both the app and test layer
docker build --target test -t theiagen/utilities:latest .

# build the docker image on your local machine, only build app layer, NOT test layer
# Use this for deploying new docker image to quay
docker build --target app -t quay.io/theiagen/utilities:latest .

# (optionally) push the docker image to Theiagen quay repo for usage in Terra
docker push quay.io/theiagen/utilities:latest
```
