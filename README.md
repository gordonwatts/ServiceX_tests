# ServiceX_tests
 Some automated tests to make sure ServiceX responds

## Introduction

I needed to automate some queires to ServiceX in order to preserve my sanity as I hacked at it. This repo contains the automation.

## Configuration

You'll need the following:

1. Machine with docker and kubernetes installed on it. This was tested on desktop docker on windows.
1. One level up from this directory you'll need to have a `yaml` file that contains config information to allow access to the an ATLAS VCOMS that `rucio` can deal with.
1. `pytest` and `python` 3.7 need to be installed as well in your shell.

## Running the tests

1. Make sure to `pip install -r requirements.txt`
1. Just use the `pytest` command to run the tests. This will setup a ServiceX cluster in your current kubernetes cluster, and run tests against it. It will leave the cluster running when the tests are done.

## Caveats

1. The tests that use the default public file are slow - they take 6 minutes, and there is no caching. And that is at the end of a fast home connection.
1. There is currently a bug in the x509 cert which means that you can't run the server standalone - so a file with credentials (even though never used) has to be provided. The test scripts default its location to one directory up from this package, called `servicex-desktop-local.yaml`. See the `ServiceX` repo for a description of what is needed in it.