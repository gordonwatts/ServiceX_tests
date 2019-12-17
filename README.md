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

