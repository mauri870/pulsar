#!/bin/bash

set -e

source $NVM_DIR/nvm.sh
nvm use 22
cd llrt
npm i
make js
