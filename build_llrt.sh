#!/bin/bash

set -e

source $NVM_DIR/nvm.sh
nvm use 25
cd llrt
npm i
make js
