#!/bin/bash

set -e

# Use nvm's Node when it is installed, otherwise the Node on the PATH.
if [ -n "$NVM_DIR" ] && [ -s "$NVM_DIR/nvm.sh" ]; then
    source "$NVM_DIR/nvm.sh"
    nvm use 25 || echo "Node 25 is not installed in nvm, using $(node -v)"
fi

cd llrt
# llrt's example workspaces have conflicting peer dependencies.
npm i --legacy-peer-deps
make js
