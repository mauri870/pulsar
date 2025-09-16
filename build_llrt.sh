#!/bin/bash

set -e

nvm use 22
cd llrt
npm i
make js
