#!/usr/bin/env bash

git clone https://github.com/apache/arrow.git $TRAVIS_BUILD_DIR/arrow-src

export CPP_DIR=$TRAVIS_BUILD_DIR/arrow-src/cpp

mkdir $ARROW_HOME
pushd $ARROW_HOME

# Build an isolated thirdparty
cp -r $CPP_DIR/thirdparty .
cp $CPP_DIR/setup_build_env.sh .

source setup_build_env.sh

CMAKE_COMMON_FLAGS="\
-DCMAKE_INSTALL_PREFIX=$ARROW_HOME"

cmake -DARROW_TEST_MEMCHECK=on \
      $CMAKE_COMMON_FLAGS \
      -DCMAKE_CXX_FLAGS="-Werror" \
      $CPP_DIR

make -j4
make install

popd $ARROW_HOME
