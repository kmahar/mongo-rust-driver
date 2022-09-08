#!/bin/bash

set -o errexit
set -o pipefail
set -o xtrace

source ./.evergreen/env.sh

OPTIONS="-- -Z unstable-options --format json --report-time"

if [ "$SINGLE_THREAD" = true ]; then
	OPTIONS="$OPTIONS --test-threads=1"
fi

FEATURE_FLAGS="tracing-unstable"
DEFAULT_FEATURES=""

if [ "$ASYNC_RUNTIME" = "async-std" ]; then
    FEATURE_FLAGS="${FEATURE_FLAGS},async-std-runtime"
    DEFAULT_FEATURES="--no-default-features"
elif [ "$ASYNC_RUNTIME" != "tokio" ]; then
    echo "invalid async runtime: ${ASYNC_RUNTIME}" >&2
    exit 1
fi

echo "cargo test options: ${DEFAULT_FEATURES} --features ${FEATURE_FLAGS} ${OPTIONS}"

set +o errexit

RUST_BACKTRACE=1 cargo test $DEFAULT_FEATURES --features $FEATURE_FLAGS $OPTIONS test::spec::trace | tee results.json
CARGO_RESULT = $?
cat results.json | cargo2junit > results.xml

exit $CARGO_RESULT
