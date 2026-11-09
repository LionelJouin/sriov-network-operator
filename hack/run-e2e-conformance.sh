#!/bin/bash

here="$(dirname "$(readlink --canonicalize "${BASH_SOURCE[0]}")")"
root="$(readlink --canonicalize "$here/..")"

GOPATH="${GOPATH:-$HOME/go}"
JUNIT_OUTPUT="${JUNIT_OUTPUT:-/tmp/artifacts}"
export PATH=$PATH:$GOPATH/bin

# GINKGO_ARGS holds extra ginkgo CLI flags, e.g. GINKGO_ARGS='--focus=DRA --fail-fast'.
# It is split on whitespace, so an argument cannot itself contain spaces: write a focus
# regex as --focus='\[sriov\].DRA' rather than --focus='[sriov] DRA'.
read -r -a ginkgo_args <<< "${GINKGO_ARGS:-}"

${root}/bin/ginkgo --timeout=3h -output-dir=$JUNIT_OUTPUT --junit-report "unit_report.xml" -v "${ginkgo_args[@]}" "$SUITE" -- -report=$JUNIT_OUTPUT
