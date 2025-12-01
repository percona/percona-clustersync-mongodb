#!/usr/bin/env bash

BASE=$(dirname "$(dirname "$0")")

# shellcheck source=/dev/null
source "$BASE/util"

RDIR="$BASE/rs"

export compose="$RDIR/compose.yml"

dcf up -d rs00 rs01 rs02 rs10 rs11 rs12

mwait "rs00:30000"
mwait "rs01:30001"
mwait "rs02:30002"

rsinit "scripts/rs0" "rs00:30000"

mwait "rs10:30100"
mwait "rs11:30101"
mwait "rs12:30102"

rsinit "scripts/rs1" "rs10:30100"
