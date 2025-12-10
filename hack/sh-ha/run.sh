#!/usr/bin/env bash
export COMPOSE_PROJECT_NAME=s1

BASE=$(dirname "$(dirname "$0")")
echo "BASE=$BASE"

# shellcheck source=/dev/null
source "$BASE/util"

SDIR="$BASE/sh-ha"

export compose=$SDIR/compose.yml

# dcf up -d src-cfg0 src-rs00 src-rs10 tgt-cfg0 tgt-rs00 tgt-rs10
dcf up -d src-cfg0 src-cfg1 src-cfg2 src-rs00 src-rs01 src-rs02 src-rs10 src-rs11 src-rs12

mwait "src-cfg0:27000"
mwait "src-cfg1:27001"
mwait "src-cfg2:27002"
rsinit "src/cfg" "src-cfg0:27000"
mwait "src-rs00:30000"
mwait "src-rs01:30001"
mwait "src-rs02:30002"
rsinit "src/rs0" "src-rs00:30000"
mwait "src-rs10:30100"
mwait "src-rs11:30101"
mwait "src-rs12:30102"
rsinit "src/rs1" "src-rs10:30100"

dcf up -d src-mongos && mwait "src-mongos:27017"
msh "src-mongos:27017" --eval "
    sh.addShard('rs0/src-rs00:30000'); //
    sh.addShard('rs0/src-rs01:30001'); //
    sh.addShard('rs0/src-rs02:30002'); //
    sh.addShard('rs1/src-rs10:30100'); //
    sh.addShard('rs1/src-rs11:30101'); //
    sh.addShard('rs1/src-rs12:30102');
"

dcf up -d tgt-cfg0 tgt-cfg1 tgt-cfg2 tgt-rs00 tgt-rs01 tgt-rs02 tgt-rs10 tgt-rs11 tgt-rs12

mwait "tgt-cfg0:28000"
mwait "tgt-cfg1:28001"
mwait "tgt-cfg2:28002"
rsinit "tgt/cfg" "tgt-cfg0:28000"
mwait "tgt-rs00:40000"
mwait "tgt-rs01:40001"
mwait "tgt-rs02:40002"
rsinit "tgt/rs0" "tgt-rs00:40000"
mwait "tgt-rs10:40100"
mwait "tgt-rs11:40101"
mwait "tgt-rs12:40102"
rsinit "tgt/rs1" "tgt-rs10:40100"

dcf up -d tgt-mongos && mwait "tgt-mongos:27017"
msh "tgt-mongos:27017" --eval "
    sh.addShard('rs0/tgt-rs00:40000'); //
    sh.addShard('rs0/tgt-rs01:40001'); //
    sh.addShard('rs0/tgt-rs02:40002'); //
    sh.addShard('rs1/tgt-rs10:40100'); //
    sh.addShard('rs1/tgt-rs11:40101'); //
    sh.addShard('rs1/tgt-rs12:40102');
"
