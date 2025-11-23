#!/usr/bin/env bash
export COMPOSE_PROJECT_NAME=s1

BASE=$(dirname "$(dirname "$0")")
echo "BASE=$BASE"

# shellcheck source=/dev/null
source "$BASE/util"

SDIR="$BASE/sh"

chmod 400 "$SDIR"/mongo/keyFile

export compose=$SDIR/compose.yml

# dcf up -d src-cfg0 src-rs00 src-rs10 tgt-cfg0 tgt-rs00 tgt-rs10
dcf up -d src-cfg0 src-rs00 src-rs10

mwait "src-cfg0:27000" && rsinit "src/cfg" "src-cfg0:27000"
mwait "src-rs00:30000" && rsinit "src/rs0" "src-rs00:30000"
mwait "src-rs10:30100" && rsinit "src/rs1" "src-rs10:30100"

dcf up -d src-mongos && mwait "src-mongos:27017"
msh "adm:pass@src-mongos:27017" --eval "
    sh.addShard('rs0/src-rs00:30000'); //
    sh.addShard('rs1/src-rs10:30100');
"
msh "adm:pass@src-mongos:27017" --eval "db.adminCommand('transitionFromDedicatedConfigServer');"

dcf up -d tgt-cfg0 tgt-rs00 tgt-rs10

mwait "tgt-cfg0:28000" && rsinit "tgt/cfg" "tgt-cfg0:28000"
mwait "tgt-rs00:40000" && rsinit "tgt/rs0" "tgt-rs00:40000"
mwait "tgt-rs10:40100" && rsinit "tgt/rs1" "tgt-rs10:40100"

dcf up -d tgt-mongos && mwait "tgt-mongos:27017"
msh "adm:pass@tgt-mongos:27017" --eval "
    sh.addShard('rs0/tgt-rs00:40000'); //
    sh.addShard('rs1/tgt-rs10:40100');
"
msh "adm:pass@tgt-mongos:27017" --eval "db.adminCommand('transitionFromDedicatedConfigServer');"
