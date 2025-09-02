#!/usr/bin/env bash
#
# SPDX-License-Identifier: Apache-2.0
#
# The Density Contributors require contributions made to
# this file be licensed under the Apache-2.0 license or a
# compatible open source license.
#
# Modifications Copyright Density Contributors. See
# GitHub history for details.
#

set -e -o pipefail

addprinc.sh "density"
#TODO(Density): fix username
addprinc.sh "hdfs/hdfs.build.density.org"

# Use this as a signal that setup is complete
python3 -m http.server 4444 &

sleep infinity
