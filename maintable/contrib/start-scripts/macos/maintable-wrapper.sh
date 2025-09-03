#!/bin/sh

# maintableQL server start script (launched by org.maintableql.maintable.plist)

# edit these as needed:

# directory containing maintable executable:
PGBINDIR="/usr/local/pgsql/bin"
# data directory:
PGDATA="/usr/local/pgsql/data"
# file to receive postmaster's initial log messages:
PGLOGFILE="${PGDATA}/pgstart.log"

# (it's recommendable to enable the Maintable logging_collector feature
# so that PGLOGFILE doesn't grow without bound)


# set umask to ensure PGLOGFILE is not created world-readable
umask 077

# wait for networking to be up (else server may not bind to desired ports)
/usr/sbin/ipconfig waitall

# and launch the server
exec "$PGBINDIR"/maintable -D "$PGDATA" >>"$PGLOGFILE" 2>&1
