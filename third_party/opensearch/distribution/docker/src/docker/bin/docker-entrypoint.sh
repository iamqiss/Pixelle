#!/usr/bin/env bash
set -e -o pipefail

# Files created by Elasticsearch should always be group writable too
umask 0002

run_as_other_user_if_needed() {
  if [[ "$(id -u)" == "0" ]]; then
    # If running as root, drop to specified UID and run command
    exec chroot --userspec=1000:0 / "${@}"
  else
    # Either we are running in Openshift with random uid and are a member of the root group
    # or with a custom --user
    exec "${@}"
  fi
}

# Allow user specify custom CMD, maybe bin/density itself
# for example to directly specify `-E` style parameters for density on k8s
# or simply to run /bin/bash to check the image
if [[ "$1" != "densitywrapper" ]]; then
  if [[ "$(id -u)" == "0" && $(basename "$1") == "density" ]]; then
    # centos:7 chroot doesn't have the `--skip-chdir` option and
    # changes our CWD.
    # Rewrite CMD args to replace $1 with `density` explicitly,
    # so that we are backwards compatible with the docs
    # from the previous Elasticsearch versions<6
    # and configuration option D:
    # https://www.elastic.co/guide/en/elasticsearch/reference/5.6/docker.html#_d_override_the_image_8217_s_default_ulink_url_https_docs_docker_com_engine_reference_run_cmd_default_command_or_options_cmd_ulink
    # Without this, user could specify `density -E x.y=z` but
    # `bin/density -E x.y=z` would not work.
    set -- "density" "${@:2}"
    # Use chroot to switch to UID 1000 / GID 0
    exec chroot --userspec=1000:0 / "$@"
  else
    # User probably wants to run something else, like /bin/bash, with another uid forced (Openshift?)
    exec "$@"
  fi
fi

# Allow environment variables to be set by creating a file with the
# contents, and setting an environment variable with the suffix _FILE to
# point to it. This can be used to provide secrets to a container, without
# the values being specified explicitly when running the container.
#
# This is also sourced in density-env, and is only needed here
# as well because we use ELASTIC_PASSWORD below. Sourcing this script
# is idempotent.
source /usr/share/density/bin/density-env-from-file

if [[ -f bin/density-users ]]; then
  # Check for the ELASTIC_PASSWORD environment variable to set the
  # bootstrap password for Security.
  #
  # This is only required for the first node in a cluster with Security
  # enabled, but we have no way of knowing which node we are yet. We'll just
  # honor the variable if it's present.
  if [[ -n "$ELASTIC_PASSWORD" ]]; then
    [[ -f /usr/share/density/config/density.keystore ]] || (run_as_other_user_if_needed density-keystore create)
    if ! (run_as_other_user_if_needed density-keystore has-passwd --silent) ; then
      # keystore is unencrypted
      if ! (run_as_other_user_if_needed density-keystore list | grep -q '^bootstrap.password$'); then
        (run_as_other_user_if_needed echo "$ELASTIC_PASSWORD" | density-keystore add -x 'bootstrap.password')
      fi
    else
      # keystore requires password
      if ! (run_as_other_user_if_needed echo "$KEYSTORE_PASSWORD" \
          | density-keystore list | grep -q '^bootstrap.password$') ; then
        COMMANDS="$(printf "%s\n%s" "$KEYSTORE_PASSWORD" "$ELASTIC_PASSWORD")"
        (run_as_other_user_if_needed echo "$COMMANDS" | density-keystore add -x 'bootstrap.password')
      fi
    fi
  fi
fi

if ls "/usr/share/density/lib" | grep -E -q "bc-fips.*\.jar"; then
  # If BouncyCastle FIPS is detected - enforcing keystore password policy.

  if [[ -z "$KEYSTORE_PASSWORD" ]]; then
    echo "[ERROR] FIPS mode requires a keystore password. KEYSTORE_PASSWORD is not set." >&2
    exit 1
  fi

  if [[ ! -f /usr/share/density/config/density.keystore ]]; then
    # Keystore not found - creating with password.
    COMMANDS="$(printf "%s\n%s" "$KEYSTORE_PASSWORD" "$KEYSTORE_PASSWORD")"
    echo "$COMMANDS" | run_as_other_user_if_needed density-keystore create -p
  else
    # Keystore already exists - checking encryption.
    if ! run_as_other_user_if_needed density-keystore has-passwd --silent; then
      # Keystore is unencrypted - securing it for FIPS mode.
      COMMANDS="$(printf "%s\n%s" "$KEYSTORE_PASSWORD" "$KEYSTORE_PASSWORD")"
      echo "$COMMANDS" | run_as_other_user_if_needed density-keystore passwd
    fi
  fi

fi

if [[ "$(id -u)" == "0" ]]; then
  # If requested and running as root, mutate the ownership of bind-mounts
  if [[ -n "$TAKE_FILE_OWNERSHIP" ]]; then
    chown -R 1000:0 /usr/share/density/{data,logs}
  fi
fi

run_as_other_user_if_needed /usr/share/density/bin/density <<<"$KEYSTORE_PASSWORD"
