#!/bin/sh
set -e

if [ -n "$MEMCACHED_USERNAME" ]; then
  echo "$MEMCACHED_USERNAME:$MEMCACHED_PASSWORD" >/tmp/memcached_auth
  set -- "$@" "--auth-file=/tmp/memcached_auth"
fi

exec "$@"
