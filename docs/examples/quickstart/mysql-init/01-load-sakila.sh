#!/bin/sh
# Download and load the sakila sample database into MySQL on first
# container start. Runs once per fresh volume.
#
# The MySQL Docker image's docker-entrypoint.sh executes files in
# /docker-entrypoint-initdb.d/ in alphabetical order *after* the
# server is started and the configured database (sakila) has been
# created. The `mysql` client is on PATH inside the container.
#
# Sakila is published by Oracle under a permissive license; the
# tarball at downloads.mysql.com is the canonical source.

set -e

cd /tmp

echo "==> Downloading sakila sample database..."
curl -sSL \
    https://downloads.mysql.com/docs/sakila-db.tar.gz \
    -o sakila.tar.gz

echo "==> Extracting sakila..."
tar xzf sakila.tar.gz

echo "==> Loading sakila schema..."
mysql -uroot -p"${MYSQL_ROOT_PASSWORD}" sakila \
    < sakila-db/sakila-schema.sql

echo "==> Loading sakila data..."
mysql -uroot -p"${MYSQL_ROOT_PASSWORD}" sakila \
    < sakila-db/sakila-data.sql

echo "==> Sakila loaded ($(mysql -uroot -p"${MYSQL_ROOT_PASSWORD}" \
    -N -e "SELECT COUNT(*) FROM sakila.film") films)"

# Cleanup downloads to keep the layer small.
rm -rf /tmp/sakila.tar.gz /tmp/sakila-db
