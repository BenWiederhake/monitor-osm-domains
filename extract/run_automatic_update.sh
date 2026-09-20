#!/bin/bash
set -ex
cd -P -- "$(dirname -- "$0")"

# FIXME
exit 42  # Not ready for actual use yet

echo "===== Step 0: Check preconditions"
if [ "MYUSERNAME" != "$USER" ]; then
    echo "Wrong user?!"
    exit 1
fi
if [ "MYMACHINE" != "$(uname -n)" ]; then
    echo "Wrong machine?!"
    exit 1
fi
if ! [ -d "/scratch/osm" ]; then
    echo "/scratch/osm doesn't exist?!"
    exit 1
fi
if ! [ -r "./build/extract" ]; then
    echo "Must build ./build/extract first"
    exit 1
fi
DATESTAMP="$(date +%Y%m%d)"
FILENAME_PBF="/scratch/osm/germany-latest_${DATESTAMP}.osm.pbf"
FILENAME_RAWJSON="/scratch/osm/raw_${DATESTAMP}_xy.monosmdom.json"
FILENAME_ALLJSON="/scratch/osm/all_${DATESTAMP}_xy.monosmdom.json"
FILENAME_PSQLOLD="/scratch/osm/monosm_monosmdom_pg_dump_${DATESTAMP}.psql"
# updated psql is uploaded directly to monosm psql instance, NOT A FILE!
if [ -e "${FILENAME_PBF}" ] || [ -e "${FILENAME_RAWJSON}" ] || [ -e "${FILENAME_ALLJSON}" ] || [ -e "${FILENAME_PSQLOLD}" ]; then
    echo "Some files already exist. This script can't handle partial continuation, resolve this mess manually, or remove partial files!"
    file "${FILENAME_PBF}"
    file "${FILENAME_RAWJSON}"
    file "${FILENAME_ALLJSON}"
    file "${FILENAME_PSQLOLD}"
    exit 1
fi

echo "===== Step 1: Download PBF file"
curl --proto '=https' --tlsv1.2 --fail --location -o "${FILENAME_PBF}" --max-redirs 3 'https://download.geofabrik.de/europe/germany-latest.osm.pbf'
md5sum "${FILENAME_PBF}"
curl 'https://download.geofabrik.de/europe/germany-latest.osm.pbf.md5'
echo "TODO: Check whether the md5 matches"

echo "===== Step 2: Extract url-ish elements"
# TODO: Build the binary if not yet existing
./build/extract "${FILENAME_PBF}" "${FILENAME_RAWJSON}"

echo "===== Step 3: Clean up dataset for import"
./cleanup.py "${FILENAME_RAWJSON}" "${FILENAME_ALLJSON}"

echo "===== Step 4: 'Gracefully' stop prod crawler"
ssh monosm '/home/monosm/bin/STOP'
echo "This sleep is necessary to make sure that the crawler really definitely has stopped gracefully."
sleep 10
ssh monosm 'supervisorctl stop monosmdomcrawl'

echo "===== Step 5: Clone database state:"
ssh monosm 'pg_dump --verbose --clean --no-acl --no-owner --if-exists monosmdom | lz4 -c' \
 | pv -cN compressed \
 | lz4 -dc \
 | pv -cN decompressed \
 > "${FILENAME_PSQLOLD}"

echo "===== Step 6: Apply database state locally"
dropdb monosmdom_mirror
createdb monosmdom_mirror
psql --set ON_ERROR_STOP=on monosmdom_mirror < "${FILENAME_PSQLOLD}"

echo "===== Step 7: Run import command locally"
(
    # We start in ~/projects/monitor-osm-domains/extract/
    cd ../monosmdom_server/
    . .venv/bin/activate
    curl https://publicsuffix.org/list/public_suffix_list.dat > storage/data/public_suffix_list.dat
    ./manage.py update_osm_state "${FILENAME_ALLJSON}" --force OVERWRITE
)

echo "===== Step 8: Drop old, irrelevant headers"
psql --set ON_ERROR_STOP=on monosmdom_mirror -c "UPDATE crawl_resultsuccess as crs set headers = NULL FROM crawl_result as cr WHERE cr.id = crs.result_id and NOW() - cr.crawl_end > '1 month';"

echo "===== Step 9: Re-export database and replace prod database"
# Exporter is too new for the importer; skip the unsupported setting:
pg_dump --verbose --clean --no-acl --no-owner --if-exists monosmdom_mirror \
 | grep -Pv '^SET transaction_timeout ' \
 | pv -cN decompressed \
 | lz4 -c \
 | pv -cN compressed \
 | ssh monosm 'dropdb monosmdom && createdb monosmdom && lz4 -dc | psql --set ON_ERROR_STOP=on monosmdom'

echo "===== Step 10: Restart prod crawler"
ssh monosm 'rm -f /tmp/STOP_OSMMONDOM && supervisorctl start monosmdomcrawl'
