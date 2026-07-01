#!/usr/bin/env bash
# Issue EXACTLY N DDL statements against the qualitydemo.trips table, for the
# Layer 2 Phase 1 acceptance test. The sequence (ADD, then N-2 MODIFYs, then DROP
# of one scratch column) is exactly N DDLs and leaves the table schema RESTORED,
# so the test is repeatable. Each ALTER is one binlog DDL event; with the source's
# CDDLAction=Ignore they land in the rollup's "Ignored DDL Count", which the agent
# sums into the schema_evolution signal.
#
# Usage:  striim/mysql/run_ddl_acceptance.sh [N]   (default N=3)
# Then:   .venv/bin/python striim/pipeline/check_ddl_acceptance.py --expected N
set -euo pipefail

N="${1:-3}"
CONTAINER="${MYSQL_CONTAINER:-striim-mysql}"
DB="${MYSQL_DB:-qualitydemo}"
COL="ddl_probe"

if [ "$N" -lt 2 ]; then
  echo "N must be >= 2 (need at least ADD + DROP to leave the table restored)"; exit 2
fi

# Build exactly N ALTERs: 1 ADD + (N-2) MODIFY + 1 DROP.
SQL="USE ${DB};"
SQL="${SQL} ALTER TABLE trips ADD COLUMN ${COL} INT NULL;"
for i in $(seq 1 $((N - 2))); do
  if [ $((i % 2)) -eq 1 ]; then T="BIGINT"; else T="INT"; fi
  SQL="${SQL} ALTER TABLE trips MODIFY COLUMN ${COL} ${T} NULL;"
done
SQL="${SQL} ALTER TABLE trips DROP COLUMN ${COL};"

echo "issuing exactly ${N} DDL statements against ${DB}.trips (table restored after)..."
docker exec "${CONTAINER}" mysql -uroot -prootpw -e "${SQL}"
echo "done: ${N} DDLs issued."
echo "wait ~30-60s for an agent tick, then:"
echo "  .venv/bin/python striim/pipeline/check_ddl_acceptance.py --expected ${N}"
