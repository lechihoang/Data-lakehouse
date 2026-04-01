#!/bin/bash
set -e

SCHEMA_REGISTRY="${SCHEMA_REGISTRY_URL:-http://localhost:8081}"
SCHEMAS_DIR="/opt/schemas"

echo "Waiting for Schema Registry..."
while ! curl -sf "${SCHEMA_REGISTRY}/subjects" >/dev/null 2>&1; do
    echo "Schema Registry not ready, waiting..."
    sleep 5
done

register_schema() {
    local schema_file="$1"
    local subject="$2"

    echo "Registering $schema_file as $subject..."
    schema_content=$(cat "$schema_file" | jq -c . | jq -R .)
    payload=$(jq -n --argjson schema "$schema_content" '{"schema": $schema}')
    curl -s -X POST \
        -H "Content-Type: application/vnd.schemaregistry.v1+json" \
        --data "$payload" \
        "${SCHEMA_REGISTRY}/subjects/${subject}/versions" >/dev/null
    echo "  OK: $subject"
}

for table in users orders order_items events products dist_centers; do
    file_name=$(echo "$table" | tr '_' '-')
    register_schema "${SCHEMAS_DIR}/thelook-${file_name}-value.avsc" "thelook.public.${table}-value"
done

echo "All schemas registered."
