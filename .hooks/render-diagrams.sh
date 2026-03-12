#!/usr/bin/env bash
# Pre-commit hook: render changed .puml files in doc/ to .png via the PlantUML service.
set -euo pipefail

SERVICE_URL="https://plantuml-aq5hycapva-nw.a.run.app"

TOKEN=$(gcloud auth print-identity-token 2>/dev/null) || {
    echo "render-diagrams: ERROR: could not obtain GCP identity token (is gcloud authenticated?)" >&2
    exit 1
}

exit_code=0

for puml_file in "$@"; do
    # Only process files that actually exist (not deletions)
    if [[ ! -f "$puml_file" ]]; then
        continue
    fi

    dir=$(dirname "$puml_file")
    stem=$(basename "$puml_file" .puml)
    png_file="${dir}/${stem}.png"

    echo "render-diagrams: rendering ${puml_file} -> ${png_file}"

    # Build curl args: main file first
    curl_args=(-s -S -f -o "$png_file"
        -H "Authorization: Bearer ${TOKEN}"
        -F "file=@${puml_file}"
    )

    # Scan for !include directives and attach each dependency
    while IFS= read -r line; do
        if [[ "$line" =~ ^[[:space:]]*\!include[[:space:]]+([^[:space:]]+) ]]; then
            include_name="${BASH_REMATCH[1]}"
            include_path="${dir}/${include_name}"
            if [[ -f "$include_path" ]]; then
                curl_args+=(-F "include=@${include_path}")
            else
                echo "render-diagrams: WARNING: include not found: ${include_path}" >&2
            fi
        fi
    done < "$puml_file"

    if curl "${curl_args[@]}" "${SERVICE_URL}"; then
        git add "$png_file"
        echo "render-diagrams: staged ${png_file}"
    else
        echo "render-diagrams: ERROR: failed to render ${puml_file}" >&2
        exit_code=1
    fi
done

exit $exit_code
