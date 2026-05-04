#!/bin/bash
set -euo pipefail

while true; do
    pixi run snakemake -c 1 -F concatenate_metadata

    read -rp $'\nEdit config.yaml and rerun? [yes/no] ' answer
    [[ "$answer" != "yes" ]] && break
done

while true; do
    pixi run snakemake -c 1 -F spellcheck_metadata

    read -rp $'\nEdit config.yaml and rerun? [yes/no] ' answer
    [[ "$answer" != "yes" ]] && break
done

while true; do
    pixi run snakemake -c 1 -F get_hierarchy

    read -rp $'\nEdit config.yaml and rerun? [yes/no] ' answer
    [[ "$answer" != "yes" ]] && break
done

geo_length=$(awk '/^geographical_hierarchy:/{f=1;next} f && /^  -/{n++} f && !/^  -/{exit} END{print n+0}' config/config.yaml)

for i in $(seq 0 $((geo_length - 2))); do
    while true; do
        pixi run snakemake -c 1 -F spellcheck_geography --config columns_to_delete=$i

        read -rp $'\nEdit config.yaml and rerun? [yes/no] ' answer
        [[ "$answer" != "yes" ]] && break
    done
done