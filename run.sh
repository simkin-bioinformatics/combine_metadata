#!/bin/bash
set -euo pipefail

while true; do
    pixi run snakemake -c 1 concatenate_metadata --delete-all-output
    pixi run snakemake -c 1 concatenate_metadata

    read -rp $'\nEdit config.yaml and rerun? [yes/no] ' answer
    [[ "$answer" != "yes" ]] && break

    pixi run micro "config/config.yaml"
done

while true; do
    pixi run snakemake -c 1 spellcheck_metadata --delete-all-output
    pixi run snakemake -c 1 spellcheck_metadata

    read -rp $'\nEdit config.yaml and rerun? [yes/no] ' answer
    [[ "$answer" != "yes" ]] && break

    pixi run micro "config/config.yaml"
done

while true; do
    pixi run snakemake -c 1 get_hierarchy --delete-all-output
    pixi run snakemake -c 1 get_hierarchy

    read -rp $'\nEdit config.yaml and rerun? [yes/no] ' answer
    [[ "$answer" != "yes" ]] && break

    pixi run micro "config/config.yaml"
done

geo_length=$(awk '/^geographical_hierarchy:/{f=1;next} f && /^  -/{n++} f && !/^  -/{exit} END{print n+0}' config/config.yaml)

for i in $(seq 0 $((geo_length - 2))); do
    while true; do
        pixi run snakemake -c 1 spellcheck_geography --config columns_to_delete=$i --delete-all-output
        pixi run snakemake -c 1 spellcheck_geography --config columns_to_delete=$i

        read -rp $'\nEdit config.yaml and rerun? [yes/no] ' answer
        [[ "$answer" != "yes" ]] && break

        pixi run micro "config/config.yaml"
    done
done