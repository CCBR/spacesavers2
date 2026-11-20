#!/usr/bin/env bash
set -euo pipefail

root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
export PATH="$root/bin:$PATH"

expected=$(spacesavers2 help)
for option in '' -h --help; do
    if [[ -z "$option" ]]; then
        actual=$(spacesavers2)
    else
        actual=$(spacesavers2 "$option")
    fi
    [[ "$actual" == "$expected" ]]
done

for command in catalog mimeo grubbers usurp e2e pdq pdq_create_db pdq_update_db; do
    grep -Fq "spacesavers2_$command " <<< "$expected"
    [[ -x "$root/bin/spacesavers2_$command" ]]
done

if output=$(spacesavers2 invalid 2>&1); then
    echo "Expected an error for an unknown argument" >&2
    exit 1
fi
grep -Fq 'unknown argument: invalid' <<< "$output"

if output=$(spacesavers2 help extra 2>&1); then
    echo "Expected an error for extra arguments" >&2
    exit 1
fi
grep -Fq 'too many arguments' <<< "$output"
