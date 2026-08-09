#!/usr/bin/env bash

set -euo pipefail

result_file="${1:-}"
expected_count="${2:-}"

if [[ -z "$result_file" || ! "$expected_count" =~ ^[1-9][0-9]*$ ]]; then
  echo "Usage: validate-trx.sh <result.trx> <expected-positive-count>" >&2
  exit 2
fi

if [[ ! -f "$result_file" ]]; then
  echo "Test result '$result_file' was not produced." >&2
  exit 1
fi

if grep --quiet 'outcome="NotExecuted"' "$result_file"; then
  echo "Test result '$result_file' contains skipped tests." >&2
  exit 1
fi

mapfile -t counter_lines < <(grep '<Counters ' "$result_file" || true)
if [[ "${#counter_lines[@]}" -ne 1 ]]; then
  echo "Expected one Counters element in '$result_file', found ${#counter_lines[@]}." >&2
  exit 1
fi

counter_line="${counter_lines[0]}"

read_counter()
{
  local attribute="$1"
  local value
  value="$(sed -n "s/.*[[:space:]]${attribute}=\"\([0-9][0-9]*\)\".*/\1/p" <<< "$counter_line")"
  if [[ -z "$value" ]]; then
    echo "Counters element in '$result_file' has no '$attribute' attribute." >&2
    exit 1
  fi

  printf '%s' "$value"
}

assert_counter()
{
  local attribute="$1"
  local expected="$2"
  local actual
  actual="$(read_counter "$attribute")"
  if [[ "$actual" -ne "$expected" ]]; then
    echo "Expected $attribute=$expected in '$result_file', found $actual." >&2
    exit 1
  fi
}

assert_counter total "$expected_count"
assert_counter executed "$expected_count"
assert_counter passed "$expected_count"
assert_counter failed 0
assert_counter notExecuted 0

result_count="$(grep -c '<UnitTestResult ' "$result_file" || true)"
passed_count="$(grep '<UnitTestResult ' "$result_file" | grep -c 'outcome="Passed"' || true)"
if [[ "$result_count" -ne "$expected_count" || "$passed_count" -ne "$expected_count" ]]; then
  echo "Expected $expected_count passed UnitTestResult elements in '$result_file'; found $passed_count passed of $result_count total." >&2
  exit 1
fi

echo "Validated $expected_count passed tests in '$result_file'."
