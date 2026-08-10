#!/usr/bin/env bash
set -euo pipefail

usage() {
  echo "usage: $0 bdn <artifacts-directory> <list-output> <expected-commit>" >&2
  echo "       $0 load <results-directory> <searchable|plain> <expected-commit> [embedded|external] [memory|postgresql|redis|azure-blob] [expected-run-id] [expected-scenario-sha256] [expected-silo-count]" >&2
  echo "       $0 secrets <artifacts-directory>" >&2
  exit 64
}

require_file() {
  if [[ ! -s "$1" ]]; then
    echo "required benchmark artifact is missing or empty: $1" >&2
    exit 1
  fi
}

run_result_contract_validator() {
  local result=$1
  local script_directory
  local repository_root
  local validator_assembly
  local dotnet_host
  script_directory=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
  repository_root=$(cd -- "$script_directory/.." && pwd)
  validator_assembly=${OSS_BENCHMARK_ARTIFACT_VALIDATOR_ASSEMBLY:-}
  if [[ -z "$validator_assembly" &&
        -s "$script_directory/../Orleans.SearchableStorage.LoadDriver.dll" ]]; then
    validator_assembly="$script_directory/../Orleans.SearchableStorage.LoadDriver.dll"
  fi
  if [[ -z "$validator_assembly" ]]; then
    validator_assembly="$repository_root/benchmarks/Orleans.SearchableStorage.LoadDriver/bin/Release/net10.0/Orleans.SearchableStorage.LoadDriver.dll"
  fi
  require_file "$validator_assembly"
  dotnet_host=${DOTNET_HOST_PATH:-}
  if [[ -z "$dotnet_host" && -n "${DOTNET_ROOT:-}" && -x "$DOTNET_ROOT/dotnet" ]]; then
    dotnet_host="$DOTNET_ROOT/dotnet"
  fi
  if [[ -z "$dotnet_host" ]]; then
    dotnet_host=$(command -v dotnet || true)
  fi
  if [[ -z "$dotnet_host" || ! -x "$dotnet_host" ]]; then
    echo "dotnet host is required for strict benchmark artifact validation" >&2
    exit 1
  fi
  "$dotnet_host" "$validator_assembly" validate-artifact --result "$result"
}

verify_base64_sha256() {
  local encoded=$1
  local expected_sha=$2
  local destination=$3
  local label=$4

  if ! printf '%s' "$encoded" | base64 --decode > "$destination"; then
    echo "$label is not valid Base64" >&2
    exit 1
  fi
  require_file "$destination"
  if ! jq --exit-status . "$destination" >/dev/null; then
    echo "$label does not decode to JSON" >&2
    exit 1
  fi

  local actual_sha
  actual_sha=$(sha256sum "$destination" | cut -d ' ' -f 1)
  if [[ "$actual_sha" != "$expected_sha" ]]; then
    echo "$label checksum mismatch" >&2
    exit 1
  fi
}

reject_secret_canaries() {
  local artifacts_directory=$1
  local secret_value=${OSS_BENCHMARK_STORAGE_CONNECTION_STRING:-}

  if [[ ! -e "$artifacts_directory" && ! -L "$artifacts_directory" ]]; then
    return
  fi

  if [[ ! -d "$artifacts_directory" ]] ||
     [[ -L "$artifacts_directory" ]] ||
     find "$artifacts_directory" -type l -print -quit | grep --quiet .; then
    echo "benchmark artifact roots must be directories and must not contain symbolic links; upload is forbidden" >&2
    exit 1
  fi
  if find "$artifacts_directory" -mindepth 1 ! -type f ! -type d -print -quit | grep --quiet .; then
    echo "benchmark artifacts must contain only regular files and directories; upload is forbidden" >&2
    exit 1
  fi

  if [[ -n "$secret_value" ]]; then
    local grep_status=0
    LC_ALL=C grep \
      --recursive \
      --binary-files=text \
      --fixed-strings \
      --quiet \
      -- "$secret_value" \
      "$artifacts_directory" || grep_status=$?
    if (( grep_status == 0 )); then
      echo "benchmark artifacts contain the selected backend secret; upload is forbidden" >&2
      exit 1
    fi
    if (( grep_status != 1 )); then
      echo "benchmark artifact secret scan failed" >&2
      exit 1
    fi
  fi

  # Exact-value scanning is the primary canary. These deliberately narrow shapes
  # catch derived credentials emitted by a provider or runner hook without treating
  # metadata names (for example connectionStringEnvironment) as secrets.
  local credential_pattern
  credential_pattern='(?i)\b(?:password|pwd|accountkey|sharedaccesssignature|sas|sig|token|access_token|secret)[[:space:]]*=[[:space:]]*(?!\[REDACTED\])(?:"(?:\\.|[^"\r\n])*"|'"'"'(?:\\.|[^'"'"'\r\n])*'"'"'|\{[^}\r\n]*\}|[^;&\r\n[:space:]][^;&\r\n]*)|"(?:password|pwd|accountkey|sharedaccesssignature|sas|sig|token|access_token|secret)"[[:space:]]*:[[:space:]]*"(?!\[REDACTED\])(?:\\.|[^"\\])*"|\bauthorization\b["'"'"']?[[:space:]]*[:=][[:space:]]*["'"'"']?[[:space:]]*bearer[[:space:]]+(?!\[REDACTED\])[^"'"'"',;}[:space:]]+|[a-z][a-z0-9+.-]*://(?!\[REDACTED\]@)[^/@[:space:]]+@|[?&](?:sig|token|access_token|password|pwd|secret)=(?!\[REDACTED\])[^&[:space:]]+'

  local artifact_path relative_path
  while IFS= read -r -d '' artifact_path; do
    relative_path=${artifact_path#"${artifacts_directory%/}/"}
    if [[ -n "$secret_value" && "$relative_path" == *"$secret_value"* ]]; then
      echo "benchmark artifact path contains the selected backend secret; upload is forbidden" >&2
      exit 1
    fi
    if LC_ALL=C grep --perl-regexp --quiet -- "$credential_pattern" <<<"$relative_path"; then
      echo "benchmark artifact path contains an unredacted credential-shaped value; upload is forbidden" >&2
      exit 1
    fi
  done < <(find "$artifacts_directory" -mindepth 1 -print0)

  local pattern_status=0
  LC_ALL=C grep \
    --recursive \
    --binary-files=without-match \
    --perl-regexp \
    --quiet \
    -- "$credential_pattern" \
    "$artifacts_directory" || pattern_status=$?
  if (( pattern_status == 0 )); then
    echo "benchmark artifacts contain an unredacted credential-shaped value; upload is forbidden" >&2
    exit 1
  fi
  if (( pattern_status != 1 )); then
    echo "benchmark artifact credential-pattern scan failed" >&2
    exit 1
  fi
}

if [[ $# -lt 1 ]]; then
  usage
fi

case "$1" in
  bdn)
    if [[ $# -ne 4 ]]; then
      usage
    fi

    artifacts_directory=$2
    list_output=$3
    expected_commit=$4
    provenance="$artifacts_directory/provenance.json"
    require_file "$provenance"
    require_file "$list_output"

    jq --exit-status --arg commit "$expected_commit" '
      .SchemaVersion == "oss-benchmarkdotnet-provenance/v1"
      and .ExecutionMode == "BenchmarkDotNet"
      and .GitCommit == $commit
      and .GitDirty == false
      and (.BenchmarkAssemblyVersion | length > 0 and . != "unknown")
      and (.BenchmarkDotNetVersion | length > 0 and . != "unknown")
      and (.SearchableStorageVersion | length > 0 and . != "unknown")
      and (.FrameworkDescription | length > 0)
      and (.OsDescription | length > 0)
      and (.ProcessArchitecture | length > 0)
      and .ProcessorCount > 0
      and .JobIdentity == "net10-server;serverGC=true;concurrentGC=true"
    ' "$provenance" >/dev/null

    mapfile -t listed_benchmarks < <(
      grep --extended-regexp '^Orleans\.SearchableStorage\.Benchmarks\.[A-Za-z0-9_+]+\.[A-Za-z0-9_]+$' "$list_output" || true
    )
    if [[ ${#listed_benchmarks[@]} -ne 16 ]]; then
      echo "BenchmarkDotNet must list exactly 16 benchmark methods; found ${#listed_benchmarks[@]}." >&2
      exit 1
    fi

    mapfile -t actual_benchmarks < <(printf '%s\n' "${listed_benchmarks[@]}" | LC_ALL=C sort)
    mapfile -t expected_benchmarks <<'EOF'
Orleans.SearchableStorage.Benchmarks.DerivedIndexBuildBenchmarks.BuildDerivedIndexes
Orleans.SearchableStorage.Benchmarks.ExactRangeLookupBenchmarks.ExactRangeValueLookup
Orleans.SearchableStorage.Benchmarks.IndexMutationBenchmarks.DeleteAndRestoreIndexedRecord
Orleans.SearchableStorage.Benchmarks.IndexMutationBenchmarks.ReplaceIndexedRecord
Orleans.SearchableStorage.Benchmarks.JournalAppendBenchmarks.AppendBoundedJournalSegment
Orleans.SearchableStorage.Benchmarks.JournalReplayBenchmarks.MaterializeSnapshotAndReplay
Orleans.SearchableStorage.Benchmarks.JournalReplayBenchmarks.ReplayValidatedJournal
Orleans.SearchableStorage.Benchmarks.JournalSerializationBenchmarks.DeserializeJournalSegment
Orleans.SearchableStorage.Benchmarks.JournalSerializationBenchmarks.SerializeJournalSegment
Orleans.SearchableStorage.Benchmarks.QueryPlanConstructionBenchmarks.CreatePartitionWirePlan
Orleans.SearchableStorage.Benchmarks.QueryPlanConstructionBenchmarks.TranslateExpression
Orleans.SearchableStorage.Benchmarks.QueryPlanEvaluationBenchmarks.EvaluatePartitionPlan
Orleans.SearchableStorage.Benchmarks.QueryPlanSerializationBenchmarks.DeserializePartitionQueryPlan
Orleans.SearchableStorage.Benchmarks.QueryPlanSerializationBenchmarks.SerializePartitionQueryPlan
Orleans.SearchableStorage.Benchmarks.RangeQueryBenchmarks.BoundedRangeQuery
Orleans.SearchableStorage.Benchmarks.SnapshotConstructionBenchmarks.ConstructCompactionSnapshot
EOF
    if ! diff --unified \
      <(printf '%s\n' "${expected_benchmarks[@]}") \
      <(printf '%s\n' "${actual_benchmarks[@]}") >&2; then
      echo "BenchmarkDotNet listed a method set outside the reviewed 16-method contract." >&2
      exit 1
    fi
    ;;

  load)
    if [[ $# -lt 4 || $# -gt 9 ]]; then
      usage
    fi

    results_directory=$2
    implementation_path=$3
    expected_commit=$4
    expected_topology=${5:-embedded}
    expected_backend=${6:-memory}
    expected_run_id=${7:-}
    expected_scenario_sha256=${8:-}
    expected_silo_count=${9:-}
    if [[ "$implementation_path" != "searchable" && "$implementation_path" != "plain" ]]; then
      usage
    fi
    if [[ "$expected_topology" != "embedded" && "$expected_topology" != "external" ]]; then
      usage
    fi
    case "$expected_backend" in
      memory|postgresql|redis|azure-blob) ;;
      *) usage ;;
    esac
    if [[ -n "$expected_run_id" &&
          ( ! "$expected_run_id" =~ ^[a-z0-9]([a-z0-9-]{0,30}[a-z0-9])?$ ||
            ${#expected_run_id} -gt 32 ) ]]; then
      usage
    fi
    if [[ -n "$expected_scenario_sha256" &&
          ! "$expected_scenario_sha256" =~ ^[0-9a-f]{64}$ ]]; then
      usage
    fi
    if [[ -n "$expected_silo_count" &&
          ( ! "$expected_silo_count" =~ ^[1-9][0-9]{0,3}$ ||
            expected_silo_count -gt 4096 ) ]]; then
      usage
    fi

    reject_secret_canaries "$results_directory"

    mapfile -t result_files < <(
      find "$results_directory" -type f \( -name result.json -o -name failure.json \) -print | sort
    )
    if [[ ${#result_files[@]} -ne 1 ]]; then
      echo "expected exactly one result.json or failure.json under $results_directory; found ${#result_files[@]}" >&2
      exit 1
    fi

    result=${result_files[0]}
    require_file "$result"
    expected_hlog_count=$(jq --exit-status '.histogramArtifacts | length' "$result")
    actual_hlog_count=0
    while IFS= read -r -d '' _; do
      actual_hlog_count=$((actual_hlog_count + 1))
    done < <(find "$results_directory" -type f -name '*.hlog' -print0)
    if [[ "$actual_hlog_count" -ne "$expected_hlog_count" ]]; then
      echo "result tree contains $actual_hlog_count HLOG files but the manifest references $expected_hlog_count" >&2
      exit 1
    fi
    run_result_contract_validator "$result"
    validation_temp=$(mktemp -d)
    trap 'rm -rf -- "$validation_temp"' EXIT
    artifact_kind=success
    if [[ $(basename "$result") == failure.json ]]; then
      artifact_kind=failure
    fi
    jq --exit-status \
      --arg path "$implementation_path" \
      --arg commit "$expected_commit" \
      --arg topology "$expected_topology" \
      --arg backend "${expected_backend//-/}" \
      --arg expectedRunId "$expected_run_id" \
      --arg expectedScenarioSha256 "$expected_scenario_sha256" \
      --arg expectedSiloCount "$expected_silo_count" \
      --arg artifactKind "$artifact_kind" '
      def exactOperations:
        (.operations | keys | sort) == ["clear", "exactquery", "rangequery", "read", "upsert"];
      def closeEnough($actual; $expected):
        (($actual - $expected) | abs)
          <= (1e-9 * ([1, ($actual | abs), ($expected | abs)] | max));
      def validRate($count; $duration; $rate):
        ($duration >= 0 and $rate >= 0)
        and ($count == 0 or $duration > 0)
        and if $duration == 0 then
          $rate == 0
        else
          closeEnough($rate; ($count / $duration))
          and (if $count == 0 then $rate == 0 else $rate > 0 end)
        end;
      def operationValid($operation; $fullyDrained; $mode):
        all([
          $operation.offered,
          $operation.started,
          $operation.completed,
          $operation.succeeded,
          $operation.failed,
          $operation.timedOut,
          $operation.lateCallDrainAttempts,
          $operation.lateCallDrainIncomplete,
          $operation.dropped,
          $operation.resultCount,
          $operation.histogramClamped
        ][]; . >= 0 and floor == .)
        and $operation.lateCallDrainDurationSeconds >= 0
        and all($operation.errors[]; . >= 0 and floor == .)
        and $operation.completed == ($operation.succeeded + $operation.failed)
        and $operation.completed <= $operation.started
        and ($operation.started + $operation.dropped) <= $operation.offered
        and $operation.timedOut <= $operation.lateCallDrainAttempts
        and $operation.lateCallDrainAttempts <= $operation.failed
        and $operation.lateCallDrainIncomplete <= $operation.lateCallDrainAttempts
        and ([$operation.errors[]] | add // 0) == $operation.failed
        and (($fullyDrained | not) or $operation.started == $operation.completed)
        and (($fullyDrained | not) or $mode != "OpenLoop"
          or $operation.offered == ($operation.started + $operation.dropped));
      def operationSum($phase; $field):
        [$phase.operations[] | .[$field]] | add;
      def phaseValid($phase; $fullyDrained; $mode):
        ($phase | exactOperations)
        and all([
          $phase.offered,
          $phase.started,
          $phase.completed,
          $phase.succeeded,
          $phase.failed,
          $phase.timedOut,
          $phase.lateCallDrainAttempts,
          $phase.lateCallDrainIncomplete,
          $phase.dropped
        ][]; . >= 0 and floor == .)
        and $phase.scheduledDurationSeconds >= 0
        and $phase.wallDurationSeconds >= 0
        and $phase.lateCallDrainDurationSeconds >= 0
        and $phase.offered == operationSum($phase; "offered")
        and $phase.started == operationSum($phase; "started")
        and $phase.completed == operationSum($phase; "completed")
        and $phase.succeeded == operationSum($phase; "succeeded")
        and $phase.failed == operationSum($phase; "failed")
        and $phase.timedOut == operationSum($phase; "timedOut")
        and $phase.lateCallDrainAttempts == operationSum($phase; "lateCallDrainAttempts")
        and $phase.lateCallDrainIncomplete == operationSum($phase; "lateCallDrainIncomplete")
        and closeEnough(
          $phase.lateCallDrainDurationSeconds;
          operationSum($phase; "lateCallDrainDurationSeconds"))
        and $phase.dropped == operationSum($phase; "dropped")
        and $phase.completed == ($phase.succeeded + $phase.failed)
        and $phase.completed <= $phase.started
        and ($phase.started + $phase.dropped) <= $phase.offered
        and $phase.timedOut <= $phase.lateCallDrainAttempts
        and $phase.lateCallDrainAttempts <= $phase.failed
        and $phase.lateCallDrainIncomplete <= $phase.lateCallDrainAttempts
        and (($fullyDrained | not) or $phase.started == $phase.completed)
        and (($fullyDrained | not) or $mode != "OpenLoop"
          or $phase.offered == ($phase.started + $phase.dropped))
        and validRate($phase.offered; $phase.scheduledDurationSeconds; $phase.offeredPerSecond)
        and validRate($phase.completed; $phase.wallDurationSeconds; $phase.completedPerSecond)
        and all($phase.operations[]; operationValid(.; $fullyDrained; $mode));
      def expectedHistogramTuples:
        ["upsert", "read", "exactquery", "rangequery", "clear"]
        | map(. as $operation | [
            [$operation, "succeeded", "latency"],
            [$operation, "failed", "latency"],
            [$operation, "all", "queue-delay"]
          ])
        | add
        | sort;
      def histogramArtifactValid($root; $phase; $mode):
        . as $artifact
        | $phase.operations[$artifact.operation] as $operation
        | (if $artifact.metric == "queue-delay" then
             (if $mode == "OpenLoop" then $operation.completed else 0 end)
           elif $artifact.outcome == "succeeded" then
             $operation.succeeded
           else
             $operation.failed
           end) as $expectedCount
        | (if $artifact.metric == "queue-delay" then
             $operation.queueDelayMicroseconds
           elif $artifact.outcome == "succeeded" then
             $operation.succeededLatencyMicroseconds
           else
             $operation.failedLatencyMicroseconds
           end) as $summary
        | $artifact.format == "HdrHistogram log v1.3 (compressed V2 histogram)"
          and $artifact.unit == "microseconds"
          and $artifact.clientInstance == $root.instanceId
          and $artifact.lowestDiscernibleValue == 1
          and $artifact.highestTrackableValue == 600000000
          and $artifact.significantDigits == 3
          and $artifact.path == "\($artifact.metric)-\($artifact.operation)-\($artifact.outcome).hlog"
          and ($artifact.sha256 | test("^[0-9a-f]{64}$"))
          and $artifact.count == $expectedCount
          and (($summary == null and $expectedCount == 0)
            or ($summary != null and $summary.count == $expectedCount))
          and $artifact.samplingSemantics ==
            (if $artifact.metric == "queue-delay" then
               "scheduled-arrival to worker-start; open-loop only"
             elif $mode == "OpenLoop" then
               "scheduled-arrival to completion; includes queue delay"
             else
               "operation-start to completion; closed-loop"
             end);
      def exactHistogramSet($root; $phase; $mode):
        ($root.histogramArtifacts | length) == 15
        and ([$root.histogramArtifacts[] | [.operation, .outcome, .metric]] | sort)
          == expectedHistogramTuples
        and ([$root.histogramArtifacts[].path] | length)
          == ([$root.histogramArtifacts[].path] | unique | length)
        and all($root.histogramArtifacts[]; histogramArtifactValid($root; $phase; $mode));
      . as $root
      | .effectiveConfiguration.workload.mode as $mode
      |
      .schemaVersion == "oss-benchmark-result/v1"
      and (.runId | test("^[a-z0-9]([a-z0-9-]{0,30}[a-z0-9])?$"))
      and (.runId | length <= 32)
      and (.instanceId | length > 0)
      and .runId == .effectiveConfiguration.runId
      and .instanceId == .effectiveConfiguration.instanceId
      and ($expectedRunId == "" or .runId == $expectedRunId)
      and .provenance.gitCommit == $commit
      and .provenance.gitDirty == false
      and (.provenance.driverVersion | length > 0 and . != "unknown")
      and (.provenance.frameworkDescription | length > 0)
      and (.provenance.osDescription | length > 0)
      and (.provenance.processArchitecture | length > 0)
      and .provenance.processorCount > 0
      and (.provenance.cpuModel | length > 0)
      and (.provenance.machineName | length > 0)
      and .provenance.serverGc == true
      and .provenance.serializer == "OrleansJsonSerializer"
      and (.provenance.components | keys | sort) == [
        "Azure.Storage.Blobs",
        "HdrHistogram",
        "Microsoft.Crank.EventSources",
        "Microsoft.Orleans.Runtime",
        "Npgsql",
        "Orleans.SearchableStorage",
        "StackExchange.Redis"
      ]
      and all(.provenance.components[]; length > 0 and . != "unknown")
      and (.effectiveConfiguration.storage.backend | ascii_downcase | gsub("-"; "")) == $backend
      and (.effectiveConfiguration.storage.implementationPath | ascii_downcase) == $path
      and (.effectiveConfiguration.topology.mode | ascii_downcase) == $topology
      and ($expectedSiloCount == ""
        or .effectiveConfiguration.topology.siloCount == ($expectedSiloCount | tonumber))
      and (.effectiveConfigurationSha256 | test("^[0-9a-f]{64}$"))
      and (.effectiveConfigurationContentBase64 | length > 0)
      and (.cleanup.policy | length > 0)
      and (.cleanup.attempted | type == "boolean")
      and (.cleanup.succeeded | type == "boolean")
      and (.cleanup.error == null or (.cleanup.error | type == "string"))
      and (.sourceSpecs | map(.kind) | sort) == ["dataset", "scenario", "workload"]
      and ($expectedScenarioSha256 == ""
        or any(.sourceSpecs[];
          .kind == "scenario" and .sha256 == $expectedScenarioSha256))
      and (.warmup == null or phaseValid(.warmup; true; $mode))
      and (.measurement == null or phaseValid(.measurement; true; $mode))
      and (.failedPhase == null or phaseValid(.failedPhase; false; $mode))
      and (
        if $artifactKind == "success" then
          .status == "succeeded"
          and .measurement.completed > 0
          and .measurement.failed == 0
          and .measurement.timedOut == 0
          and .measurement.dropped == 0
          and ($topology != "embedded"
            or (.cleanup.attempted and .cleanup.succeeded and .cleanup.error == null))
          and exactHistogramSet($root; .measurement; $mode)
        else
          .status == "failed"
          and (.failure.type | length > 0)
          and (.failure.message | length > 0)
          and (
            if .measurement != null then
              exactHistogramSet($root; .measurement; $mode)
            elif .failedPhase != null then
              if .effectiveConfiguration.workload.warmupSeconds > 0 and .warmup == null then
                (.histogramArtifacts | length) == 0
              else
                exactHistogramSet($root; .failedPhase; $mode)
              end
            else
              (.histogramArtifacts | length) == 0
            end)
        end)
    ' "$result" >/dev/null

    effective_content=$(jq --exit-status --raw-output '.effectiveConfigurationContentBase64' "$result")
    effective_sha=$(jq --exit-status --raw-output '.effectiveConfigurationSha256' "$result")
    effective_decoded="$validation_temp/effective.json"
    verify_base64_sha256 \
      "$effective_content" \
      "$effective_sha" \
      "$effective_decoded" \
      "effective configuration"
    if ! jq --exit-status --slurpfile decoded "$effective_decoded" \
      '.effectiveConfiguration == $decoded[0]' "$result" >/dev/null; then
      echo "effectiveConfiguration differs structurally from its decoded content" >&2
      exit 1
    fi

    while IFS=$'\t' read -r kind encoded expected_sha; do
      verify_base64_sha256 \
        "$encoded" \
        "$expected_sha" \
        "$validation_temp/source-$kind.json" \
        "source spec '$kind'"
    done < <(jq --raw-output '.sourceSpecs[] | [.kind, .contentBase64, .sha256] | @tsv' "$result")

    if ! jq --exit-status \
      '[.histogramArtifacts[].path] | length == (unique | length)' "$result" >/dev/null; then
      echo "histogram artifact paths must be unique" >&2
      exit 1
    fi

    result_directory=$(dirname "$result")
    while IFS=$'\t' read -r relative_path expected_sha; do
      if [[ ! "$relative_path" =~ ^[A-Za-z0-9][A-Za-z0-9._-]*$ ||
            "$relative_path" == "." ||
            "$relative_path" == ".." ]]; then
        echo "unsafe histogram artifact path: $relative_path" >&2
        exit 1
      fi
      histogram="$result_directory/$relative_path"
      require_file "$histogram"
      if [[ -L "$histogram" ]]; then
        echo "histogram artifact must not be a symbolic link: $histogram" >&2
        exit 1
      fi
      actual_sha=$(sha256sum "$histogram" | cut -d ' ' -f 1)
      if [[ "$actual_sha" != "$expected_sha" ]]; then
        echo "histogram checksum mismatch: $histogram" >&2
        exit 1
      fi
    done < <(jq --raw-output '.histogramArtifacts[] | [.path, .sha256] | @tsv' "$result")
    ;;

  secrets)
    if [[ $# -ne 2 ]]; then
      usage
    fi

    reject_secret_canaries "$2"
    ;;

  *)
    usage
    ;;
esac
