"""Schema checks, at two different stages of the pipeline.

**Raw stage** — structural only, against the ``RawSchema`` registered for the source.
This is where source schema drift surfaces: a renamed column, a dropped column, an
extra column that appeared without notice. Types are deliberately *not* checked here,
because the raw layer is stored as text; claiming to type-check it would be theatre.

**Normalized stage** — the full contract of the canonical table: columns, dtype families,
nullability, primary-key uniqueness, and a per-row Pydantic model on a sample.
"""

from __future__ import annotations

import pandas as pd
from pydantic import ValidationError

from pipeline.dtypes import family_matches
from pipeline.schemas import RECORD_MODELS, RawSchema, TableSchema
from pipeline.validation.base import CheckResult, Severity, fail, ok

RECORD_SAMPLE_SIZE = 500


# ------------------------------------------------------------------ raw stage


def check_payload_not_empty(frame: pd.DataFrame) -> CheckResult:
    if frame.empty:
        return fail(
            "payload_not_empty",
            "source returned zero rows",
            Severity.ERROR,
            n_offending=0,
        )
    return ok("payload_not_empty", f"{len(frame)} rows received")


def check_min_rows(frame: pd.DataFrame, min_rows: int) -> CheckResult:
    if len(frame) < min_rows:
        return fail(
            "min_rows",
            f"only {len(frame)} rows, expected at least {min_rows}",
            Severity.WARNING,
            n_offending=len(frame),
            min_rows=min_rows,
        )
    return ok("min_rows", f"{len(frame)} rows >= {min_rows}")


def check_duplicate_columns(frame: pd.DataFrame) -> CheckResult:
    columns = [str(c) for c in frame.columns]
    duplicates = sorted({c for c in columns if columns.count(c) > 1})
    if duplicates:
        return fail(
            "duplicate_columns",
            f"payload contains duplicated column labels: {duplicates}",
            Severity.ERROR,
            n_offending=len(duplicates),
            duplicates=duplicates,
        )
    return ok("duplicate_columns", "no duplicated column labels")


def check_raw_schema_drift(frame: pd.DataFrame, raw_schema: RawSchema | None) -> list[CheckResult]:
    """Compare the payload's column set against the registered expectation."""
    if raw_schema is None:
        return [
            fail(
                "raw_schema_registered",
                "no raw schema registered for this source/dataset; drift cannot be detected",
                Severity.WARNING,
            )
        ]

    present = {str(c) for c in frame.columns}
    expected = set(raw_schema.columns)
    required = set(raw_schema.required)

    results: list[CheckResult] = []

    missing_required = sorted(required - present)
    if missing_required:
        results.append(
            fail(
                "raw_required_columns",
                f"source stopped delivering required columns: {missing_required}",
                Severity.ERROR,
                n_offending=len(missing_required),
                missing=missing_required,
                raw_schema_version=raw_schema.version,
            )
        )
    else:
        results.append(
            ok("raw_required_columns", f"all {len(required)} required columns present")
        )

    missing_optional = sorted((expected - required) - present)
    if missing_optional:
        results.append(
            fail(
                "raw_optional_columns",
                f"optional columns absent from payload: {missing_optional}",
                Severity.WARNING,
                n_offending=len(missing_optional),
                missing=missing_optional,
            )
        )

    unexpected = sorted(present - expected)
    if unexpected:
        # New columns are not an error, but they are the earliest visible signal that a
        # provider changed its payload, so they are recorded rather than ignored.
        results.append(
            fail(
                "raw_unexpected_columns",
                f"payload contains columns not in the registered schema: {unexpected}",
                Severity.WARNING,
                n_offending=len(unexpected),
                unexpected=unexpected,
                raw_schema_version=raw_schema.version,
            )
        )
    else:
        results.append(ok("raw_unexpected_columns", "no unexpected columns"))

    return results


# ----------------------------------------------------------- normalized stage


def check_canonical_columns(frame: pd.DataFrame, schema: TableSchema) -> list[CheckResult]:
    present = set(frame.columns)
    missing = [c for c in schema.required_columns if c not in present]
    extra = sorted(present - set(schema.column_names))

    results: list[CheckResult] = []
    if missing:
        results.append(
            fail(
                "canonical_columns",
                f"normalized frame is missing canonical columns: {missing}",
                Severity.ERROR,
                n_offending=len(missing),
                missing=missing,
                schema_version=schema.version,
            )
        )
    else:
        results.append(
            ok("canonical_columns", f"all {len(schema.required_columns)} canonical columns present")
        )
    if extra:
        results.append(
            fail(
                "canonical_extra_columns",
                f"normalized frame carries undeclared columns: {extra}",
                Severity.WARNING,
                n_offending=len(extra),
                extra=extra,
            )
        )
    return results


def check_dtypes(frame: pd.DataFrame, schema: TableSchema) -> CheckResult:
    mismatches: dict[str, str] = {}
    for column, declared in schema.dtype_map().items():
        if column not in frame.columns:
            continue
        if not family_matches(frame[column], declared):
            mismatches[column] = f"declared {declared}, got {frame[column].dtype}"
    if mismatches:
        return fail(
            "dtypes",
            f"{len(mismatches)} column(s) do not match the declared dtype family",
            Severity.ERROR,
            n_offending=len(mismatches),
            mismatches=mismatches,
            schema_version=schema.version,
        )
    return ok("dtypes", "all columns match their declared dtype family")


def check_nullability(
    frame: pd.DataFrame, schema: TableSchema, missing_ratio_error: float
) -> list[CheckResult]:
    """Nulls in non-nullable columns, with severity driven by how much is missing."""
    results: list[CheckResult] = []
    total = max(len(frame), 1)
    for column in schema.non_nullable_columns:
        if column not in frame.columns:
            continue
        n_null = int(frame[column].isna().sum())
        if n_null == 0:
            continue
        ratio = n_null / total
        severity = Severity.ERROR if ratio > missing_ratio_error else Severity.WARNING
        results.append(
            fail(
                "nullability",
                f"{column} is declared non-nullable but has {n_null} nulls "
                f"({ratio:.2%} of rows)",
                severity,
                column=column,
                n_offending=n_null,
                missing_ratio=round(ratio, 6),
                threshold=missing_ratio_error,
            )
        )
    if not results:
        results.append(ok("nullability", "no nulls in non-nullable columns"))
    return results


def check_primary_key_unique(frame: pd.DataFrame, schema: TableSchema) -> CheckResult:
    keys = [c for c in schema.primary_key if c in frame.columns]
    if len(keys) != len(schema.primary_key):
        return fail(
            "primary_key_unique",
            "cannot evaluate primary key: key columns missing",
            Severity.ERROR,
            missing=[c for c in schema.primary_key if c not in frame.columns],
        )
    duplicated = frame.duplicated(subset=keys, keep=False)
    n_duplicated = int(duplicated.sum())
    if n_duplicated:
        examples = (
            frame.loc[duplicated, keys]
            .head(5)
            .astype(str)
            .to_dict(orient="records")
        )
        return fail(
            "primary_key_unique",
            f"{n_duplicated} rows violate primary key {schema.primary_key}",
            Severity.ERROR,
            n_offending=n_duplicated,
            primary_key=list(schema.primary_key),
            examples=examples,
        )
    return ok("primary_key_unique", f"primary key {schema.primary_key} is unique")


def check_record_contract(frame: pd.DataFrame, schema: TableSchema) -> CheckResult:
    """Validate a sample of rows against the strict Pydantic record model.

    Vectorized checks catch statistical problems; the record model catches contract
    problems (a currency code of the wrong length, a negative price, an unexpected
    field). It runs on a sample because per-row model construction is the slowest thing
    in the pipeline and the contract either holds structurally or it does not.
    """
    model = RECORD_MODELS.get(schema.name)
    if model is None:
        return ok("record_contract", "no record model registered")

    columns = [c for c in schema.record_columns if c in frame.columns]
    subset = frame.loc[:, columns]
    if len(subset) > RECORD_SAMPLE_SIZE:
        subset = subset.sample(RECORD_SAMPLE_SIZE, random_state=0)

    failures: list[dict[str, object]] = []
    for record in subset.to_dict(orient="records"):
        payload = {k: (None if pd.isna(v) else v) for k, v in record.items()}
        if isinstance(payload.get("date"), pd.Timestamp):
            payload["date"] = payload["date"].date()
        try:
            model.model_validate(payload)
        except ValidationError as exc:
            failures.append({"row": payload, "errors": exc.errors(include_url=False)[:2]})
            if len(failures) >= 5:
                break

    if failures:
        return fail(
            "record_contract",
            f"{len(failures)} sampled row(s) violate the {model.__name__} contract",
            Severity.ERROR,
            n_offending=len(failures),
            sample_size=len(subset),
            examples=failures[:3],
        )
    return ok("record_contract", f"{len(subset)} sampled rows satisfy {model.__name__}")
