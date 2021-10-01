from binascii import unhexlify
import json
from datetime import datetime
from enum import Enum, auto

import click

from .exceptions import (
    NO_TABLE,
    NO_WORKING_COPY,
    NotFound,
    NotYetImplemented,
    InvalidOperation,
)
from .diff_structs import RepoDiff, DeltaDiff, Delta, KeyValue
from .geometry import hex_wkb_to_gpkg_geom
from .schema import Schema
from .timestamps import iso8601_utc_to_datetime, iso8601_tz_to_timedelta


V1_NO_META_UPDATE = (
    "Sorry, patches that make meta changes are not supported until Datasets V2\n"
    "Use `kart upgrade`"
)
V1_NO_DATASET_CREATE_DELETE = (
    "Sorry, patches that create or delete datasets are not supported until Datasets V2\n"
    "Use `kart upgrade`"
)
NO_COMMIT_NO_DATASET_CREATE_DELETE = (
    "Sorry, patches that create or delete datasets cannot be applied with --no-commit"
)


class MetaChangeType(Enum):
    CREATE_DATASET = auto()
    DELETE_DATASET = auto()
    META_UPDATE = auto()


def _meta_change_type(ds_diff_dict, allow_missing_old_values):
    meta_diff = ds_diff_dict.get("meta", {})
    if not meta_diff:
        return None
    schema_diff = meta_diff.get("schema.json", {})
    if "+" in schema_diff and "-" not in schema_diff and not allow_missing_old_values:
        return MetaChangeType.CREATE_DATASET
    elif "-" in schema_diff and "+" not in schema_diff:
        return MetaChangeType.DELETE_DATASET
    return MetaChangeType.META_UPDATE


def check_change_supported(repo_version, dataset, ds_path, meta_change_type, commit):
    desc = None
    if meta_change_type == MetaChangeType.CREATE_DATASET:
        desc = f"Patch creates dataset '{ds_path}'"
    elif meta_change_type == MetaChangeType.DELETE_DATASET:
        desc = f"Patch deletes dataset '{ds_path}'"
    else:
        desc = f"Patch contains meta changes for dataset '{ds_path}'"

    if repo_version < 2 and meta_change_type is not None:
        message = (
            V1_NO_META_UPDATE
            if meta_change_type == MetaChangeType.META_UPDATE
            else V1_NO_DATASET_CREATE_DELETE
        )
        raise NotYetImplemented(f"{desc}\n{message}")

    if dataset is None and meta_change_type != MetaChangeType.CREATE_DATASET:
        raise NotFound(
            f"Patch contains changes for dataset '{ds_path}' which is not in this repository",
            exit_code=NO_TABLE,
        )
    if dataset is not None and meta_change_type == MetaChangeType.CREATE_DATASET:
        raise InvalidOperation(
            f"Patch creates dataset '{ds_path}' which already exists in this repository"
        )
    if not commit and meta_change_type in (
        MetaChangeType.CREATE_DATASET,
        MetaChangeType.DELETE_DATASET,
    ):
        raise InvalidOperation(f"{desc}\n{NO_COMMIT_NO_DATASET_CREATE_DELETE}")


class KeyValueParser:
    """Parses JSON for an individual feature object into a KeyValue object."""

    def __init__(self, schema):
        self.pk_name = schema.pk_columns[0].name
        self.geom_names = [c.name for c in schema.geometry_columns]
        self.bytes_names = [c.name for c in schema.columns if c.data_type == "blob"]

    def parse(self, f):
        if f is None:
            return None
        for g in self.geom_names:
            f[g] = hex_wkb_to_gpkg_geom(f[g])
        for b in self.bytes_names:
            f[b] = unhexlify(f[b]) if f[b] is not None else None
        pk = f[self.pk_name]
        return KeyValue.of((pk, f))


class NullSchemaParser:
    """
    A parser which expects only null values, since there is no schema for parsing actual features.
    Useful for parsing all the old values in a patch which creates a new dataset - these should all be null.
    """

    def __init__(self, old_or_new):
        self.old_or_new = old_or_new

    def parse(self, f):
        if f is None:
            return None
        raise InvalidOperation(
            f"Can't parse {self.old_or_new} feature value - {self.old_or_new} schema is missing"
        )


class DeltaParser:
    """Parses JSON for a delta - ie {"-": old-value, "+": new-value} - into a Delta object."""

    def __init__(self, old_schema, new_schema):
        self.old_parser = (
            KeyValueParser(old_schema) if old_schema else NullSchemaParser("old")
        )
        self.new_parser = (
            KeyValueParser(new_schema) if new_schema else NullSchemaParser("new")
        )

    def parse(self, change):
        return Delta(
            self.old_parser.parse(change.get("-")),
            self.new_parser.parse(change.get("+")),
        )


def _build_signature(patch_metadata, person, repo):
    signature = {}
    for signature_key, patch_key in (
        ("time", f"{person}Time"),
        ("email", f"{person}Email"),
        ("offset", f"{person}TimeOffset"),
        ("name", f"{person}Name"),
    ):
        if patch_key in patch_metadata:
            signature[signature_key] = patch_metadata[patch_key]

    if "time" in signature:
        signature["time"] = int(
            datetime.timestamp(iso8601_utc_to_datetime(signature["time"]))
        )
    if "offset" in signature:
        signature["offset"] = int(
            iso8601_tz_to_timedelta(signature["offset"]).total_seconds() / 60  # minutes
        )

    return repo.author_signature(**signature)


def apply_patch(
    *,
    repo,
    do_commit,
    patch_file,
    allow_empty,
    allow_missing_old_values=False,
    ref="HEAD",
    **kwargs,
):
    try:
        patch = json.load(patch_file)
    except json.JSONDecodeError as e:
        raise click.FileError("Failed to parse JSON patch file") from e

    json_diff = patch.get("kart.diff/v1+hexwkb")
    if json_diff is None:
        json_diff = patch.get("sno.diff/v1+hexwkb")
    if json_diff is None:
        raise click.FileError(
            "Failed to parse JSON patch file: patch contains no `kart.diff/v1+hexwkb` object"
        )
    if ref != "HEAD":
        if not do_commit:
            raise click.UsageError("--no-commit and --ref are incompatible")
        if not ref.startswith("refs/heads/"):
            ref = f"refs/heads/{ref}"
        try:
            repo.references[ref]
        except KeyError:
            raise NotFound(f"No such ref {ref}")

    rs = repo.structure(ref)
    wc = repo.working_copy
    if not do_commit and not wc:
        # TODO: might it be useful to apply without committing just to *check* if the patch applies?
        raise NotFound("--no-commit requires a working copy", exit_code=NO_WORKING_COPY)

    if wc:
        wc.check_not_dirty()

    repo_diff = RepoDiff()
    for ds_path, ds_diff_dict in json_diff.items():
        dataset = rs.datasets.get(ds_path)
        meta_change_type = _meta_change_type(ds_diff_dict, allow_missing_old_values)
        check_change_supported(
            repo.version, dataset, ds_path, meta_change_type, do_commit
        )

        meta_changes = ds_diff_dict.get("meta", {})

        if meta_changes:
            meta_diff = DeltaDiff(
                Delta(
                    (k, v["-"]) if "-" in v else None,
                    (k, v["+"]) if "+" in v else None,
                )
                for (k, v) in meta_changes.items()
            )
            repo_diff.recursive_set([ds_path, "meta"], meta_diff)
        else:
            meta_diff = None

        feature_changes = ds_diff_dict.get("feature", [])
        if feature_changes:
            old_schema = new_schema = None
            if dataset is not None:
                old_schema = new_schema = dataset.schema

            schema_delta = meta_diff.get("schema.json") if meta_diff else None
            if schema_delta and schema_delta.old_value:
                old_schema = Schema.from_column_dicts(schema_delta.old_value)
            if schema_delta and schema_delta.new_value:
                new_schema = Schema.from_column_dicts(schema_delta.new_value)

            delta_parser = DeltaParser(old_schema, new_schema)
            feature_diff = DeltaDiff(
                (delta_parser.parse(change) for change in feature_changes)
            )
            repo_diff.recursive_set([ds_path, "feature"], feature_diff)

    if do_commit:
        metadata = patch.get("kart.patch/v1")
        if metadata is None:
            metadata = patch.get("sno.patch/v1")
        if metadata is None:
            # Not all diffs are patches. If we're given a raw diff, we can't commit it properly
            raise click.UsageError(
                "Patch contains no author information (`kart.patch/v1` object), and --no-commit was not supplied"
            )

        commit = rs.commit_diff(
            repo_diff,
            metadata["message"],
            author=_build_signature(metadata, "author", repo),
            allow_empty=allow_empty,
            allow_missing_old_values=allow_missing_old_values,
        )
        click.echo(f"Commit {commit.hex}")

        # Only touch the working copy if we applied the patch to the head branch
        if repo.head_commit == commit:
            new_wc_target = commit
        else:
            new_wc_target = None
    else:
        new_wc_target = rs.create_tree_from_diff(
            repo_diff,
            allow_missing_old_values=allow_missing_old_values,
        )

    if wc and new_wc_target:
        # oid refers to either a commit or tree
        click.echo(f"Updating {wc} ...")
        wc.reset(new_wc_target, track_changes_as_dirty=not do_commit)


@click.command()
@click.pass_context
@click.option(
    "--commit/--no-commit",
    "do_commit",
    default=True,
    help="Commit changes",
)
@click.option(
    "--allow-empty",
    is_flag=True,
    default=False,
    help=(
        "Usually recording a commit that has the exact same tree as its sole "
        "parent commit is a mistake, and the command prevents you from making "
        "such a commit. This option bypasses the safety"
    ),
)
@click.option(
    "--allow-missing-old-values",
    is_flag=True,
    default=False,
    hidden=True,
    help=(
        "Treats deltas with no '-' value loosely, as either an "
        "insert or an update. Doesn't check for conflicts with the old "
        "version of the feature. For use in external patch generators that "
        "don't have access to the old features, or which have extra "
        "certainty about the applicability of the patch. Use with caution."
    ),
)
@click.option("--ref", default="HEAD", help="Which ref to apply the patch onto.")
@click.argument("patch_file", type=click.File("r", encoding="utf-8"))
def apply(ctx, **kwargs):
    """
    Applies and commits the given JSON patch (as created by `kart show -o json`)
    """
    repo = ctx.obj.repo
    apply_patch(repo=repo, **kwargs)
    repo.gc("--auto")
