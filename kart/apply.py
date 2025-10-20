import json
from binascii import unhexlify
from datetime import datetime
from enum import Enum, auto

import click
import pygit2

from kart.completion_shared import ref_completer
from kart.core import check_git_user
from kart.cli_util import KartCommand
from kart.diff_structs import (
    FILES_KEY,
    KeyValue,
    Delta,
    DeltaDiff,
    DatasetDiff,
    RepoDiff,
)
from kart.exceptions import (
    NO_TABLE,
    NO_WORKING_COPY,
    InvalidOperation,
    NotFound,
    NotYetImplemented,
)
from kart.geometry import hex_wkb_to_gpkg_geom
from kart.schema import Schema
from kart.serialise_util import b64decode_str, ensure_bytes
from kart.timestamps import iso8601_tz_to_timedelta, iso8601_utc_to_datetime

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


def _meta_change_type(ds_diff_input):
    meta_diff_input = ds_diff_input.get("meta", {})
    if not meta_diff_input:
        return None
    schema_diff = meta_diff_input.get("schema.json", {})
    if "+" in schema_diff and "-" not in schema_diff:
        return MetaChangeType.CREATE_DATASET
    elif "-" in schema_diff and "+" not in schema_diff:
        return MetaChangeType.DELETE_DATASET
    return MetaChangeType.META_UPDATE


def check_change_supported(
    table_dataset_version, dataset, ds_path, meta_change_type, commit
):
    desc = None
    if meta_change_type == MetaChangeType.CREATE_DATASET:
        desc = f"Patch creates dataset '{ds_path}'"
    elif meta_change_type == MetaChangeType.DELETE_DATASET:
        desc = f"Patch deletes dataset '{ds_path}'"
    else:
        desc = f"Patch contains meta changes for dataset '{ds_path}'"

    if table_dataset_version < 2 and meta_change_type is not None:
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
            if g in f:
                f[g] = hex_wkb_to_gpkg_geom(f[g])
        for b in self.bytes_names:
            if b in f:
                f[b] = unhexlify(f[b]) if f[b] is not None else None
        if self.pk_name not in f:
            raise InvalidOperation(
                f"Patch feature is missing required primary key field {self.pk_name!r}: {f}"
            )
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


class FeatureDeltaParser:
    """Parses JSON for a delta - ie {"-": old-value, "+": new-value} - into a Delta object."""

    def __init__(self, old_schema, new_schema):
        self.old_parser = (
            KeyValueParser(old_schema) if old_schema else NullSchemaParser("old")
        )
        self.new_parser = (
            KeyValueParser(new_schema) if new_schema else NullSchemaParser("new")
        )

    def parse(self, change):
        if "*" in change:
            raise NotYetImplemented(
                "Sorry, minimal patches with * values are no longer supported."
            )
        if "--" in change:
            return Delta.delete(self.old_parser.parse(change["--"]))
        elif "++" in change:
            return Delta.insert(self.new_parser.parse(change["++"]))
        else:
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


def parse_file_diff(file_diff_input):
    def convert_half_delta(half_delta):
        if half_delta is None:
            return None
        val = half_delta.value
        if val.startswith("base64:"):
            return (half_delta.key, b64decode_str(val))
        if val.startswith("text:"):
            val = val[5:]  # len("text:") = 5
        return (half_delta.key, ensure_bytes(val))

    def convert_delta(delta):
        return Delta(convert_half_delta(delta.old), convert_half_delta(delta.new))

    delta_diff = DeltaDiff(
        convert_delta(Delta.from_key_and_plus_minus_dict(k, v))
        for (k, v) in file_diff_input.items()
    )
    return DatasetDiff([(FILES_KEY, delta_diff)])


def parse_meta_diff(meta_diff_input):
    def convert_delta(delta):
        if delta.old_key == "schema.json" or delta.new_key == "schema.json":
            return Schema.schema_delta_from_raw_delta(delta)
        return delta

    return DeltaDiff(
        convert_delta(Delta.from_key_and_plus_minus_dict(k, v))
        for (k, v) in meta_diff_input.items()
    )


def parse_feature_diff(feature_diff_input, dataset, meta_diff):
    old_schema = new_schema = None
    if dataset is not None:
        old_schema = new_schema = dataset.schema

    schema_delta = meta_diff.get("schema.json") if meta_diff else None
    if schema_delta and schema_delta.old_value:
        old_schema = schema_delta.old_value
    if schema_delta and schema_delta.new_value:
        new_schema = schema_delta.new_value

    delta_parser = FeatureDeltaParser(old_schema, new_schema)
    return DeltaDiff((delta_parser.parse(change) for change in feature_diff_input))


def apply_patch(
    *,
    repo,
    do_commit,
    patch_file,
    ref="HEAD",
    allow_empty=False,
    amend=False,
    **kwargs,
):
    try:
        patch = json.load(patch_file)
    except json.JSONDecodeError as e:
        raise click.FileError("Failed to parse JSON patch file") from e

    diff_input = patch.get("kart.diff/v1+hexwkb")
    if diff_input is None:
        diff_input = patch.get("sno.diff/v1+hexwkb")
    if diff_input is None:
        raise click.FileError(
            "Failed to parse JSON patch file: patch contains no `kart.diff/v1+hexwkb` object"
        )

    metadata = patch.get("kart.patch/v1")
    if metadata is None:
        metadata = patch.get("sno.patch/v1")
    if metadata is None:
        # Not all diffs are patches.
        raise click.UsageError("Patch contains no author or head information")

    resolve_missing_values_from_rs = None
    if metadata.get("base") is not None:
        # if the patch has a `base` that's present in this repo,
        # then we allow the `-` blobs to be missing, because we can resolve the `-` blobs
        # from that revision.
        try:
            # this only resolves if it's a commit or tree ID, not if it's a symref
            patch_tree = repo[metadata["base"]].peel(pygit2.Tree)
            resolve_missing_values_from_rs = repo.structure(patch_tree)
        except KeyError:
            # this might be fine (if it's a 'full' patch), but maybe we should warn?
            pass

    if ref != "HEAD":
        if not do_commit:
            raise click.UsageError("--no-commit and --ref are incompatible")
        if not ref.startswith("refs/"):
            ref = f"refs/heads/{ref}"
        try:
            repo.references[ref]
        except KeyError:
            raise NotFound(f"No such ref {ref}")

    if amend and not do_commit:
        raise click.UsageError("--no-commit and --amend are incompatible")

    if do_commit:
        check_git_user(repo)

    rs = repo.structure(ref)
    # TODO: this code shouldn't special-case tabular working copies
    # Specifically, we need to check if those part(s) of the WC exists which the patch applies to.
    table_wc = repo.working_copy.tabular
    if not do_commit and not table_wc:
        # TODO: might it be useful to apply without committing just to *check* if the patch applies?
        raise NotFound("--no-commit requires a working copy", exit_code=NO_WORKING_COPY)

    repo.working_copy.check_not_dirty()

    repo_diff = RepoDiff()
    for ds_path, ds_diff_input in diff_input.items():
        if ds_path == FILES_KEY:
            repo_diff[FILES_KEY] = parse_file_diff(ds_diff_input)
            continue

        dataset = rs.datasets().get(ds_path)
        meta_change_type = _meta_change_type(ds_diff_input)
        check_change_supported(
            repo.table_dataset_version, dataset, ds_path, meta_change_type, do_commit
        )

        meta_diff_input = ds_diff_input.get("meta", {})

        if meta_diff_input:
            meta_diff = parse_meta_diff(meta_diff_input)
            repo_diff.recursive_set([ds_path, "meta"], meta_diff)
        else:
            meta_diff = None

        feature_diff_input = ds_diff_input.get("feature", [])
        if feature_diff_input:
            feature_diff = parse_feature_diff(feature_diff_input, dataset, meta_diff)
            repo_diff.recursive_set([ds_path, "feature"], feature_diff)

    if do_commit:
        commit = rs.commit_diff(
            repo_diff,
            metadata.get("message"),
            author=_build_signature(metadata, "author", repo),
            allow_empty=allow_empty,
            amend=amend,
            resolve_missing_values_from_rs=resolve_missing_values_from_rs,
        )
        click.echo(f"Commit {commit.hex}")

        # Only touch the working copy if we applied the patch to the head branch
        if repo.head_commit == commit:
            new_wc_target = commit
        else:
            new_wc_target = None
    else:
        new_wc_target = rs.create_tree_from_diff(
            repo_diff, resolve_missing_values_from_rs=resolve_missing_values_from_rs
        )

    if new_wc_target:
        repo.working_copy.reset(new_wc_target, track_changes_as_dirty=not do_commit)


@click.command(cls=KartCommand)
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
    "--ref",
    default="HEAD",
    help="Which ref to apply the patch onto.",
    shell_complete=ref_completer,  # type: ignore[call-arg]
)
@click.option(
    "--amend",
    default=False,
    is_flag=True,
    help="Amend the previous commit instead of adding a new commit",
)
@click.argument("patch_file", type=click.File("r", encoding="utf-8"))
def apply(ctx, **kwargs):
    """
    Applies and commits the given JSON patch (as created by `kart create-patch`)
    """
    repo = ctx.obj.repo
    apply_patch(repo=repo, **kwargs)
    repo.gc("--auto")
