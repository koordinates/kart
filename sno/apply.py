import copy
import json
from datetime import datetime

import click

import pygit2

from .exceptions import (
    NO_CHANGES,
    NO_TABLE,
    NO_WORKING_COPY,
    NotFound,
    InvalidOperation,
)
from .diff_structs import RepoDiff, DeltaDiff, Delta
from .geometry import hex_wkb_to_gpkg_geom
from .structure import RepositoryStructure
from .timestamps import iso8601_utc_to_datetime, iso8601_tz_to_timedelta
from .working_copy import WorkingCopy


def unjson_feature(dataset, f):
    if f is None:
        return f
    f = copy.deepcopy(f)
    if dataset.geom_column_name:
        # add geometry in
        f[dataset.geom_column_name] = hex_wkb_to_gpkg_geom(f[dataset.geom_column_name])
    return f


def apply_patch(*, repo, commit, patch_file, allow_empty, **kwargs):
    try:
        patch = json.load(patch_file)
        json_diff = patch['sno.diff/v1+hexwkb']
    except (KeyError, json.JSONDecodeError) as e:
        raise click.FileError("Failed to parse JSON patch file") from e

    rs = RepositoryStructure(repo)
    wc = WorkingCopy.get(repo)
    if not commit and not wc:
        # TODO: might it be useful to apply without committing just to *check* if the patch applies?
        raise NotFound("--no-commit requires a working copy", exit_code=NO_WORKING_COPY)

    if wc:
        wc.check_not_dirty()

    repo_diff = RepoDiff()
    for ds_name, ds_diff_dict in json_diff.items():
        dataset = rs.get(ds_name)
        if dataset is None:
            raise NotFound(
                f"Patch contains dataset '{ds_name}' which is not in this repository",
                exit_code=NO_TABLE,
            )

        meta_changes = ds_diff_dict.get('meta', {})

        if meta_changes:
            if dataset.version < 2:
                raise InvalidOperation(
                    "This repo doesn't support meta changes, use `sno upgrade`"
                )
            meta_diff = DeltaDiff(
                Delta((k, v.get('-')), (k, v.get('+')))
                for (k, v) in meta_changes.items()
            )
            repo_diff.recursive_set([dataset.path, "meta"], meta_diff)

        feature_changes = ds_diff_dict.get('feature', [])
        pk_name = dataset.primary_key

        def extract_key(feature):
            if feature is None:
                return None
            return str(feature[pk_name]), feature

        def parse_delta(change):
            return Delta(
                extract_key(unjson_feature(dataset, change.get('-'))),
                extract_key(unjson_feature(dataset, change.get('+'))),
            )

        if feature_changes:
            feature_diff = DeltaDiff(
                (parse_delta(change) for change in feature_changes)
            )
            repo_diff.recursive_set([dataset.path, "feature"], feature_diff)

    if commit:
        if not repo_diff and not allow_empty:
            raise NotFound("No changes to commit", exit_code=NO_CHANGES)
        try:
            metadata = patch['sno.patch/v1']
        except KeyError:
            # Not all diffs are patches. If we're given a raw diff, we can't commit it properly
            raise click.UsageError(
                "Patch contains no author information, and --no-commit was not supplied"
            )

        default_sig = repo.default_signature
        if 'authorTime' in metadata:
            timestamp = int(
                datetime.timestamp(iso8601_utc_to_datetime(metadata['authorTime']))
            )
        else:
            timestamp = default_sig.time
        if 'authorTimeOffset' in metadata:
            offset = int(
                iso8601_tz_to_timedelta(metadata['authorTimeOffset']).total_seconds()
                / 60  # minutes
            )
        else:
            offset = default_sig.offset

        oid = rs.commit(
            repo_diff,
            metadata['message'],
            author=pygit2.Signature(
                name=metadata.get('authorName', default_sig.name),
                email=metadata.get('authorEmail', default_sig.email),
                time=timestamp,
                offset=offset,
            ),
            allow_empty=allow_empty,
        )
        click.echo(f"Commit {oid.hex}")

    else:
        oid = rs.create_tree_from_diff(repo_diff)

    if wc:
        # oid refers to either a commit or tree
        wc_target = repo.get(oid)
        click.echo(f"Updating {wc.path} ...")
        wc.reset(wc_target, update_meta=commit)


@click.command()
@click.pass_context
@click.option(
    "--commit/--no-commit", "commit", default=True, help="Commit changes",
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
@click.argument("patch_file", type=click.File('r', encoding="utf-8"))
def apply(ctx, **kwargs):
    """
    Applies and commits the given JSON patch (as created by `sno show -o json`)
    """
    apply_patch(repo=ctx.obj.repo, **kwargs)
