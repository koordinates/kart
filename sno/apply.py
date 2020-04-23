import copy
import json
from datetime import datetime

import click

from osgeo import ogr
import pygit2

from .diff import Diff
from .exceptions import (
    NO_CHANGES,
    NO_WORKING_COPY,
    NotFound,
    NotYetImplemented,
    InvalidOperation,
)
from .gpkg import ogr_to_geom
from .structure import RepositoryStructure
from .timestamps import iso8601_utc_to_datetime, iso8601_tz_to_timedelta
from .working_copy import WorkingCopy


def ungeojson_feature(dataset, d):
    if d is None:
        return d
    r = copy.deepcopy(d['properties'])
    if dataset.geom_column_name:
        # add geometry in
        r[dataset.geom_column_name] = ogr_to_geom(
            ogr.CreateGeometryFromJson(json.dumps(d['geometry']))
        )
    return r


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
def apply(ctx, *, commit, patch_file, allow_empty, **kwargs):
    """
    Applies and commits the given JSON patch (as created by `sno show --json`)
    """
    try:
        patch = json.load(patch_file)
        json_diff = patch['sno.diff/v1']
    except (KeyError, json.JSONDecodeError):
        raise click.FileError("Failed to parse JSON patch file")

    repo = ctx.obj.repo
    rs = RepositoryStructure(repo)
    wc = WorkingCopy.open(repo)
    if not commit and not wc:
        # TODO: might it be useful to apply without committing just to *check* if the patch applies?
        raise NotFound("--no-commit requires a working copy", exit_code=NO_WORKING_COPY)

    if wc and wc.is_dirty():
        raise InvalidOperation(
            "You have uncommitted changes in your working copy. Commit or discard first"
        )

    diff = Diff(None)
    for ds_name, ds_diff_dict in json_diff.items():
        meta_changes = ds_diff_dict.get('metaChanges', {})
        if meta_changes:
            raise NotYetImplemented(
                "Patches containing schema changes are not yet handled"
            )

        feature_changes = ds_diff_dict['featureChanges']
        if not feature_changes:
            continue

        inserts = []
        updates = {}
        deletes = {}
        dataset = rs.get(ds_name)
        for change in feature_changes:
            old = ungeojson_feature(dataset, change.get('-'))
            new = ungeojson_feature(dataset, change.get('+'))
            if old and new:
                # update
                assert old['fid'] == new['fid']
                updates[old['fid']] = (old, new)
            elif old:
                deletes[old['fid']] = old
            else:
                inserts.append(new)
        diff += Diff(dataset, inserts=inserts, updates=updates, deletes=deletes)

    if commit:
        if not diff and not allow_empty:
            raise NotFound("No changes to commit", exit_code=NO_CHANGES)
        try:
            metadata = patch['sno.patch/v1']
        except KeyError:
            # Not all diffs are patches. If we're given a raw diff, we can't commit it properly
            # TODO: maybe commit as current user with current timestamp? or maybe don't bother.
            raise click.UsageError(
                "Patch contains no author information, and --no-commit was not supplied"
            )

        oid = rs.commit(
            diff,
            metadata['message'],
            author=pygit2.Signature(
                name=metadata['authorName'],
                email=metadata['authorEmail'],
                time=int(
                    datetime.timestamp(iso8601_utc_to_datetime(metadata['authorTime']))
                ),
                offset=int(
                    iso8601_tz_to_timedelta(
                        metadata['authorTimeOffset']
                    ).total_seconds()
                    / 60  # minutes
                ),
            ),
            allow_empty=allow_empty,
            # Don't call WorkingCopy.commit_callback(), because it *assumes* the working
            # copy already has changes being committed. In this case the working copy
            # does *not* have the changes yet. We tackle updating the working copy below.
            update_working_copy_head=False,
        )
        click.echo(f"Commit {oid.hex}")

    else:
        oid = rs.create_tree_from_diff(diff)

    if wc:
        # oid refers to either a commit or tree
        wc_target = repo.get(oid)
        click.echo(f"Updating {wc.path} ...")
        wc.reset(wc_target, rs)
