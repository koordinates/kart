import click

from kart.exceptions import CONFLICTING_POSSIBILITIES, InvalidOperation


class ListOfConflicts(list):
    """
    A list of conflicting possibilities.

    There is not necessarily a 1:1 mapping between the model of an import-source or working-copy, and the Kart model.
    In some cases, it may be possible to store multiple instances of something in the import-source or working-copy,
    but only one in the Kart model. For example, GPKG allows the user to store arbitrarily many pieces of metadata
    of arbitrary mimetype associated with a single table, but the Kart model only allows for one - meta/metadata.xml.

    For this reason, querying the state of an import-source, or the working-copy, or generating a diff between some
    commit and the working-copy, will sometimes result in a ListOfConflicts instead of a single item. For example,
    if the user generates a diff between HEAD and the working-copy, and the working-copy has multiple metadata.xml
    associated with dataset DATASET, then the diff generated will look something like this:

    {
      "DATASET": {
        "meta": {
          "metadata.xml": {
            old_value: "<old metadata.xml>",
            new_value: ListOfConflicts(["<first possibility>", "<second possibility>"]),
          }
      }
    }

    Note that this cannot happen when querying the state of a particular commit or diffing commits, since
    every commit conforms to the Kart model.

    Not to confused with the conflicts caused by a merge. A ListOfConflicts:
    - can have arbitrarily many conflicting possibilities, none of which are named
    - all conflicting possibilities are co-present in the import-source or working-copy
    - is not associated with any commit or commits
    - is resolved by the user manually fixing the import source or working copy so that it has only one possibility.

    By contrast, the conflicts from a merge - see AncestorOursTheirs in merge_util.py - are such:
    - have two conflicting possibilities (or three if you count the ancestor as a third possibility)
    - each conflicting possibility comes from a particular commit, where they were valid separately before the merge
    - can be resolved by the user selecting which version will be the winner, by name eg ancestor, ours or theirs.
    """

    pass


def check_diff_is_committable(repo_diff):
    has_conflicts = False
    for ds_path, ds_diff in repo_diff.items():
        # Currently only meta-items can be over-specified and have ListOfConflicts.
        if "meta" not in ds_diff:
            continue
        for key, item in ds_diff["meta"].items():
            if isinstance(item.new_value, ListOfConflicts):
                # TODO - make this output a bit more informative.
                click.echo(
                    f"Sorry, committing more than one {key} for a single dataset ({ds_path}) is not supported",
                    err=True,
                )
                has_conflicts = True
    if has_conflicts:
        raise InvalidOperation(
            "Failed to commit changes into Kart", exit_code=CONFLICTING_POSSIBILITIES
        )


def check_sources_are_importable(sources):
    has_conflicts = False
    for source in sources:
        for key, item in source.meta_items().items():
            if isinstance(item, ListOfConflicts):
                click.echo(
                    f"Sorry, importing more than one {key} for a single dataset ({source.dest_path}) is not supported",
                    err=True,
                )
                has_conflicts = True
    if has_conflicts:
        raise InvalidOperation(
            "Failed to import into the Kart", exit_code=CONFLICTING_POSSIBILITIES
        )
