import click

from kart.exceptions import WORKING_COPY_OR_IMPORT_CONFLICT, InvalidOperation


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

    def get_error_message(self, item_name, ds_path, is_import_cmd=False):
        error_message = getattr(self, "error_message", None)
        if not error_message:
            error_message = self._generate_error_message(
                item_name, ds_path, is_import_cmd=is_import_cmd
            )
        return error_message

    def _generate_error_message(self, item_name, ds_path, is_import_cmd=False):
        verb = "Importing" if is_import_cmd else "Committing"
        result = f"{verb} more than one {item_name!r} for {ds_path!r} is not supported."
        if not is_import_cmd and item_name == "format.json":
            result += " Specify --convert-to-dataset-format to convert the new tiles to match the dataset's format."
        return result


class InvalidNewValue(ListOfConflicts):
    """
    Less commonly than multiple conflicting values, sometimes there are single values that are disallowed in the Kart model.

    Extending "ListOfConflicts" means to cover this use-case means we only need to handle the one special case during diffs -
    - an InvalidNewValue is mostly just a ListOfConflicts of length 1, but with a different error message.
    """

    def _generate_error_message(self, item_name, ds_path, is_import_cmd=False):
        verb = "import" if is_import_cmd else "commit"
        return f"Cannot {verb} invalid {item_name!r} for {ds_path!r}."


def check_repo_diff_is_committable(repo_diff):
    errors = []
    extract_error_messages_from_repo_diff(repo_diff, errors)
    if not errors:
        return

    for error in errors:
        click.echo(error, err=True)
    raise InvalidOperation(
        "Failed to commit changes",
        exit_code=WORKING_COPY_OR_IMPORT_CONFLICT,
    )


def extract_error_messages_from_repo_diff(repo_diff, result):
    for ds_path, ds_diff in repo_diff.items():
        extract_error_messages_from_dataset_diff(ds_path, ds_diff, result)


def extract_error_messages_from_dataset_diff(dataset_path, dataset_diff, result):
    # Currently only meta-items can be over-specified and have ListOfConflicts.
    if "meta" not in dataset_diff:
        return
    for key, item in dataset_diff["meta"].items():
        if isinstance(item.new_value, ListOfConflicts):
            result.append(item.new_value.get_error_message(key, dataset_path))


def check_sources_are_importable(sources):
    has_conflicts = False
    for source in sources:
        for key, item in source.meta_items().items():
            if isinstance(item, ListOfConflicts):
                item.print_error_message(key, source.dest_path, is_import_cmd=True)
                has_conflicts = True
    if has_conflicts:
        raise InvalidOperation(
            "Failed to import", exit_code=WORKING_COPY_OR_IMPORT_CONFLICT
        )
