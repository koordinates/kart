import click
from kart.diff_format import DiffFormat
from .base_diff_writer import BaseDiffWriter


class QuietDiffWriter(BaseDiffWriter):
    # TODO: Pass DiffFormat.NO_DATA_CHANGES to make it more efficient while providing enough info to exit with the correct code.
    def write_diff(self, diff_format=DiffFormat.FULL):
        if diff_format != DiffFormat.FULL:
            raise click.UsageError("Quiet format only supports full diffs")
        # Nothing to write, but we still need to set self.has_changes
        self.has_changes = any(
            self.has_ds_changes_for_path(ds_path) for ds_path in self.all_ds_paths
        )

    def has_ds_changes_for_path(self, ds_path):
        # TODO: optimise - no need to generate the entire diff for a dataset.
        # (This is not quite as bad as it looks, since parts of the diff object are lazily generated.)
        ds_diff = self.get_dataset_diff(ds_path)
        return bool(ds_diff)
