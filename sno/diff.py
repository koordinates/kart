import collections
import copy
import logging
import re
import sys
from pathlib import Path

import click

from .cli_util import MutexOption
from .diff_output import *  # noqa - used from globals()
from .exceptions import (
    InvalidOperation,
    NotYetImplemented,
    NotFound,
    NO_WORKING_COPY,
    UNCATEGORIZED_ERROR,
)


L = logging.getLogger("sno.diff")


class Conflict(Exception):
    pass


class Diff:
    def __init__(
        self, dataset_or_diff, meta=None, inserts=None, updates=None, deletes=None
    ):
        # @meta: {}
        # @inserts: [{object}, ...]
        # @deletes: {pk:(oldObject, newObject), ...}
        # @updates: {pk:{object}, ...}
        if dataset_or_diff is None:
            # empty
            self._data = {}
            self._datasets = {}
        elif isinstance(dataset_or_diff, Diff):
            # clone
            diff = dataset_or_diff
            self._data = copy.deepcopy(diff._data)
            self._datasets = copy.copy(diff._datasets)
        else:
            dataset = dataset_or_diff
            self._data = {
                dataset.path: {
                    "META": meta or {},
                    "I": inserts or [],
                    "U": updates or {},
                    "D": deletes or {},
                }
            }
            self._datasets = {dataset.path: dataset}

    def __invert__(self):
        """ Return a new Diff that is the reverse of this Diff """
        new_diff = Diff(self)
        for ds_path, od in new_diff._data.items():
            ds = new_diff._datasets[ds_path]
            if od["META"]:
                raise NotImplementedError(
                    "Can't invert diffs containing meta changes yet"
                )

            new_diff._data[ds_path] = {
                # deletes become inserts
                "I": list(od["D"].values()),
                # inserts become deletes
                "D": {str(o[ds.primary_key]): o for o in od["I"]},
                # updates are swapped old<>new
                "U": {k: (v1, v0) for k, (v0, v1) in od["U"].items()},
                "META": {},
            }
        return new_diff

    def __or__(self, other):
        """
        Return a new Diff with datasets from this Diff and other.
        If a dataset exists in both this Diff and other, a ValueError will be raised
        """
        my_datasets = set(self._data.keys())
        other_datasets = set(other._data.keys())
        if my_datasets & other_datasets:
            raise ValueError(
                f"Same dataset appears in both Diffs, do you want + ? {', '.join(my_datasets & other_datasets)}"
            )

        new_diff = Diff(self)
        new_diff._data.update(copy.deepcopy(other._data))
        new_diff._datasets.update(copy.deepcopy(other._datasets))
        return new_diff

    def __ior__(self, other):
        """
        Update this Diff with datasets from other.
        If a dataset exists in both this Diff and other, a ValueError will be raised
        """
        my_datasets = set(self._datasets.keys())
        other_datasets = set(other._datasets.keys())
        if my_datasets & other_datasets:
            raise ValueError(
                f"Same dataset appears in both Diffs, do you want += ? {', '.join(my_datasets & other_datasets)}"
            )

        self._data.update(copy.deepcopy(other._data))
        self._datasets.update(copy.copy(other._datasets))
        return self

    @classmethod
    def _add(cls, a, b, a_pk, b_pk):

        if any(a["META"].values()) or any(b["META"].values()):
            raise NotImplementedError("Metadata changes")

        conflict_keys = set()

        # we edit both sides during iteration

        a_inserts = {str(o[a_pk]): o for o in a["I"]}
        a_updates = a["U"].copy()
        a_deletes = a["D"].copy()
        L.debug("initial a.inserts: %s", sorted(a_inserts.keys()))
        L.debug("initial a.updates: %s", sorted(a_updates.keys()))
        L.debug("initial a.deletes: %s", sorted(a_deletes.keys()))

        b_inserts = {str(o[b_pk]): o for o in b["I"]}
        b_updates = b["U"].copy()
        b_deletes = b["D"].copy()
        L.debug("initial b.inserts: %s", sorted(b_inserts.keys()))
        L.debug("initial b.updates: %s", sorted(b_updates.keys()))
        L.debug("initial b.deletes: %s", sorted(b_deletes.keys()))

        out_ins = {}
        out_upd = {}
        out_del = {}

        for pk, o in a_inserts.items():
            # ins + ins -> Conflict
            # ins + upd -> ins
            # ins + del -> noop
            # ins +     -> ins

            b_ins = b_inserts.pop(pk, None)
            if b_ins:
                conflict_keys.add(pk)
                continue

            b_upd = b_updates.pop(pk, None)
            if b_upd:
                out_ins[pk] = b_upd[1]
                continue

            b_del = b_deletes.pop(pk, None)
            if b_del:
                continue  # never existed -> noop

            out_ins[pk] = o

        for pk, (a_old, a_new) in a_updates.items():
            # upd + ins -> Conflict
            # upd + upd -> upd?
            # upd + del -> del
            # upd +     -> upd

            b_ins = b_inserts.pop(pk, None)
            if b_ins:
                conflict_keys.add(pk)
                continue

            b_upd = b_updates.pop(pk, None)
            if b_upd:
                b_old, b_new = b_upd
                if a_old != b_new:
                    out_upd[pk] = (a_old, b_new)
                else:
                    pass  # changed back -> noop
                continue

            b_del = b_deletes.pop(pk, None)
            if b_del:
                out_del[pk] = a_old
                continue

            out_upd[pk] = (a_old, a_new)

        for pk, o in a_deletes.items():
            # del + del -> Conflict
            # del + upd -> Conflict
            # del + ins -> upd?
            # del +     -> del

            b_del = b_deletes.pop(pk, None)
            if b_del:
                conflict_keys.add(pk)
                continue

            b_upd = b_updates.pop(pk, None)
            if b_upd:
                conflict_keys.add(pk)
                continue

            b_ins = b_inserts.pop(pk, None)
            if b_ins:
                if b_ins != o:
                    out_upd[pk] = (o, b_ins)
                else:
                    pass  # inserted same as deleted -> noop
                continue

            out_del[pk] = o

        # we should only have keys left in b.* that weren't in a.*
        L.debug("out_ins: %s", sorted(out_ins.keys()))
        L.debug("out_upd: %s", sorted(out_upd.keys()))
        L.debug("out_del: %s", sorted(out_del.keys()))
        L.debug("remaining b.inserts: %s", sorted(b_inserts.keys()))
        L.debug("remaining b.updates: %s", sorted(b_updates.keys()))
        L.debug("remaining b.deletes: %s", sorted(b_deletes.keys()))

        all_keys = sum(
            [
                list(l)
                for l in [
                    out_ins.keys(),
                    out_upd.keys(),
                    out_del.keys(),
                    b_inserts.keys(),
                    b_updates.keys(),
                    b_deletes.keys(),
                ]
            ],
            [],
        )
        e = set(all_keys)
        if len(e) != len(all_keys):
            e_keys = [
                k for k, count in collections.Counter(all_keys).items() if count > 1
            ]
            raise AssertionError(
                f"Unexpected key conflict between operations: {e_keys}"
            )

        #     + ins -> ins
        #     + upd -> upd
        #     + del -> del
        out_ins.update(b_inserts)
        out_upd.update(b_updates)
        out_del.update(b_deletes)

        return (
            {
                "META": {},
                "I": sorted(out_ins.values(), key=lambda o: o[b_pk]),
                "U": out_upd,
                "D": out_del,
            },
            conflict_keys or None,
        )

    def __add__(self, other):
        my_datasets = set(self._data.keys())
        other_datasets = set(other._data.keys())

        new_diff = Diff(self)
        for ds in other_datasets:
            if ds not in my_datasets:
                new_diff._data[ds] = other._data[ds]
                new_diff._datasets[ds] = other._datasets[ds]
            else:
                rdiff, conflicts = self._add(
                    a=self._data[ds],
                    b=other._data[ds],
                    a_pk=self._datasets[ds].primary_key,
                    b_pk=other._datasets[ds].primary_key,
                )
                if conflicts:
                    raise Conflict(conflicts)
                else:
                    new_diff._data[ds] = rdiff
        return new_diff

    def __iadd__(self, other):
        my_datasets = set(self._data.keys())
        other_datasets = set(other._data.keys())

        for ds in other_datasets:
            if ds not in my_datasets:
                self._data[ds] = other._data[ds]
                self._datasets[ds] = other._datasets[ds]
            else:
                rdiff, conflicts = self._add(
                    a=self._data[ds],
                    b=other._data[ds],
                    a_pk=self._datasets[ds].primary_key,
                    b_pk=other._datasets[ds].primary_key,
                )
                if conflicts:
                    raise Conflict(conflicts)
                else:
                    self._data[ds] = rdiff
        return self

    def __len__(self):
        count = 0
        for dataset_diff in self._data.values():
            count += sum(len(o) for o in dataset_diff.values())
        return count

    def __getitem__(self, dataset):
        return self._data[dataset.path]

    def __iter__(self):
        for ds_path, dsdiff in self._data.items():
            ds = self._datasets[ds_path]
            yield ds, dsdiff

    def __eq__(self, other):
        if set(self._datasets.keys()) != set(other._datasets.keys()):
            return False

        for ds, sdiff in self:
            odiff = other[ds]
            if sorted(sdiff["I"], key=lambda o: o[ds.primary_key]) != sorted(
                odiff["I"], key=lambda o: o[ds.primary_key]
            ):
                return False
            if sdiff["META"] != odiff["META"]:
                return False
            if sdiff["U"] != odiff["U"]:
                return False
            if sdiff["D"] != odiff["D"]:
                return False

        return True

    def dataset_counts(self, dataset=None):
        """Returns a dict containing the count of each type of diff, for a particular dataset."""
        return {k: len(v) for k, v in self._data[dataset.path].items()}

    def counts(self, dataset=None):
        """
        Returns multiple dataset_counts dicts, one for each dataset touched by this diff.
        The dataset_counts dicts are returned in a top-level dict keyed by dataset path.
        """

        return {
            dataset.path: self.dataset_counts(dataset) for dataset in self.datasets()
        }

    def __repr__(self):
        return repr(self._data)

    def datasets(self):
        return self._datasets.values()


def get_dataset_diff(base_rs, target_rs, working_copy, dataset_path, pk_filter):
    dataset = base_rs.get(dataset_path) or target_rs.get(dataset_path)
    diff = Diff(dataset)

    if base_rs != target_rs:
        # diff += base_rs<>target_rs
        base_ds = base_rs.get(dataset_path)
        target_ds = target_rs.get(dataset_path)

        params = {}
        if not base_ds:
            base_ds, target_ds = target_ds, base_ds
            params["reverse"] = True

        diff_cc = base_ds.diff(target_ds, pk_filter=(pk_filter or None), **params)
        L.debug("commit<>commit diff (%s): %s", dataset_path, repr(diff_cc))
        diff += diff_cc

    if working_copy:
        # diff += target_rs<>working_copy
        target_ds = target_rs.get(dataset_path)
        diff_wc = working_copy.diff_db_to_tree(target_ds, pk_filter=(pk_filter or None))
        L.debug(
            "commit<>working_copy diff (%s): %s", dataset_path, repr(diff_wc),
        )
        diff += diff_wc

    return diff


def get_repo_diff(base_rs, target_rs):
    """Generates a Diff for every dataset in both RepositoryStructures."""
    all_datasets = {ds.path for ds in base_rs} | {ds.path for ds in target_rs}
    result = Diff(None)
    for dataset in all_datasets:
        result += get_dataset_diff(base_rs, target_rs, None, dataset, None)
    return result


def diff_with_writer(
    ctx, diff_writer, *, output_path='-', exit_code, args, json_style="pretty"
):
    """
    Calculates the appropriate diff from the arguments,
    and writes it using the given writer contextmanager.

      ctx: the click context
      diff_writer: One of the `diff_output_*` contextmanager factories.
                   When used as a contextmanager, the diff_writer should yield
                   another callable which accepts (dataset, diff) arguments
                   and writes the output by the time it exits.
      output_path: The output path, or a file-like object, or the string '-' to use stdout.
      exit_code:   If True, the process will exit with code 1 if the diff is non-empty.
      args:        The arguments given on the command line, including the refs to diff.
    """
    from .working_copy import WorkingCopy
    from .structure import RepositoryStructure

    try:
        if isinstance(output_path, str) and output_path != "-":
            output_path = Path(output_path).expanduser()

        repo = ctx.obj.repo
        args = list(args)

        # TODO: handle [--] and [<dataset>[:pk]...] without <commit>

        # Parse <commit> or <commit>...<commit>
        commit_arg = args.pop(0) if args else "HEAD"
        commit_parts = re.split(r"(\.{2,3})", commit_arg)

        if len(commit_parts) == 3:
            # Two commits specified - base and target. We diff base<>target.
            base_rs = RepositoryStructure.lookup(repo, commit_parts[0] or "HEAD")
            target_rs = RepositoryStructure.lookup(repo, commit_parts[2] or "HEAD")
            working_copy = None
        else:
            # When one commit is specified, it is base, and we diff base<>working_copy.
            # When no commits are specified, base is HEAD, and we do the same.
            # We diff base<>working_copy by diffing base<>target + target<>working_copy,
            # and target is set to HEAD.
            base_rs = RepositoryStructure.lookup(repo, commit_parts[0])
            target_rs = RepositoryStructure.lookup(repo, "HEAD")
            working_copy = WorkingCopy.open(repo)
            if not working_copy:
                raise NotFound(
                    "No working copy, use 'checkout'", exit_code=NO_WORKING_COPY
                )
            working_copy.assert_db_tree_match(target_rs.tree)

        # Parse [<dataset>[:pk]...]
        pk_filters = {}
        for p in args:
            pp = p.split(":", maxsplit=1)
            pk_filters.setdefault(pp[0], [])
            if len(pp) > 1:
                pk_filters[pp[0]].append(pp[1])

        base_str = base_rs.id
        target_str = "working-copy" if working_copy else target_rs.id
        L.debug('base=%s target=%s', base_str, target_str)

        # check whether we need to do a 3-way merge
        if base_rs.head_commit and target_rs.head_commit:
            merge_base_id = repo.merge_base(base_rs.id, target_rs.id)
            L.debug("Found merge base: %s", merge_base_id)

            if not merge_base_id:
                # there is no relation between the commits
                raise InvalidOperation(
                    f"Commits {base_rs.id} and {target_rs.id} aren't related."
                )
            elif merge_base_id not in (base_rs.id, target_rs.id):
                # this needs a 3-way diff and we don't support them yet
                raise NotYetImplemented(f"Sorry, 3-way diffs aren't supported yet.")

        all_datasets = {ds.path for ds in base_rs} | {ds.path for ds in target_rs}

        if pk_filters:
            all_datasets = set(filter(lambda dsp: dsp in pk_filters, all_datasets))

        writer_params = {
            "repo": repo,
            "base": base_rs,
            "target": target_rs,
            "output_path": output_path,
            "dataset_count": len(all_datasets),
            "json_style": json_style,
        }

        L.debug(
            "base_rs %s == target_rs %s: %s",
            repr(base_rs),
            repr(target_rs),
            base_rs == target_rs,
        )

        num_changes = 0
        with diff_writer(**writer_params) as w:
            for dataset_path in all_datasets:
                diff = get_dataset_diff(
                    base_rs,
                    target_rs,
                    working_copy,
                    dataset_path,
                    pk_filters.get(dataset_path),
                )
                [dataset] = diff.datasets()
                num_changes += len(diff)
                L.debug("overall diff (%s): %s", dataset_path, repr(diff))
                w(dataset, diff[dataset])

    except click.ClickException as e:
        L.debug("Caught ClickException: %s", e)
        if exit_code and e.exit_code == 1:
            e.exit_code = UNCATEGORIZED_ERROR
        raise
    except Exception as e:
        L.debug("Caught non-ClickException: %s", e)
        if exit_code:
            click.secho(f"Error: {e}", fg="red", file=sys.stderr)
            raise SystemExit(UNCATEGORIZED_ERROR) from e
        else:
            raise
    else:
        if exit_code and num_changes:
            sys.exit(1)


@click.command()
@click.pass_context
@click.option(
    "--text",
    "output_format",
    flag_value="text",
    default=True,
    help="Get the diff in text format",
    cls=MutexOption,
    exclusive_with=["html", "json", "geojson", "quiet"],
)
@click.option(
    "--json",
    "output_format",
    flag_value="json",
    help="Get the diff in JSON format",
    hidden=True,
    cls=MutexOption,
    exclusive_with=["html", "text", "geojson", "quiet"],
)
@click.option(
    "--geojson",
    "output_format",
    flag_value="geojson",
    help="Get the diff in GeoJSON format",
    cls=MutexOption,
    exclusive_with=["html", "text", "json", "quiet"],
)
@click.option(
    "--html",
    "output_format",
    flag_value="html",
    help="View the diff in a browser",
    hidden=True,
    cls=MutexOption,
    exclusive_with=["json", "text", "geojson", "quiet"],
)
@click.option(
    "--quiet",
    "output_format",
    flag_value="quiet",
    help="Disable all output of the program. Implies --exit-code.",
    cls=MutexOption,
    exclusive_with=["json", "text", "geojson", "html"],
)
@click.option(
    "--exit-code",
    is_flag=True,
    help="Make the program exit with codes similar to diff(1). That is, it exits with 1 if there were differences and 0 means no differences.",
)
@click.option(
    "--output",
    "output_path",
    help="Output to a specific file/directory instead of stdout.",
    type=click.Path(writable=True, allow_dash=True),
)
@click.option(
    "--json-style",
    type=click.Choice(["extracompact", "compact", "pretty"]),
    default="pretty",
    help="How to format the output. Only used with --json or --geojson",
    cls=MutexOption,
    exclusive_with=["html", "text", "quiet"],
)
@click.argument("args", nargs=-1)
def diff(ctx, output_format, output_path, exit_code, json_style, args):
    """
    Show changes between commits, commit and working tree, etc

    sno diff [options] [--] [<dataset>[:pk]...]
    sno diff [options] <commit> [--] [<dataset>[:pk]...]
    sno diff [options] <commit>..<commit> [--] [<dataset>[:pk]...]
    """

    diff_writer = globals()[f"diff_output_{output_format}"]
    if output_format == "quiet":
        exit_code = True

    return diff_with_writer(
        ctx,
        diff_writer,
        output_path=output_path,
        exit_code=exit_code,
        args=args,
        json_style=json_style,
    )
