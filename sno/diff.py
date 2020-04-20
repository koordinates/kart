import collections
import contextlib
import copy
import io
import json
import logging
import re
import string
import sys
import webbrowser
from pathlib import Path

import click
import pygit2

from . import gpkg
from .cli_util import MutexOption
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


class Diff(object):
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

    def counts(self, dataset):
        return {k: len(v) for k, v in self._data[dataset.path].items()}

    def __repr__(self):
        return repr(self._data)

    def datasets(self):
        return self._datasets.values()


def diff_with_writer(ctx, diff_writer, *, output_path='-', exit_code, args):
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
        paths = {}
        for p in args:
            pp = p.split(":", maxsplit=1)
            paths.setdefault(pp[0], [])
            if len(pp) > 1:
                paths[pp[0]].append(pp[1])

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

        if paths:
            all_datasets = set(filter(lambda dsp: dsp in paths, all_datasets))

        writer_params = {
            "repo": repo,
            "base": base_rs,
            "target": target_rs,
            "output_path": output_path,
            "dataset_count": len(all_datasets),
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

                    diff_cc = base_ds.diff(
                        target_ds, pk_filter=(paths.get(dataset_path) or None), **params
                    )
                    L.debug("commit<>commit diff (%s): %s", dataset_path, repr(diff_cc))
                    diff += diff_cc

                if working_copy:
                    # diff += target_rs<>working_copy
                    target_ds = target_rs.get(dataset_path)
                    diff_wc = working_copy.diff_db_to_tree(
                        target_ds, pk_filter=(paths.get(dataset_path) or None)
                    )
                    L.debug(
                        "commit<>working_copy diff (%s): %s",
                        dataset_path,
                        repr(diff_wc),
                    )
                    diff += diff_wc

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
@click.argument("args", nargs=-1)
def diff(ctx, output_format, output_path, exit_code, args):
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
        ctx, diff_writer, output_path=output_path, exit_code=exit_code, args=args,
    )


@contextlib.contextmanager
def diff_output_quiet(**kwargs):
    """
    Contextmanager.
    Yields a callable which can be called with dataset diffs
    (see `diff_output_text` docstring for more on that)

    Writes nothing to the output. This is useful when you just want to find out
    whether anything has changed in the diff (you can use the exit code)
    and don't need output.
    """

    def _out(dataset, diff):
        pass

    yield _out


@contextlib.contextmanager
def diff_output_text(*, output_path, **kwargs):
    """
    Contextmanager.

    Yields a callable which can be called with dataset diffs.
    The callable takes two arguments:
        dataset: A sno.structure.DatasetStructure instance representing
                 either the old or new version of the dataset.
        diff:    The sno.diff.Diff instance to serialize

    On exit, writes a human-readable diff to the given output file.

    Certain shortcuts are taken to make the diff human readable,
    so it may not be suitable as a patch to apply.
    In particular, geometry WKT is abbreviated and null values are represented
    by a unicode "␀" character.
    """
    fp = resolve_output_path(output_path)
    pecho = {'file': fp, 'color': fp.isatty()}
    if isinstance(output_path, Path) and output_path.is_dir():
        raise click.BadParameter(
            "Directory is not valid for --output with --text", param_hint="--output"
        )

    def _out(dataset, diff):
        path = dataset.path
        pk_field = dataset.primary_key
        prefix = f"{path}:"
        repr_excl = [pk_field]

        for k, (v_old, v_new) in diff["META"].items():
            click.secho(
                f"--- {prefix}meta/{k}\n+++ {prefix}meta/{k}", bold=True, **pecho
            )

            s_old = set(v_old.items())
            s_new = set(v_new.items())

            diff_add = dict(s_new - s_old)
            diff_del = dict(s_old - s_new)
            all_keys = set(diff_del.keys()) | set(diff_add.keys())

            for k in all_keys:
                if k in diff_del:
                    click.secho(
                        _repr_row({k: diff_del[k]}, prefix="- ", exclude=repr_excl),
                        fg="red",
                        **pecho,
                    )
                if k in diff_add:
                    click.secho(
                        _repr_row({k: diff_add[k]}, prefix="+ ", exclude=repr_excl),
                        fg="green",
                        **pecho,
                    )

        prefix = f"{path}:{pk_field}="

        for k, v_old in diff["D"].items():
            click.secho(f"--- {prefix}{k}", bold=True, **pecho)
            click.secho(
                _repr_row(v_old, prefix="- ", exclude=repr_excl), fg="red", **pecho
            )

        for o in diff["I"]:
            click.secho(f"+++ {prefix}{o[pk_field]}", bold=True, **pecho)
            click.secho(
                _repr_row(o, prefix="+ ", exclude=repr_excl), fg="green", **pecho
            )

        for _, (v_old, v_new) in diff["U"].items():
            click.secho(
                f"--- {prefix}{v_old[pk_field]}\n+++ {prefix}{v_new[pk_field]}",
                bold=True,
                **pecho,
            )

            s_old = set(v_old.items())
            s_new = set(v_new.items())

            diff_add = dict(s_new - s_old)
            diff_del = dict(s_old - s_new)
            all_keys = sorted(set(diff_del.keys()) | set(diff_add.keys()))

            for k in all_keys:
                if k in diff_del:
                    rk = _repr_row({k: diff_del[k]}, prefix="- ", exclude=repr_excl)
                    if rk:
                        click.secho(rk, fg="red", **pecho)
                if k in diff_add:
                    rk = _repr_row({k: diff_add[k]}, prefix="+ ", exclude=repr_excl)
                    if rk:
                        click.secho(rk, fg="green", **pecho)

    yield _out


def _repr_row(row, prefix="", exclude=None):
    m = []
    exclude = exclude or set()
    for k in sorted(row.keys()):
        if k.startswith("__") or k in exclude:
            continue

        v = row[k]

        if isinstance(v, bytes):
            g = gpkg.geom_to_ogr(v)
            geom_typ = g.GetGeometryName()
            if g.IsEmpty():
                v = f"{geom_typ} EMPTY"
            else:
                v = f"{geom_typ}(...)"
            del g

        v = "␀" if v is None else v
        m.append("{prefix}{k:>40} = {v}".format(k=k, v=v, prefix=prefix))

    return "\n".join(m)


@contextlib.contextmanager
def diff_output_geojson(*, output_path, dataset_count, **kwargs):
    """
    Contextmanager.

    Yields a callable which can be called with dataset diffs
    (see `diff_output_text` docstring for more on that)

    For features already existed but have changed, two features are written to the output:
    one for the 'deleted' version of the feature, and one for the 'added' version.
    This is intended for visualising in a map diff.

    On exit, writes the diff as GeoJSON to the given output file.
    For repos with more than one dataset, the output path must be a directory.
    In that case:
        * any .geojson files already in that directory will be deleted
        * files will be written to `{layer_name}.geojson in the given directory

    If the output file is stdout and isn't piped anywhere,
    the json is prettified before writing.
    """
    if dataset_count > 1:
        # output_path needs to be a directory
        if not output_path:
            raise click.BadParameter(
                "Need to specify a directory via --output for --geojson with >1 dataset",
                param_hint="--output",
            )
        elif output_path == "-" or output_path.is_file():
            raise click.BadParameter(
                "A file is not valid for --output + --geojson with >1 dataset",
                param_hint="--output",
            )

        if not output_path.exists():
            output_path.mkdir()
        else:
            for p in output_path.glob("*.geojson"):
                p.unlink()

    def _out(dataset, diff):
        json_params = {}
        if not output_path or output_path == '-':
            fp = sys.stdout
            json_params = {"indent": 2, "sort_keys": True}
        elif output_path.is_dir():
            fp = (output_path / f"{dataset.name}.geojson").open("w")
        else:
            fp = output_path.open("w")

        pk_field = dataset.primary_key

        fc = {"type": "FeatureCollection", "features": []}

        for k, (v_old, v_new) in diff["META"].items():
            click.secho(
                f"Warning: meta changes aren't included in GeoJSON output: {k}",
                fg="yellow",
                file=sys.stderr,
            )

        for k, v_old in diff["D"].items():
            fc["features"].append(_json_row(v_old, "D", pk_field))

        for o in diff["I"]:
            fc["features"].append(_json_row(o, "I", pk_field))

        for _, (v_old, v_new) in diff["U"].items():
            fc["features"].append(_json_row(v_old, "U-", pk_field))
            fc["features"].append(_json_row(v_new, "U+", pk_field))

        json.dump(fc, fp, **json_params)

    yield _out


def resolve_output_path(output_path):
    """
    Takes a path-ish thing, and returns the appropriate writable file-like object.
    The path-ish thing could be:
      * a pathlib.Path object
      * a file-like object
      * the string '-' or None (both will return sys.stdout)
    """
    if isinstance(output_path, io.IOBase):
        return output_path
    elif (not output_path) or output_path == "-":
        return sys.stdout
    else:
        return output_path.open("w")


def dump_json_diff_output(output, output_path):
    """
    Dumps the output to JSON in the output file.
    """
    json_params = {}
    if (not output_path) or output_path == "-":
        # Prettier output for humans
        json_params.update({"indent": 2, "sort_keys": True})
    fp = resolve_output_path(output_path)
    json.dump(output, fp, **json_params)


@contextlib.contextmanager
def diff_output_json(*, output_path, dataset_count, **kwargs):
    """
    Contextmanager.
    Yields a callable which can be called with dataset diffs
    (see `diff_output_text` docstring for more on that)

    On exit, writes the diff as JSON to the given output file.
    If the output file is stdout and isn't piped anywhere,
    the json is prettified first.
    """
    if isinstance(output_path, Path):
        if output_path.is_dir():
            raise click.BadParameter(
                "Directory is not valid for --output with --json", param_hint="--output"
            )

    accumulated = {}

    def _out(dataset, diff):
        pk_field = dataset.primary_key

        d = {"metaChanges": {}, "featureChanges": []}
        for k, (v_old, v_new) in diff["META"].items():
            d["metaChanges"][k] = [v_old, v_new]

        for k, v_old in diff["D"].items():
            d["featureChanges"].append({'-': _json_row(v_old, "D", pk_field)})

        for o in diff["I"]:
            d["featureChanges"].append({'+': _json_row(o, "I", pk_field)})

        for _, (v_old, v_new) in diff["U"].items():
            d["featureChanges"].append(
                {
                    '-': _json_row(v_old, "U-", pk_field),
                    '+': _json_row(v_new, "U+", pk_field),
                }
            )

        # sort for reproducibility
        d["featureChanges"].sort(
            key=lambda fc: (
                fc['-']["id"] if '-' in fc else "",
                fc['+']["id"] if '+' in fc else "",
            )
        )
        accumulated[dataset.path] = d

    yield _out

    dump_json_diff_output({"sno.diff/v1": accumulated}, output_path)


def _json_row(row, change, pk_field):
    f = {
        "type": "Feature",
        "geometry": None,
        "properties": {},
        "id": f"{change}::{row[pk_field]}",
    }

    for k in row.keys():
        v = row[k]
        if isinstance(v, bytes):
            g = gpkg.geom_to_ogr(v)
            f["geometry"] = json.loads(g.ExportToJson())
        else:
            f["properties"][k] = v

    return f


@contextlib.contextmanager
def diff_output_html(*, output_path, repo, base, target, dataset_count, **kwargs):
    """
    Contextmanager.
    Yields a callable which can be called with dataset diffs
    (see `diff_output_text` docstring for more on that)

    On exit, writes an HTML diff to the given output file
    (defaults to 'DIFF.html' in the repo directory).

    If `-` is given as the output file, the HTML is written to stdout,
    and no web browser is opened.
    """
    if isinstance(output_path, Path):
        if output_path.is_dir():
            raise click.BadParameter(
                "Directory is not valid for --output with --html", param_hint="--output"
            )

    json_data = io.StringIO()
    with diff_output_json(
        output_path=json_data, dataset_count=dataset_count
    ) as json_writer:
        yield json_writer

    with open(
        Path(__file__).resolve().with_name("diff-view.html"), "r", encoding="utf8"
    ) as ft:
        template = string.Template(ft.read())

    title = f"{Path(repo.path).name}: {base.short_id} .. {target.short_id if target else 'working-copy'}"

    if not output_path:
        output_path = Path(repo.path) / "DIFF.html"
    fo = resolve_output_path(output_path)

    fo.write(
        template.substitute({"title": title, "geojson_data": json_data.getvalue()})
    )
    if fo != sys.stdout:
        fo.close()
        webbrowser.open_new(f"file://{output_path.resolve()}")
