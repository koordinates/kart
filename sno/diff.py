import contextlib
import copy
import logging
import re

import click
import pygit2

from . import gpkg
from .cli_util import MutexOption


L = logging.getLogger('sno.diff')


class Diff(object):
    def __init__(self, dataset_or_diff, meta=None, inserts=None, updates=None, deletes=None):
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
                    'META': meta or {},
                    'I': inserts or [],
                    'U': updates or {},
                    'D': deletes or {},
                }
            }
            self._datasets = {dataset.path: dataset}

    def __add__(self, other):
        my_datasets = set(self._data.keys())
        other_datasets = set(other._data.keys())
        if my_datasets & other_datasets:
            raise NotImplementedError(f"Same dataset appears in both Diffs? {', '.join(my_datasets & other_datasets)}")

        new_diff = Diff(self)
        new_diff._data.update(copy.deepcopy(other._data))
        return new_diff

    def __iadd__(self, other):
        my_datasets = set(self._datasets.keys())
        other_datasets = set(other._datasets.keys())
        if my_datasets & other_datasets:
            raise NotImplementedError(f"Same dataset appears in both Diffs? {', '.join(my_datasets & other_datasets)}")

        self._data.update(copy.deepcopy(other._data))
        self._datasets.update(copy.copy(other._datasets))
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

    def counts(self, dataset):
        return {k: len(v) for k, v in self._data[dataset.path].items()}

    def repr(self):
        return repr(self._data)


@click.command()
@click.pass_context
@click.option(
    "--text",
    "output_format",
    flag_value="text",
    default=True,
    help="Get the diff in text format",
    cls=MutexOption, exclusive_with=["html", "json"]
)
@click.option(
    "--output",
    "output_file",
    help="Output to a specific file instead of stdout.",
    type=click.File(mode='w')
)
@click.argument('args', nargs=-1)
def diff(ctx, output_format, output_file, args):
    """
    Show changes between commits, commit and working tree, etc

    sno diff [options] [--] [<dataset>[:pk]...]
    sno diff [options] <commit> [--] [<dataset>[:pk]...]
    sno diff [options] <commit>..<commit> [--] [<dataset>[:pk]...]
    """
    from .working_copy import WorkingCopy
    from .structure import RepositoryStructure

    repo_dir = ctx.obj["repo_dir"]
    repo = pygit2.Repository(repo_dir)
    if not repo:
        raise click.BadParameter("Not an existing repository", param_hint="--repo")

    paths = {}
    commit_base = repo.head.peel(pygit2.Commit)
    commit_target = None  # None for working-copy
    if len(args):
        path_list = list(args)
        commit_parts = re.split(r'(\.{2,3})', args[0])

        if len(commit_parts) == 3:
            try:
                commit_base = repo.revparse_single(commit_parts[0] or 'HEAD')
                commit_target = repo.revparse_single(commit_parts[2] or 'HEAD')
                L.debug("commit_target=%s", commit_target.id)
            except KeyError:
                raise click.BadParameter('Invalid commit spec', param_hint='commit')
            else:
                path_list.pop(0)
        else:
            try:
                commit_base = repo.revparse_single(commit_parts[0] or 'HEAD')
            except KeyError:
                pass
            else:
                path_list.pop(0)

        for p in path_list:
            pp = p.split(':', maxsplit=1)
            paths.setdefault(pp[0], [])
            if len(pp) > 1:
                paths[pp[0]].append(pp[1])

    if commit_target is None:
        L.debug("commit_target=working-copy")
        working_copy = WorkingCopy.open(repo)
        if not working_copy:
            raise click.UsageError("No working copy, use 'checkout'")

        if commit_base != repo.head.peel(pygit2.Commit):
            raise click.ClickException("Diffs between WorkingCopy and non-HEAD aren't supported yet.")

        working_copy.assert_db_tree_match(commit_base.peel(pygit2.Tree))

    L.debug("commit_base=%s", commit_base.id)

    diff_writer = globals()[f"diff_output_{output_format}"]
    writer_params = {
        'repo': repo,
        'commit_base': commit_base,
        'commit_target': commit_target,
        'fp': output_file,
    }

    with diff_writer(**writer_params) as w:
        base_rs = RepositoryStructure(repo, commit_base)
        if commit_target is None:
            # diff against working copy
            for dataset in base_rs:
                if not paths or dataset.path in paths:
                    diff = working_copy.diff_db_to_tree(
                        dataset,
                        pk_filter=(paths.get(dataset.path) or None)
                    )[dataset]
                    w(dataset, diff)
        else:
            # commit<>commit diff
            target_rs = RepositoryStructure(repo, commit_target)
            all_datasets = {ds.path for ds in base_rs} | {ds.path for ds in target_rs}
            if paths:
                all_datasets = set(filter(lambda dsp: dsp in paths, all_datasets))

            for dataset_path in all_datasets:
                base_ds = base_rs.get(dataset_path)
                target_ds = target_rs.get(dataset_path)

                params = {}
                if not base_ds:
                    base_ds, target_ds = target_ds, base_ds
                    params['reverse'] = True

                diff = base_ds.diff(
                    target_ds,
                    pk_filter=(paths.get(dataset_path) or None),
                    **params
                )[base_ds]
                w(base_ds, diff)


@contextlib.contextmanager
def diff_output_text(*, fp, **kwargs):
    def _out(dataset, diff):
        path = dataset.path
        pk_field = dataset.primary_key
        prefix = f'{path}:'
        repr_excl = [pk_field]

        pecho = {}
        if fp:
            pecho['file'] = fp
            pecho['color'] = False

        for k, (v_old, v_new) in diff["META"].items():
            click.secho(f"--- {prefix}meta/{k}\n+++ {prefix}meta/{k}", bold=True, **pecho)

            s_old = set(v_old.items())
            s_new = set(v_new.items())

            diff_add = dict(s_new - s_old)
            diff_del = dict(s_old - s_new)
            all_keys = set(diff_del.keys()) | set(diff_add.keys())

            for k in all_keys:
                if k in diff_del:
                    click.secho(_repr_row({k: diff_del[k]}, prefix="- ", exclude=repr_excl), fg="red", **pecho)
                if k in diff_add:
                    click.secho(_repr_row({k: diff_add[k]}, prefix="+ ", exclude=repr_excl), fg="green", **pecho)

        prefix = f'{path}:{pk_field}='

        for k, v_old in diff["D"].items():
            click.secho(f"--- {prefix}{k}", bold=True, **pecho)
            click.secho(_repr_row(v_old, prefix="- ", exclude=repr_excl), fg="red", **pecho)

        for o in diff["I"]:
            click.secho(f"+++ {prefix}{o[pk_field]}", bold=True, **pecho)
            click.secho(_repr_row(o, prefix="+ ", exclude=repr_excl), fg="green", **pecho)

        for _, (v_old, v_new) in diff["U"].items():
            click.secho(f"--- {prefix}{v_old[pk_field]}\n+++ {prefix}{v_new[pk_field]}", bold=True, **pecho)

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
