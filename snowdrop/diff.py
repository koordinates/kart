import copy
import logging

import click
import pygit2

from . import gpkg


L = logging.getLogger('snowdrop.diff')


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


@click.command()
@click.pass_context
def diff(ctx):
    """ Show changes between commits, commit and working tree, etc """
    from .working_copy import WorkingCopy
    from .structure import RepositoryStructure

    repo_dir = ctx.obj["repo_dir"]
    repo = pygit2.Repository(repo_dir)
    if not repo:
        raise click.BadParameter("Not an existing repository", param_hint="--repo")

    working_copy = WorkingCopy.open(repo)
    if not working_copy:
        raise click.UsageError("No working copy, use 'checkout'")

    for dataset in RepositoryStructure(repo):
        working_copy.assert_db_tree_match(repo.head.peel(pygit2.Tree))

        path = dataset.path
        pk_field = dataset.primary_key

        diff = working_copy.diff_db_to_tree(dataset)[dataset]

        prefix = f'{path}:'
        repr_excl = [pk_field]

        for k, (v_old, v_new) in diff["META"].items():
            click.secho(f"--- {prefix}meta/{k}\n+++ {prefix}meta/{k}", bold=True)

            s_old = set(v_old.items())
            s_new = set(v_new.items())

            diff_add = dict(s_new - s_old)
            diff_del = dict(s_old - s_new)
            all_keys = set(diff_del.keys()) | set(diff_add.keys())

            for k in all_keys:
                if k in diff_del:
                    click.secho(_repr_row({k: diff_del[k]}, prefix="- ", exclude=repr_excl), fg="red")
                if k in diff_add:
                    click.secho(_repr_row({k: diff_add[k]}, prefix="+ ", exclude=repr_excl), fg="green")

        prefix = f'{path}:{pk_field}='

        for k, v_old in diff["D"].items():
            click.secho(f"--- {prefix}{k}", bold=True)
            click.secho(_repr_row(v_old, prefix="- ", exclude=repr_excl), fg="red")

        for o in diff["I"]:
            click.secho(f"+++ {prefix}{o[pk_field]}", bold=True)
            click.secho(_repr_row(o, prefix="+ ", exclude=repr_excl), fg="green")

        for _, (v_old, v_new) in diff["U"].items():
            click.secho(f"--- {prefix}{v_old[pk_field]}\n+++ {prefix}{v_new[pk_field]}", bold=True)

            s_old = set(v_old.items())
            s_new = set(v_new.items())

            diff_add = dict(s_new - s_old)
            diff_del = dict(s_old - s_new)
            all_keys = sorted(set(diff_del.keys()) | set(diff_add.keys()))

            for k in all_keys:
                if k in diff_del:
                    rk = _repr_row({k: diff_del[k]}, prefix="- ", exclude=repr_excl)
                    if rk:
                        click.secho(rk, fg="red")
                if k in diff_add:
                    rk = _repr_row({k: diff_add[k]}, prefix="+ ", exclude=repr_excl)
                    if rk:
                        click.secho(rk, fg="green")


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
