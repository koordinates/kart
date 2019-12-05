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
    cls=MutexOption, exclusive_with=["html", "json", "geojson"]
)
@click.option(
    "--json",
    "output_format",
    flag_value="json",
    help="Get the diff in JSON format",
    hidden=True,
    cls=MutexOption, exclusive_with=["html", "text", "geojson"]
)
@click.option(
    "--geojson",
    "output_format",
    flag_value="geojson",
    help="Get the diff in GeoJSON format",
    cls=MutexOption, exclusive_with=["html", "text", "json"]
)
@click.option(
    "--html",
    "output_format",
    flag_value="html",
    help="View the diff in a browser",
    hidden=True,
    cls=MutexOption, exclusive_with=["json", "text", "geojson"]
)
@click.option(
    "--output",
    "output_path",
    help="Output to a specific file/directory instead of stdout.",
    type=click.Path(writable=True, allow_dash=True),
)
@click.argument('args', nargs=-1)
def diff(ctx, output_format, output_path, args):
    """
    Show changes between commits, commit and working tree, etc

    sno diff [options] [--] [<dataset>[:pk]...]
    sno diff [options] <commit> [--] [<dataset>[:pk]...]
    sno diff [options] <commit>..<commit> [--] [<dataset>[:pk]...]
    """
    from .working_copy import WorkingCopy
    from .structure import RepositoryStructure

    if output_path and output_path != '-':
        output_path = Path(output_path).expanduser()

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

    base_rs = RepositoryStructure(repo, commit_base)
    all_datasets = {ds.path for ds in base_rs}

    if commit_target is not None:
        target_rs = RepositoryStructure(repo, commit_target)
        all_datasets |= {ds.path for ds in target_rs}

    if paths:
        all_datasets = set(filter(lambda dsp: dsp in paths, all_datasets))

    diff_writer = globals()[f"diff_output_{output_format}"]
    writer_params = {
        'repo': repo,
        'commit_base': commit_base,
        'commit_target': commit_target,
        'output_path': output_path,
        'dataset_count': len(all_datasets),
    }

    with diff_writer(**writer_params) as w:
        if commit_target is None:
            # diff against working copy
            for dataset_path in all_datasets:
                dataset = base_rs.get(dataset_path)
                diff = working_copy.diff_db_to_tree(
                    dataset,
                    pk_filter=(paths.get(dataset.path) or None)
                )[dataset]
                w(dataset, diff)
        else:
            # commit<>commit diff
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
def diff_output_text(*, output_path, **kwargs):
    pecho = {}
    if output_path:
        if output_path == "-":
            pecho['file'] = sys.stdout
        elif output_path.is_dir():
            raise click.BadParameter("Directory is not valid for --output + --text", param_hint="--output")
        else:
            pecho['file'] = output_path.open('w')

        pecho['color'] = False

    def _out(dataset, diff):
        path = dataset.path
        pk_field = dataset.primary_key
        prefix = f'{path}:'
        repr_excl = [pk_field]

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


@contextlib.contextmanager
def diff_output_geojson(*, output_path, dataset_count, **kwargs):
    if dataset_count > 1:
        # output_path needs to be a directory
        if not output_path:
            raise click.BadParameter("Need to specify a directory via --output for --geojson with >1 dataset", param_hint="--output")
        elif output_path == "-" or output_path.is_file():
            raise click.BadParameter("A file is not valid for --output + --geojson with >1 dataset", param_hint="--output")

        if not output_path.exists():
            output_path.mkdir()
        else:
            for p in output_path.glob("*.geojson"):
                p.unlink()

    def _out(dataset, diff):
        json_params = {}
        if not output_path:
            fp = sys.stdout
            json_params = {'indent': 2}
        elif output_path == "-":
            fp = sys.stdout
        elif output_path.is_dir():
            fp = (output_path / f"{dataset.name}.geojson").open('w')
        else:
            fp = output_path.open('w')

        pk_field = dataset.primary_key

        fc = {
            "type": "FeatureCollection",
            "features": []
        }

        for k, (v_old, v_new) in diff["META"].items():
            click.secho(f"Warning: meta changes aren't included in GeoJSON output: {k}", fg='yellow', file=sys.stderr)

        for k, v_old in diff["D"].items():
            fc['features'].append(_json_row(v_old, 'D', pk_field))

        for o in diff["I"]:
            fc['features'].append(_json_row(o, 'I', pk_field))

        for _, (v_old, v_new) in diff["U"].items():
            fc['features'].append(_json_row(v_old, 'U-', pk_field))
            fc['features'].append(_json_row(v_new, 'U+', pk_field))

        json.dump(fc, fp, **json_params)

    yield _out


@contextlib.contextmanager
def diff_output_json(*, output_path, dataset_count, **kwargs):
    if isinstance(output_path, Path):
        if output_path.is_dir():
            raise click.BadParameter("Directory is not valid for --output + --json", param_hint="--output")

    json_params = {}

    if isinstance(output_path, io.IOBase):
        fp = output_path
    elif output_path == "-":
        fp = sys.stdout
    elif output_path:
        fp = output_path.open('w')
    else:
        fp = sys.stdout
        json_params['indent'] = 2

    accumulated = {}

    def _out(dataset, diff):
        pk_field = dataset.primary_key

        d = {'metaChanges': {}, 'featureChanges': []}
        for k, (v_old, v_new) in diff["META"].items():
            d['metaChanges'][k] = [v_old, v_new]

        for k, v_old in diff["D"].items():
            d['featureChanges'].append([
                _json_row(v_old, 'D', pk_field),
                None,
            ])

        for o in diff["I"]:
            d['featureChanges'].append([
                None,
                _json_row(o, 'I', pk_field),
            ])

        for _, (v_old, v_new) in diff["U"].items():
            d['featureChanges'].append([
                _json_row(v_old, 'U-', pk_field),
                _json_row(v_new, 'U+', pk_field),
            ])

        # sort for reproducibility
        d['featureChanges'].sort(key=lambda fc: (
            fc[0]['id'] if fc[0] is not None else '',
            fc[1]['id'] if fc[1] is not None else '',
        ))
        accumulated[dataset.path] = d

    yield _out

    json.dump({"sno.diff/v1": accumulated}, fp, **json_params)


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
            f['geometry'] = json.loads(g.ExportToJson())
        else:
            f['properties'][k] = v

    return f


@contextlib.contextmanager
def diff_output_html(*, output_path, repo, commit_base, commit_target, dataset_count, **kwargs):
    if output_path:
        if output_path.is_dir():
            raise click.BadParameter("Directory is not valid for --output + --html", param_hint="--output")

    json_data = io.StringIO()
    with diff_output_json(output_path=json_data, dataset_count=dataset_count) as json_writer:
        yield json_writer

    with open(Path(__file__).resolve().with_name('diff-view.html'), 'r', encoding='utf8') as ft:
        template = string.Template(ft.read())

    title = f"{Path(repo.path).name}: {commit_base.short_id} .. {commit_target.short_id if commit_target else 'working-copy'}"

    if output_path == "-":
        fo = sys.stdout
    elif output_path:
        fo = output_path.open('w')
    else:
        html_path = Path(repo.path) / 'DIFF.html'
        fo = html_path.open('w')

    with contextlib.closing(fo):
        fo.write(template.substitute({
            'title': title,
            'geojson_data': json_data.getvalue(),
        }))

    if not output_path:
        webbrowser.open_new(f'file://{html_path}')
