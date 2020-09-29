import itertools

import pygit2

from sno.diff_structs import RepoDiff, DatasetDiff, DeltaDiff, Delta
from sno.filter_util import UNFILTERED
from sno.repository_version import get_repo_version
from sno.schema import Schema
from sno.structure import RepositoryStructure


SNO_WORKINGCOPY_PATH = "sno.workingcopy.path"


class WorkingCopyDirty(Exception):
    pass


class WorkingCopy:
    VALID_VERSIONS = (1, 2)

    TRACKING_NAME = "track"
    STATE_NAME = "state"

    @property
    def TRACKING_TABLE(self):
        return self._sno_table(self.TRACKING_NAME)

    @property
    def STATE_TABLE(self):
        return self._sno_table(self.STATE_NAME)

    @classmethod
    def get(cls, repo, create_if_missing=False):
        from .gpkg import WorkingCopy_GPKG_1, WorkingCopy_GPKG_2
        from .postgis import WorkingCopy_Postgis

        if create_if_missing:
            cls.ensure_config_exists(repo)

        repo_cfg = repo.config
        path_key = SNO_WORKINGCOPY_PATH
        if path_key not in repo_cfg:
            return None

        path = repo_cfg[path_key]
        if path.startswith("postgresql://"):
            return WorkingCopy_Postgis(repo, path)

        full_path = repo.workdir_path / path
        if not full_path.is_file() and not create_if_missing:
            return None

        version = get_repo_version(repo)
        if version not in cls.VALID_VERSIONS:
            raise NotImplementedError(f"Working copy version: {version}")
        if version < 2:
            return WorkingCopy_GPKG_1(repo, path)
        else:
            return WorkingCopy_GPKG_2(repo, path)

    @classmethod
    def ensure_config_exists(cls, repo):
        repo_cfg = repo.config
        bare_key = repo.BARE_CONFIG_KEY
        is_bare = bare_key in repo_cfg and repo_cfg.get_bool(bare_key)
        if is_bare:
            return

        path_key = SNO_WORKINGCOPY_PATH
        path = repo_cfg[path_key] if path_key in repo_cfg else None
        if path is None:
            cls.write_config(repo, None, False)

    @classmethod
    def write_config(cls, repo, path=None, bare=False):
        repo_cfg = repo.config
        bare_key = repo.BARE_CONFIG_KEY
        path_key = SNO_WORKINGCOPY_PATH

        if bare:
            repo_cfg[bare_key] = True
            repo.del_config(path_key)
        else:
            path = path or cls.default_path(repo)
            repo_cfg[bare_key] = False
            repo_cfg[path_key] = str(path)

    @classmethod
    def default_path(cls, repo):
        """Returns `example.gpkg` for a sno repo in a directory named `example`."""
        stem = repo.workdir_path.stem
        return f"{stem}.gpkg"

    class Mismatch(ValueError):
        def __init__(self, working_copy_tree_id, match_tree_id):
            self.working_copy_tree_id = working_copy_tree_id
            self.match_tree_id = match_tree_id

        def __str__(self):
            return f"Working Copy is tree {self.working_copy_tree_id}; expecting {self.match_tree_id}"

    def _chunk(self, iterable, size):
        """Generator. Yield successive chunks from iterable of length <size>."""
        it = iter(iterable)
        while True:
            chunk = tuple(itertools.islice(it, size))
            if not chunk:
                return
            yield chunk

    def diff_to_tree(self, repo_filter=UNFILTERED, raise_if_dirty=False):
        """
        Generates a diff between a working copy DB and the underlying repository tree,
        for every dataset in the given repository structure.
        """
        repo_filter = repo_filter or UNFILTERED

        repo_diff = RepoDiff()
        for dataset in RepositoryStructure.lookup(self.repo, self.get_db_tree()):
            if dataset.path not in repo_filter:
                continue
            ds_diff = self.diff_db_to_tree(
                dataset,
                ds_filter=repo_filter[dataset.path],
                raise_if_dirty=raise_if_dirty,
            )
            repo_diff[dataset.path] = ds_diff
        repo_diff.prune()
        return repo_diff

    def diff_db_to_tree(self, dataset, ds_filter=None, raise_if_dirty=False):
        """
        Generates a diff between a working copy DB and the underlying repository tree,
        for a single dataset only.
        """
        ds_filter = ds_filter or UNFILTERED
        with self.session():
            ds_diff = DatasetDiff()
            ds_diff["meta"] = self.diff_db_to_tree_meta(dataset, raise_if_dirty)
            find_renames = self.can_find_renames(ds_diff["meta"])
            ds_diff["feature"] = self.diff_db_to_tree_feature(
                dataset, ds_filter.get("feature", ()), find_renames, raise_if_dirty
            )
        return ds_diff

    def diff_db_to_tree_meta(self, dataset, raise_if_dirty=False):
        """
        Returns a DeltaDiff showing all the changes of metadata between the dataset and this working copy.
        """
        meta_old = dict(dataset.meta_items())
        meta_new = dict(self.meta_items(dataset))
        if "schema.json" in meta_old and "schema.json" in meta_new:
            Schema.align_schema_cols(meta_old["schema.json"], meta_new["schema.json"])
        result = DeltaDiff.diff_dicts(meta_old, meta_new)
        if raise_if_dirty and result:
            raise WorkingCopyDirty()
        return result

    def diff_db_to_tree_feature(
        self, dataset, feature_filter, find_renames, raise_if_dirty=False
    ):
        pk_field = dataset.schema.pk_columns[0].name

        with self.session() as db:
            dbcur = db.cursor()
            self._execute_diff_query(dbcur, dataset, feature_filter)

            feature_diff = DeltaDiff()
            insert_count = delete_count = 0

            geom_col = dataset.geom_column_name

            for row in dbcur:
                track_pk = row[0]  # This is always a str
                db_obj = {k: row[k] for k in row.keys() if k != ".__track_pk"}

                if db_obj[pk_field] is None:
                    db_obj = None

                if db_obj is not None and geom_col is not None:
                    db_obj[geom_col] = self._db_geom_to_gpkg_geom(db_obj[geom_col])

                try:
                    repo_obj = dataset.get_feature(track_pk)
                except KeyError:
                    repo_obj = None

                if repo_obj == db_obj:
                    # DB was changed and then changed back - eg INSERT then DELETE.
                    # TODO - maybe delete track_pk from tracking table?
                    continue

                if raise_if_dirty:
                    raise WorkingCopyDirty()

                if db_obj and not repo_obj:  # INSERT
                    insert_count += 1
                    feature_diff.add_delta(Delta.insert((db_obj[pk_field], db_obj)))

                elif repo_obj and not db_obj:  # DELETE
                    delete_count += 1
                    feature_diff.add_delta(Delta.delete((repo_obj[pk_field], repo_obj)))

                else:  # UPDATE
                    pk = db_obj[pk_field]
                    feature_diff.add_delta(Delta.update((pk, repo_obj), (pk, db_obj)))

        if find_renames and (insert_count + delete_count) <= 400:
            self.find_renames(feature_diff, dataset)

        return feature_diff

    def _execute_diff_query(self, dbcur, dataset, feature_filter):
        """
        Does a join on the tracking table and the table for the given dataset, such that the dbcursor's result
        is all the rows that have been inserted / updated / deleted.
        """
        raise NotImplementedError()

    def _db_geom_to_gpkg_geom(self, g):
        """Convert a geometry as returned by the database to a sno geometry.Geometry object."""
        raise NotImplementedError()

    def can_find_renames(self, meta_diff):
        """Can we find a renamed (aka moved) feature? There's no point looking for renames if the schema has changed."""
        if "schema.json" not in meta_diff:
            return True

        schema_delta = meta_diff["schema.json"]
        if not schema_delta.old_value or not schema_delta.new_value:
            return False

        old_schema = Schema.from_column_dicts(schema_delta.old_value)
        new_schema = Schema.from_column_dicts(schema_delta.new_value)
        dt = old_schema.diff_type_counts(new_schema)
        # We could still recognise a renamed feature in the case of type updates (eg int32 -> int64),
        # but basically any other type of schema modification means there's no point looking for renames.
        dt.pop("type_updates")
        return sum(dt.values()) == 0

    def find_renames(self, feature_diff, dataset):
        """
        Matches inserts + deletes into renames on a best effort basis.
        changes at most one matching insert and delete into an update per blob-hash.
        Modifies feature_diff in place.
        """

        def hash_feature(feature):
            return pygit2.hash(dataset.encode_feature_blob(feature)).hex

        inserts = {}
        deletes = {}

        for delta in feature_diff.values():
            if delta.type == "insert":
                inserts[hash_feature(delta.new_value)] = delta
            elif delta.type == "delete":
                deletes[hash_feature(delta.old_value)] = delta

        for h in deletes:
            if h in inserts:
                delete_delta = deletes[h]
                insert_delta = inserts[h]

                del feature_diff[delete_delta.key]
                del feature_diff[insert_delta.key]
                update_delta = delete_delta + insert_delta
                feature_diff.add_delta(update_delta)
