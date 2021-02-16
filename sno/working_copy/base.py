import contextlib
import functools
import itertools
import logging
import time
from enum import Enum, auto


import click
import pygit2
import sqlalchemy

from sno.base_dataset import BaseDataset
from sno.diff_structs import RepoDiff, DatasetDiff, DeltaDiff, Delta
from sno.exceptions import (
    InvalidOperation,
    NotYetImplemented,
    NotFound,
    NO_WORKING_COPY,
)
from sno.filter_util import UNFILTERED
from sno.schema import Schema, DefaultRoundtripContext


L = logging.getLogger("sno.working_copy.base")


class WorkingCopyType(Enum):
    """Different types of working copy currently supported by Sno."""

    GPKG = auto()
    POSTGIS = auto()
    SQL_SERVER = auto()

    @classmethod
    def from_path(cls, path, allow_invalid=False):
        path = str(path)
        if path.startswith("postgresql:"):
            return WorkingCopyType.POSTGIS
        elif path.startswith("mssql:"):
            return WorkingCopyType.SQL_SERVER
        elif path.lower().endswith(".gpkg"):
            return WorkingCopyType.GPKG
        elif allow_invalid:
            return None
        else:
            raise click.UsageError(
                f"Unrecognised working copy type: {path}\n"
                "Try one of:\n"
                "  PATH.gpkg\n"
                "  postgresql://[HOST]/DBNAME/SCHEMA\n"
            )

    @property
    def class_(self):
        if self is WorkingCopyType.GPKG:
            from .gpkg import WorkingCopy_GPKG

            return WorkingCopy_GPKG
        elif self is WorkingCopyType.POSTGIS:
            from .postgis import WorkingCopy_Postgis

            return WorkingCopy_Postgis
        elif self is WorkingCopyType.SQL_SERVER:
            from .sqlserver import WorkingCopy_SqlServer

            return WorkingCopy_SqlServer
        raise RuntimeError("Invalid WorkingCopyType")


class WorkingCopyDirty(Exception):
    """Exception to abort immediately if working copy is dirty."""

    pass


class Mismatch(ValueError):
    """Error for if the tree id stored in state table doesn't match the one at HEAD."""

    def __init__(self, working_copy_tree_id, expected_tree_id):
        self.working_copy_tree_id = working_copy_tree_id
        self.expected_tree_id = expected_tree_id

    def __str__(self):
        return f"Working Copy is tree {self.working_copy_tree_id}; expecting {self.expected_tree_id}"


class WorkingCopy:
    """
    Abstract working copy implementation.
    Subclasses to override any unimplemented methods below, and also to set the following fields:

    self.repo - SnoRepo containing this WorkingCopy
    self.path - string describing the location of this WorkingCopy
    self.engine - sqlalchemy engine for connecting to the database
    self.sessionmaker - sqlalchemy sessionmaker bound to the engine
    self.preparer - sqlalchemy IdentifierPreparer for quoting SQL in the appropriate dialect

    self.db_schema - database-schema that this working copy controls, if any.
    self.sno_tables - sqlalchemy Table definitions for sno_track and sno_state tables.
    """

    SNO_WORKINGCOPY_PATH = "sno.workingcopy.path"

    @property
    @functools.lru_cache(maxsize=1)
    def DB_SCHEMA(self):
        """Escaped, dialect-specific name of the database-schema owned by this working copy (if any)."""
        if self.db_schema is None:
            raise RuntimeError("No schema to escape.")
        return self.preparer.format_schema(self.db_schema)

    @property
    @functools.lru_cache(maxsize=1)
    def SNO_TRACK(self):
        """Escaped, dialect-specific fully-qualified name of sno_track table."""
        return self.table_identifier(self.sno_tables.sno_track)

    @property
    @functools.lru_cache(maxsize=1)
    def SNO_STATE(self):
        """Escaped, dialect-specific fully-qualified name of sno_state table."""
        return self.table_identifier(self.sno_tables.sno_state)

    @property
    @functools.lru_cache(maxsize=1)
    def SNO_TRACK_NAME(self):
        """The table name of sno_track table, not including the schema."""
        return self.sno_tables.sno_track.name

    @property
    @functools.lru_cache(maxsize=1)
    def SNO_STATE_NAME(self):
        """The table name of sno_track table, not including the schema."""
        return self.sno_tables.sno_state.name

    def quote(self, ident):
        """Conditionally quote an identifier - eg if it is a reserved word or contains special characters."""
        return self.preparer.quote(ident)

    @functools.lru_cache()
    def table_identifier(self, dataset_or_table):
        """Given a dataset, tablename, or sqlalchemy table, escapes its fullname for use in a SQL statement."""
        table = (
            dataset_or_table.table_name
            if isinstance(dataset_or_table, BaseDataset)
            else dataset_or_table
        )
        sqlalchemy_table = (
            sqlalchemy.table(table, schema=self.db_schema)
            if isinstance(table, str)
            else table
        )
        return self.preparer.format_table(sqlalchemy_table)

    def _table_def_for_column_schema(self, col, dataset):
        # This is just used for selects/inserts/updates - we don't need the full type information.
        # We only need type information if some automatic conversion needs to happen on read or write.
        # TODO: return the full type information so we can also use it for CREATE TABLE.
        return sqlalchemy.column(col.name)

    @functools.lru_cache()
    def _table_def_for_dataset(self, dataset, schema=None):
        """
        A minimal table definition for a dataset.
        Specifies table and column names only - the minimum required such that an insert() or update() will work.
        """
        schema = schema or dataset.schema
        return sqlalchemy.table(
            dataset.table_name,
            *[self._table_def_for_column_schema(c, dataset) for c in schema],
            schema=self.db_schema,
        )

    def _insert_into_dataset(self, dataset):
        """Returns a SQL command for inserting features into the table for that dataset."""
        return self._table_def_for_dataset(dataset).insert()

    def _insert_or_replace_into_dataset(self, dataset):
        """
        Returns a SQL command for inserting/replacing features that may or may not already exist in the table
        for that dataset.
        """
        # Its not possible to do this in a DB agnostic way.
        raise NotImplementedError()

    @classmethod
    def get(cls, repo, *, allow_uncreated=False, allow_invalid_state=False):
        """
        Get the working copy associated with this sno repo, as specified in the repo config.
        Note that the working copy specified in the repo config may or may not exist or be in a valid state.
        An instance of this class can represent a working copy that doesn't exist or is in an invalid state,
        (similar to how pathlib.Path can point to files that may or may not exist).
        If allow_uncreated is True, a non-existant working copy may be returned - otherwise, only an existing
        working copy will be returned, and None will be returned if no working copy is found.
        If allow_invalid_state is True, an invalid-state working copy may be returned - otherwise, only a valid
        working copy will be returned, and a NotFound(NO_WORKING_COPY) will be raised if it is in an invalid state.
        """
        repo_cfg = repo.config
        path_key = cls.SNO_WORKINGCOPY_PATH
        if path_key not in repo_cfg:
            return None

        path = repo_cfg[path_key]
        return cls.get_at_path(
            repo,
            path,
            allow_uncreated=allow_uncreated,
            allow_invalid_state=allow_invalid_state,
        )

    @classmethod
    def get_at_path(
        cls, repo, path, *, allow_uncreated=False, allow_invalid_state=False
    ):
        if not path:
            return None

        wc_type = WorkingCopyType.from_path(path, allow_invalid=allow_invalid_state)
        if not wc_type:
            return None
        wc = wc_type.class_(repo, path)

        if not allow_invalid_state:
            wc.check_valid_state()

        if not wc.is_created() and not allow_uncreated:
            wc = None

        return wc

    @classmethod
    def ensure_config_exists(cls, repo):
        repo_cfg = repo.config
        bare_key = repo.BARE_CONFIG_KEY
        is_bare = bare_key in repo_cfg and repo_cfg.get_bool(bare_key)
        if is_bare:
            return

        path_key = cls.SNO_WORKINGCOPY_PATH
        path = repo_cfg[path_key] if path_key in repo_cfg else None
        if path is None:
            cls.write_config(repo, None, False)

    @classmethod
    def write_config(cls, repo, path=None, bare=False):
        repo_cfg = repo.config
        bare_key = repo.BARE_CONFIG_KEY
        path_key = cls.SNO_WORKINGCOPY_PATH

        if bare:
            repo_cfg[bare_key] = True
            repo.del_config(path_key)
        else:
            if path is None:
                path = cls.default_path(repo.workdir_path)
            else:
                path = cls.normalise_path(repo, path)

            repo_cfg[bare_key] = False
            repo_cfg[path_key] = str(path)

    @classmethod
    def check_valid_creation_path(cls, workdir_path, wc_path):
        """
        Given a user-supplied string describing where to put the working copy, ensures it is a valid location,
        and nothing already exists there that prevents us from creating it. Raises InvalidOperation if it is not.
        Doesn't check if we have permissions to create a working copy there.
        """
        if not wc_path:
            wc_path = cls.default_path(workdir_path)
        WorkingCopyType.from_path(wc_path).class_.check_valid_creation_path(
            workdir_path, wc_path
        )

    @classmethod
    def check_valid_path(cls, workdir_path, wc_path):
        """
        Given a user-supplied string describing where to put the working copy, ensures it is a valid location,
        and nothing already exists there that prevents us from creating it. Raises InvalidOperation if it is not.
        Doesn't check if we have permissions to create a working copy there.
        """
        WorkingCopyType.from_path(wc_path).class_.check_valid_path(
            workdir_path, wc_path
        )

    def check_valid_state(self):
        if self.is_created():
            if not self.is_initialised():
                message = [
                    f"Working copy at {self.path} is not yet fully initialised",
                    "Try `sno create-workingcopy` to delete and recreate working copy if problem persists",
                ]
                if self.has_data():
                    message.append(
                        f"But beware: {self.path} already seems to contain data, make sure it is backed up"
                    )
                raise NotFound("\n".join(message), NO_WORKING_COPY)

    @classmethod
    def default_path(cls, workdir_path):
        """Returns `example.gpkg` for a sno repo in a directory named `example`."""
        stem = workdir_path.stem
        return f"{stem}.gpkg"

    @classmethod
    def normalise_path(cls, repo, path):
        """If the path is in a non-standard form, normalise it to the equivalent standard form."""
        return WorkingCopyType.from_path(path).class_.normalise_path(repo, path)

    @contextlib.contextmanager
    def session(self, bulk=0):
        """
        Context manager for DB sessions, yields a session object inside a transaction
        Calling again yields the _same_ session, the transaction/etc only happen in the outer one.

        @bulk controls bulk-loading operating mode - subject to change. See ./gpkg.py
        """
        raise NotImplementedError()

    def is_created(self):
        """
        Returns true if the location specified by self.path alreaddy exists.
        Note that it might not be initialised as a working copy - see self.is_initialised.
        """
        raise NotImplementedError()

    def is_initialised(self):
        """
        Returns true if the working copy is initialised -
        it exists and has the necessary sno tables - sno_state and sno_track.
        """
        raise NotImplementedError()

    def has_data(self):
        """Returns true if the working copy seems to have user-created content already."""
        raise NotImplementedError()

    def create_and_initialise(self):
        """Create the database container or database schema if required, and the sno tables."""
        raise NotImplementedError()

    def delete(self, keep_db_schema_if_possible=False):
        """
        Delete the entire working copy.

        keep_db_schema_if_possible - set to True if the WC is being recreated in the same location
        in a moment's time. It's possible we lack permission to recreate the db schema, so in this
        case, better not to delete it.
        """
        raise NotImplementedError()

    def get_db_tree(self):
        """Returns the hex tree ID from the state table."""
        with self.session() as sess:
            return sess.scalar(
                f"""SELECT value FROM {self.SNO_STATE} WHERE "table_name"='*' AND "key"='tree';""",
            )

    def assert_db_tree_match(self, tree):
        """Raises a Mismatch if sno_state refers to a different tree and not the given tree."""
        wc_tree_id = self.get_db_tree()
        expected_tree_id = tree.id.hex if isinstance(tree, pygit2.Tree) else tree

        if wc_tree_id != expected_tree_id:
            raise Mismatch(wc_tree_id, expected_tree_id)
        return wc_tree_id

    def tracking_changes_count(self, dataset=None):
        """
        Returns the total number of changes tracked in sno_track,
        or the number of changes tracked for the given dataset.
        """
        with self.session() as sess:
            if dataset is not None:
                return sess.scalar(
                    f"SELECT COUNT(*) FROM {self.SNO_TRACK} WHERE table_name=:table_name;",
                    {"table_name": dataset.table_name},
                )
            else:
                return sess.scalar(f"SELECT COUNT(*) FROM {self.SNO_TRACK};")

    def _chunk(self, iterable, size):
        """Generator. Yield successive chunks from iterable of length <size>."""
        it = iter(iterable)
        while True:
            chunk = tuple(itertools.islice(it, size))
            if not chunk:
                return
            yield chunk

    def check_not_dirty(self, help_message=None):
        """Checks the working copy has no changes in it. Otherwise, raises InvalidOperation"""
        if not help_message:
            help_message = "Commit these changes (`sno commit`) or discard these changes (`sno reset`) first."
        if self.is_dirty():
            raise InvalidOperation(
                f"You have uncommitted changes in your working copy.\n{help_message}"
            )

    def is_dirty(self):
        """
        Returns True if there are uncommitted changes in the working copy,
        or False otherwise.
        """
        if not self.is_initialised() or self.get_db_tree() is None:
            return False
        try:
            self.diff_to_tree(raise_if_dirty=True)
            return False
        except WorkingCopyDirty:
            return True

    def diff_to_tree(self, repo_filter=UNFILTERED, raise_if_dirty=False):
        """
        Generates a diff between a working copy DB and the underlying repository tree,
        for every dataset in the given repository structure.
        """
        repo_filter = repo_filter or UNFILTERED

        with self.session():
            repo_diff = RepoDiff()
            for dataset in self.repo.datasets(self.get_db_tree()):
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
            meta_diff = self.diff_db_to_tree_meta(dataset, raise_if_dirty)
            feature_diff = self.diff_db_to_tree_feature(
                dataset, ds_filter.get("feature", ()), meta_diff, raise_if_dirty
            )

        ds_diff = DatasetDiff()
        ds_diff["meta"] = meta_diff
        ds_diff["feature"] = feature_diff
        return ds_diff

    def diff_db_to_tree_meta(self, dataset, raise_if_dirty=False):
        """
        Returns a DeltaDiff showing all the changes of metadata between the dataset and this working copy.
        """
        ds_meta_items = dict(dataset.meta_items())
        wc_meta_items = dict(self.meta_items(dataset))
        self._remove_hidden_meta_diffs(dataset, ds_meta_items, wc_meta_items)
        result = DeltaDiff.diff_dicts(ds_meta_items, wc_meta_items)
        if raise_if_dirty and result:
            raise WorkingCopyDirty()
        return result

    # Subclasses should override this function if there are certain types they cannot represent perfectly.
    @classmethod
    def try_align_schema_col(cls, old_col_dict, new_col_dict):
        return DefaultRoundtripContext.try_align_schema_col(old_col_dict, new_col_dict)

    def _remove_hidden_meta_diffs(self, dataset, ds_meta_items, wc_meta_items):
        """
        Remove any meta diffs that can't or shouldn't be committed, and so shouldn't be shown to the user.
        For all WC's, this means re-adding the column-IDs to schema.json since no WC can store column IDs.
        Subclasses can override and make more changes, depending on the WC's limitations - for instance, if the WC
        can't store the dataset description, then that should be removed from the diff.
        """

        # A dataset should have at most ONE of "metadata.xml" or "metadata/dataset.json".
        # The XML file is newer and supercedes the JSON file.
        # The GPKG adapter generates both, so we delete one so as to match the dataset.
        try:
            if "metadata/dataset.json" in ds_meta_items:
                del wc_meta_items["metadata.xml"]
            else:
                del wc_meta_items["metadata/dataset.json"]
        except KeyError:
            pass

        if "schema.json" in ds_meta_items and "schema.json" in wc_meta_items:
            ds_schema = ds_meta_items["schema.json"]
            wc_schema = wc_meta_items["schema.json"]
            Schema.align_schema_cols(ds_schema, wc_schema, roundtrip_ctx=self)

    def meta_items(self, dataset):
        """Returns all the meta items for the given dataset from the working copy DB."""
        raise NotImplementedError()

    def diff_db_to_tree_feature(
        self, dataset, feature_filter, meta_diff, raise_if_dirty=False
    ):
        pk_field = dataset.schema.pk_columns[0].name
        find_renames = self.can_find_renames(meta_diff)

        with self.session() as sess:
            r = self._execute_dirty_rows_query(sess, dataset, feature_filter, meta_diff)

            feature_diff = DeltaDiff()
            insert_count = delete_count = 0

            for row in r:
                track_pk = row[0]  # This is always a str
                db_obj = {k: row[k] for k in row.keys() if k != ".__track_pk"}

                if db_obj[pk_field] is None:
                    db_obj = None

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

    def _execute_dirty_rows_query(
        self, sess, dataset, feature_filter=None, meta_diff=None
    ):
        """
        Does a join on the tracking table and the table for the given dataset, and returns a result
        containing all the rows that have been inserted / updated / deleted.
        """

        feature_filter = feature_filter or UNFILTERED

        if (
            meta_diff
            and "schema.json" in meta_diff
            and meta_diff["schema.json"].new_value
        ):
            schema = Schema.from_column_dicts(meta_diff["schema.json"].new_value)
        else:
            schema = dataset.schema

        sno_track = self.sno_tables.sno_track
        table = self._table_def_for_dataset(dataset, schema=schema)

        cols_to_select = [sno_track.c.pk.label(".__track_pk"), *table.columns]
        pk_column = table.columns[schema.pk_columns[0].name]
        tracking_col_type = sno_track.c.pk.type

        base_query = sqlalchemy.select(columns=cols_to_select).select_from(
            sno_track.outerjoin(
                table,
                sno_track.c.pk == sqlalchemy.cast(pk_column, tracking_col_type),
            )
        )

        if feature_filter is UNFILTERED:
            query = base_query.where(sno_track.c.table_name == dataset.table_name)
        else:
            pks = list(feature_filter)
            query = base_query.where(
                sqlalchemy.and_(
                    sno_track.c.table_name == dataset.table_name,
                    sno_track.c.pk.in_(pks),
                )
            )

        return sess.execute(query)

    def reset_tracking_table(self, reset_filter=UNFILTERED):
        """Delete the rows from the tracking table that match the given filter."""
        reset_filter = reset_filter or UNFILTERED

        with self.session() as sess:
            if reset_filter == UNFILTERED:
                sess.execute(f"DELETE FROM {self.SNO_TRACK};")
                return

            for dataset_path, dataset_filter in reset_filter.items():
                table_name = dataset_path.strip("/").replace("/", "__")
                if (
                    dataset_filter == UNFILTERED
                    or dataset_filter.get("feature") == UNFILTERED
                ):
                    sess.execute(
                        f"DELETE FROM {self.SNO_TRACK} WHERE table_name=:table_name;",
                        {"table_name": table_name},
                    )
                    continue

                pks = list(dataset_filter.get("feature", []))
                t = sqlalchemy.text(
                    f"DELETE FROM {self.SNO_TRACK} WHERE table_name=:table_name AND pk IN :pks;"
                )
                t = t.bindparams(sqlalchemy.bindparam("pks", expanding=True))
                sess.execute(t, {"table_name": table_name, "pks": pks})

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

        schema = dataset.schema
        inserts = {}
        deletes = {}

        for delta in feature_diff.values():
            if delta.type == "insert":
                inserts[schema.hash_feature(delta.new_value, without_pk=True)] = delta
            elif delta.type == "delete":
                deletes[schema.hash_feature(delta.old_value, without_pk=True)] = delta

        for h in deletes:
            if h in inserts:
                delete_delta = deletes[h]
                insert_delta = inserts[h]

                del feature_diff[delete_delta.key]
                del feature_diff[insert_delta.key]
                update_delta = delete_delta + insert_delta
                feature_diff.add_delta(update_delta)

    def update_state_table_tree(self, tree):
        """Write the given tree to the state table."""
        tree_id = tree.id.hex if isinstance(tree, pygit2.Tree) else tree
        L.info(f"Tree sha: {tree_id}")
        with self.session() as sess:
            changes = self._insert_or_replace_state_table_tree(sess, tree_id)
        assert changes == 1, f"{self.SNO_STATE} update: expected 1Δ, got {changes}"

    def _insert_or_replace_state_table_tree(self, sess, tree_id):
        """
        Write the given tree ID to the state table.

        sess - sqlalchemy session.
        tree_id - str, the hex SHA of the tree at HEAD.
        """
        r = sess.execute(
            f"""
            INSERT INTO {self.SNO_STATE} ("table_name", "key", "value") VALUES ('*', 'tree', :value)
            ON CONFLICT ("table_name", "key") DO UPDATE SET value=EXCLUDED.value;
            """,
            {"value": tree_id},
        )
        return r.rowcount

    def write_full(self, commit, *datasets, **kwargs):
        """
        Writes a full layer into a working-copy table

        Use for new working-copy checkouts.
        """
        L = logging.getLogger(f"{self.__class__.__qualname__}.write_full")

        with self.session(bulk=2) as sess:

            for dataset in datasets:
                # Create the table
                self._create_table_for_dataset(sess, dataset)
                self._write_meta(sess, dataset)

                if dataset.has_geometry:
                    # This should be called while the table is still empty.
                    self._create_spatial_index(sess, dataset)

                L.info("Creating features...")
                sql = self._insert_into_dataset(dataset)
                feat_progress = 0
                t0 = time.monotonic()
                t0p = t0

                CHUNK_SIZE = 10000
                total_features = dataset.feature_count

                for row_dicts in self._chunk(
                    dataset.features_with_crs_ids(), CHUNK_SIZE
                ):
                    sess.execute(sql, row_dicts)
                    feat_progress += len(row_dicts)

                    t0a = time.monotonic()
                    L.info(
                        "%.1f%% %d/%d features... @%.1fs (+%.1fs, ~%d F/s)",
                        feat_progress / total_features * 100,
                        feat_progress,
                        total_features,
                        t0a - t0,
                        t0a - t0p,
                        CHUNK_SIZE / (t0a - t0p or 0.001),
                    )
                    t0p = t0a

                t1 = time.monotonic()
                L.info(
                    "Added %d features to working copy in %.1fs", feat_progress, t1 - t0
                )
                L.info(
                    "Overall rate: %d features/s", (feat_progress / (t1 - t0 or 0.001))
                )

                self._create_triggers(sess, dataset)
                self._update_last_write_time(sess, dataset, commit)

            self._insert_or_replace_state_table_tree(
                sess, commit.peel(pygit2.Tree).id.hex
            )

    def _create_table_for_dataset(self, sess, dataset):
        """Create the working-copy table for checking out the given dataset."""
        raise NotImplementedError

    def _write_meta(self, sess, dataset):
        """Write any non-feature data relating to dataset - title, description, CRS, etc."""
        raise NotImplementedError()

    def _create_spatial_index(self, sess, dataset):
        """
        Creates a spatial index for the table for the given dataset.
        The spatial index is configured so that it is automatically updated when the table is modified.
        It is not guaranteed that the spatial index will take into account features that are already present
        in the table when this function is called - therefore, this should be called while the table is still empty.
        """
        raise NotImplementedError()

    def _drop_spatial_index(self, sess, dataset):
        """Inverse of _create_spatial_index - deletes the spatial index."""
        raise NotImplementedError()

    def _update_last_write_time(self, sess, dataset, commit=None):
        """Hook for updating the last-modified timestamp stored for a particular dataset, if there is one."""
        pass

    def _write_features(self, sess, dataset, pk_list, *, ignore_missing=False):
        """Write the features from the dataset with the given PKs to the table for the dataset."""
        if not pk_list:
            return 0

        sql = self._insert_or_replace_into_dataset(dataset)
        feat_count = 0
        CHUNK_SIZE = 10000
        for row_dicts in self._chunk(
            dataset.get_features_with_crs_ids(pk_list, ignore_missing=ignore_missing),
            CHUNK_SIZE,
        ):
            sess.execute(sql, row_dicts)
            feat_count += len(row_dicts)

        return feat_count

    def _delete_features(self, sess, dataset, pk_list):
        """Delete all of the features with the given PKs in the table for the dataset."""
        if not pk_list:
            return 0

        pk_column = self.preparer.quote(dataset.primary_key)
        sql = f"""DELETE FROM {self.table_identifier(dataset)} WHERE {pk_column} IN :pks;"""
        stmt = sqlalchemy.text(sql).bindparams(
            sqlalchemy.bindparam("pks", expanding=True)
        )
        feat_count = 0
        CHUNK_SIZE = 100
        for pks in self._chunk(pk_list, CHUNK_SIZE):
            r = sess.execute(stmt, {"pks": pks})
            feat_count += r.rowcount

        return feat_count

    def drop_table(self, target_tree_or_commit, *datasets):
        """Drop the tables for all the given datasets."""
        with self.session() as sess:
            for dataset in datasets:
                if dataset.has_geometry:
                    self._drop_spatial_index(sess, dataset)

                sess.execute(f"DROP TABLE IF EXISTS {self.table_identifier(dataset)};")
                self.delete_meta(dataset)

                sess.execute(
                    f"DELETE FROM {self.SNO_TRACK} WHERE table_name=:table_name;",
                    {"table_name": dataset.table_name},
                )

    def reset(
        self,
        target_tree_or_commit,
        *,
        force=False,
        paths=None,
        track_changes_as_dirty=False,
    ):
        """
        Resets the working copy to the given target-tree (or the tree pointed to by the given target-commit).

        If there are uncommitted changes, raises InvalidOperation, unless force=True is given
        (in which case the changes are discarded)

        If track_changes_as_dirty=False (the default) the tree ID in the sno-state table gets set to the
        new tree ID and the tracking table is left empty. If it is True, the old tree ID is kept and the
        tracking table is used to record all the changes, so that they can be committed later.
        """
        if not force:
            self.check_not_dirty()

        L = logging.getLogger(f"{self.__class__.__qualname__}.reset")
        commit = None
        if isinstance(target_tree_or_commit, pygit2.Commit):
            commit = target_tree_or_commit
            target_tree = commit.tree
        else:
            commit = None
            target_tree = target_tree_or_commit
        target_tree_id = target_tree.id.hex

        # base_tree is the tree the working copy is based on.
        # If the working copy exactly matches base_tree, it is clean and has an empty tracking table.
        base_tree_id = self.get_db_tree()
        base_tree = self.repo[base_tree_id]
        repo_tree_id = self.repo.head_tree.hex

        L.debug(
            "reset(): WorkingCopy base_tree:%s, Repo HEAD has tree:%s. Resetting working copy to tree: %s",
            base_tree_id,
            repo_tree_id,
            target_tree_id,
        )
        L.debug(
            f"reset(): commit={commit.id if commit else 'none'} track_changes_as_dirty={track_changes_as_dirty}",
        )

        base_datasets = {
            ds.table_name: ds
            for ds in self._filter_by_paths(self.repo.datasets(base_tree), paths)
        }
        if base_tree == target_tree:
            target_datasets = base_datasets
        else:
            target_datasets = {
                ds.table_name: ds
                for ds in self._filter_by_paths(self.repo.datasets(target_tree), paths)
            }

        table_inserts = target_datasets.keys() - base_datasets.keys()
        table_deletes = base_datasets.keys() - target_datasets.keys()
        table_updates = base_datasets.keys() & target_datasets.keys()
        table_updates_unsupported = set()

        for table in table_updates:
            base_ds = base_datasets[table]
            ds_version = base_ds.VERSION

            # Do we support changing the WC metadata to back to base_ds metadata?
            rev_wc_meta_diff = self.diff_db_to_tree_meta(base_ds)
            update_supported = self._is_meta_update_supported(
                ds_version, rev_wc_meta_diff
            )

            # And, do we support then changing it from base_ds metadata to target_ds metadata?
            target_ds = target_datasets[table]
            if target_ds != base_ds:
                rev_rev_meta_diff = base_ds.diff_meta(target_ds)
                update_supported = update_supported and self._is_meta_update_supported(
                    ds_version, rev_rev_meta_diff
                )

            if not update_supported:
                table_updates_unsupported.add(table)

        for table in table_updates_unsupported:
            table_updates.remove(table)
            table_inserts.add(table)
            table_deletes.add(table)

        L.debug(
            "reset(): table_inserts: %s, table_deletes: %s, table_updates %s",
            table_inserts,
            table_deletes,
            table_updates,
        )

        structural_changes = table_inserts | table_deletes
        if track_changes_as_dirty and structural_changes:
            # We don't yet support tracking changes as dirty if we delete, create, or rewrite an entire table.
            structural_changes_text = "\n".join(structural_changes)
            raise NotYetImplemented(
                "Sorry, this operation is not possible when there are structural changes."
                f"Structural changes are affecting:\n{structural_changes_text}"
            )

        with self.session(bulk=1) as sess:
            # Delete old tables
            if table_deletes:
                self.drop_table(
                    target_tree_or_commit, *[base_datasets[d] for d in table_deletes]
                )
            # Write new tables
            if table_inserts:
                self.write_full(
                    target_tree_or_commit, *[target_datasets[d] for d in table_inserts]
                )

            # Update tables that can be updated in place.
            for table in table_updates:
                base_ds = base_datasets[table]
                target_ds = target_datasets[table]
                self._update_table(
                    sess,
                    base_ds,
                    target_ds,
                    commit,
                    track_changes_as_dirty=track_changes_as_dirty,
                )

            if not track_changes_as_dirty:
                # update the tree id
                self._insert_or_replace_state_table_tree(sess, target_tree_id)

    def _filter_by_paths(self, datasets, paths):
        """Filters the datasets so that only those matching the paths are returned."""
        if paths:
            return [ds for ds in datasets if ds.path.startswith(paths)]
        else:
            return datasets

    def _update_table(
        self, sess, base_ds, target_ds, commit=None, track_changes_as_dirty=False
    ):
        """
        Update the given table in working copy from its current state to target_ds.
        The table must exist in the working copy in the source and continue to exist in the destination,
        and not have any unsupported meta changes - see _is_meta_update_supported.

        sess - sqlalchemy session.
        base_ds - the dataset that this working copy table is currently based on.
        target_ds - the target desired state for this working copy table.
        commit - the commit that contains target_ds, if any.
        track_changes_if_dirty - whether to track changes made from base_ds -> target_ds as WC edits.
        """

        self._apply_meta_diff(sess, base_ds, ~self.diff_db_to_tree_meta(base_ds))
        # WC now has base_ds structure and so we can write base_ds features to WC.
        self._reset_dirty_rows(sess, base_ds)

        if target_ds != base_ds:
            self._apply_meta_diff(sess, target_ds, base_ds.diff_meta(target_ds))
            # WC now has target_ds structure and so we can write target_ds features to WC.
            self._apply_feature_diff(sess, base_ds, target_ds, track_changes_as_dirty)

        self._update_last_write_time(sess, target_ds, commit)

    def _apply_feature_diff(
        self, sess, base_ds, target_ds, track_changes_as_dirty=False
    ):
        """
        Change the features of this working copy from their current state, base_ds - to the desired state, target_ds.

        sess - sqlalchemy session.
        base_ds - dataset containing the features that match the WC table currently.
        target_ds - dataset containing the desired features of the WC table.
        track_changes_as_dirty - whether to track these changes as working-copy edits in the tracking table.
        """
        feature_diff_index = base_ds.feature_tree.diff_to_tree(target_ds.feature_tree)
        if not feature_diff_index:
            return

        L.debug("Applying feature diff: about %s changes", len(feature_diff_index))

        delete_pks = []
        insert_and_update_pks = []

        for d in feature_diff_index.deltas:
            if d.old_file and d.old_file.path.startswith(base_ds.META_PATH):
                continue
            if d.new_file and d.new_file.path.startswith(base_ds.META_PATH):
                continue

            if d.status == pygit2.GIT_DELTA_DELETED:
                delete_pks.append(base_ds.decode_path_to_1pk(d.old_file.path))
            elif d.status in (pygit2.GIT_DELTA_ADDED, pygit2.GIT_DELTA_MODIFIED):
                insert_and_update_pks.append(
                    target_ds.decode_path_to_1pk(d.new_file.path)
                )
            else:
                # RENAMED, COPIED, IGNORED, TYPECHANGE, UNMODIFIED, UNREADABLE, UNTRACKED
                raise NotImplementedError(f"Delta status: {d.status_char()}")

        if not track_changes_as_dirty:
            # We don't want to track these changes as working copy edits - they will be part of the new WC base.
            ctx = self._suspend_triggers(sess, base_ds)
        else:
            # We want to track these changes as working copy edits so they can be committed later.
            ctx = contextlib.nullcontext()

        with ctx:
            self._delete_features(sess, base_ds, delete_pks)
            self._write_features(sess, target_ds, insert_and_update_pks)

    def _is_meta_update_supported(self, dataset_version, meta_diff):
        """
        Returns True if the given meta-diff is supported *without* dropping and rewriting the table.
        (Any meta change is supported if we drop and rewrite the table, but of course it is less efficient).

        meta_diff - DeltaDiff object containing the meta changes.
        """

        # By default, no meta updates are supported (without dropping and rewriting).
        # Subclasses can override to support various types of meta updates.
        return not meta_diff

    def _apply_meta_diff(self, sess, target_ds, meta_diff):
        """
        Change the metadata of this working copy according to the given meta diff.
        Not all changes are possible or supported - see _is_meta_update_supported.

        sess - sqlalchemy session.
        target_ds - controls which table to update. May also be used to look up target CRS.
        meta_diff - a DeltaDiff object containing meta-item deltas for this dataset.
        """
        L.debug("Meta diff: %s changes", len(meta_diff))
        for key in meta_diff:
            if key.startswith("crs/"):
                # CRS changes are handled by _apply_meta_schema_json
                continue
            func_key = key.replace("/", "_").replace(".", "_")
            func = getattr(self, f"_apply_meta_{func_key}")
            delta = meta_diff[key]
            func(sess, target_ds, delta.old_value, delta.new_value)

    def _reset_dirty_rows(self, sess, base_ds):
        """
        Reset the dirty rows recorded in the tracking table to match the originals from the dataset.

        sess - sqlalchemy session.
        base_ds - the dataset this WC table is based on.
        """
        r = sess.execute(
            f"""SELECT pk FROM {self.SNO_TRACK} WHERE table_name = :table_name;""",
            {"table_name": base_ds.table_name},
        )
        dirty_pk_list = [row[0] for row in r]
        track_count = len(dirty_pk_list)
        if not dirty_pk_list:
            return

        # We're resetting the dirty rows so we don't track these changes in the tracking table.
        with self._suspend_triggers(sess, base_ds):
            # todo: suspend/remove spatial index
            L.debug("Cleaning up dirty rows...")

            count = self._delete_features(sess, base_ds, dirty_pk_list)
            L.debug(
                "_reset_dirty_rows(): removed %s features, tracking Δ count=%s",
                count,
                track_count,
            )
            count = self._write_features(
                sess, base_ds, dirty_pk_list, ignore_missing=True
            )
            L.debug(
                "_reset_dirty_rows(): wrote %s features, tracking Δ count=%s",
                count,
                track_count,
            )

            sess.execute(
                f"DELETE FROM {self.SNO_TRACK} WHERE table_name=:table_name;",
                {"table_name": base_ds.table_name},
            )
