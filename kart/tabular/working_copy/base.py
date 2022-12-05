import contextlib
import functools
import logging
import time

import click
import pygit2

import sqlalchemy as sa
from kart.core import peel_to_commit_and_tree
from kart.diff_structs import WORKING_COPY_EDIT, DatasetDiff, Delta, DeltaDiff, RepoDiff
from kart.exceptions import (
    NO_WORKING_COPY,
    NotFound,
    NotYetImplemented,
)
from kart.key_filters import DatasetKeyFilter, FeatureKeyFilter, RepoKeyFilter
from kart import meta_items
from kart.promisor_utils import LibgitSubcode
from kart.sqlalchemy.upsert import Upsert as upsert
from kart.tabular.table_dataset import TableDataset
from kart.schema import DefaultRoundtripContext, Schema, is_schema_delta_pk_compatible
from kart.utils import chunk
from kart.working_copy import WorkingCopyDirty, WorkingCopyPart

from . import TableWorkingCopyStatus, TableWorkingCopyType

L = logging.getLogger("kart.tabular.working_copy.base")


class TableWorkingCopy(WorkingCopyPart):
    """
    Abstract tabular working copy implementation.
    Subclasses to override any unimplemented methods below, and also to set the following fields:

    self.repo - KartRepo containing this WorkingCopy
    self.location - string describing the location of this WorkingCopy
    self.engine - sqlalchemy engine for connecting to the database
    self.sessionmaker - sqlalchemy sessionmaker bound to the engine
    self.preparer - sqlalchemy IdentifierPreparer for quoting SQL in the appropriate dialect

    self.db_schema - database-schema that this working copy controls, if any.
    self.kart_tables - sqlalchemy Table definitions for kart_state and kart_track tables.
    """

    # Subclasses should override if they can support more meta-items eg description or metadata.xml
    SUPPORTED_META_ITEMS = (
        meta_items.TITLE,
        meta_items.SCHEMA_JSON,
        meta_items.CRS_DEFINITIONS
        # Not description, not metadata.xml, except where overridden by a subclass.
    )

    @property
    def WORKING_COPY_TYPE_NAME(self):
        """Human readable name of this type of working copy, eg "PostGIS"."""
        raise NotImplementedError()

    @property
    @functools.lru_cache(maxsize=1)
    def KART_STATE(self):
        """Escaped, dialect-specific fully-qualified name of kart_state table."""
        return self.table_identifier(self.kart_tables.kart_state)

    @property
    @functools.lru_cache(maxsize=1)
    def KART_TRACK(self):
        """Escaped, dialect-specific fully-qualified name of kart_track table."""
        return self.table_identifier(self.kart_tables.kart_track)

    @property
    @functools.lru_cache(maxsize=1)
    def KART_STATE_NAME(self):
        """The table name of kart_state table, not including the schema."""
        return self.kart_tables.kart_state.name

    @property
    @functools.lru_cache(maxsize=1)
    def KART_TRACK_NAME(self):
        """The table name of kart_track table, not including the schema."""
        return self.kart_tables.kart_track.name

    @property
    def clean_location(self):
        """The location, but with any passwords hidden so we can print it without exposing them."""
        return self.location

    def __repr__(self):
        return f"<{self.__class__.__name__}: {self.clean_location}>"

    def __str__(self):
        return self.clean_location

    def quote(self, ident):
        """Conditionally quote an identifier - eg if it is a reserved word or contains special characters."""
        return self.preparer.quote(ident)

    @functools.lru_cache()
    def table_identifier(self, dataset_or_table):
        """Given a dataset, tablename, or sqlalchemy table, escapes its fullname for use in a SQL statement."""
        table = (
            dataset_or_table.table_name
            if isinstance(dataset_or_table, TableDataset)
            else dataset_or_table
        )
        sqlalchemy_table = (
            sa.table(table, schema=self.db_schema) if isinstance(table, str) else table
        )
        return self.preparer.format_table(sqlalchemy_table)

    def _quoted_tracking_name(self, trigger_type, dataset=None):
        """
        Returns the name of the trigger responsible for populating the kart_track table.
        There are a few different name variants for naming the trigger itself, a stored procedure
        that the trigger uses, or three triggers that handle inserts, updates, and deletes separately.
        If dataset is supplied, the name will include dataset.table_name so that multiple triggers,
        one per dataset, can be created.
        """
        assert trigger_type in ("trigger", "proc", "ins", "upd", "del")
        if self.repo.is_kart_branded:
            trigger_name = self._kart_tracking_name(trigger_type, dataset)
        else:
            trigger_name = self._sno_tracking_name(trigger_type, dataset)

        if self.db_schema and self._trigger_type_requires_db_schema(trigger_type):
            # self.table_identifier fully-qualifies the trigger_name with self.db_schema:
            return self.table_identifier(trigger_name)
        else:
            # self.quote just quotes the name without adding any db_schema.
            return self.quote(trigger_name)

    def _kart_tracking_name(self, trigger_type, dataset=None):
        """Returns the kart-branded name of the trigger reponsible for populating the kart_track table."""
        if dataset is not None:
            return f"_kart_track_{dataset.table_name}_{trigger_type}"
        else:
            return f"_kart_track_{trigger_type}"

    def _trigger_type_requires_db_schema(self, trigger_type):
        # Subclasses should return True if the given trigger_type would be put in a database-wide namespace,
        # and so the db_schema should be included in the trigger's fully qualified name.
        # Return False if the given trigger_type exists in some smaller namespace - eg if it is already
        # namespaced within the table it is attached to - and so including the db_schema would be an error.
        return True

    def _sno_tracking_name(self, trigger_type, dataset=None):
        """
        Returns the name of the trigger responsible for populating the sno_track table.
        These names are older and are not as consistent - each WorkingClass type must override to provide its own logic.
        """
        raise NotImplementedError()

    @functools.lru_cache()
    def _table_def_for_dataset(self, dataset):
        """Returns a sqlalchemy table definition with conversion-logic for reading or writing to the dataset table."""
        return self.adapter.table_def_for_schema(
            dataset.schema,
            db_schema=self.db_schema,
            table_name=dataset.table_name,
            dataset=dataset,
        )

    @functools.lru_cache()
    def _table_def_for_schema(self, schema, table_name):
        """Returns a sqlalchemy table definition with conversion-logic for reading or writing data with the given schema."""
        return self.adapter.table_def_for_schema(
            schema, db_schema=self.db_schema, table_name=table_name
        )

    def insert_into_dataset_cmd(self, dataset):
        """Returns a SQL command for inserting features into the table for that dataset."""
        return self._table_def_for_dataset(dataset).insert()

    def insert_or_replace_into_dataset_cmd(self, dataset):
        """
        Returns a SQL command for inserting/replacing features that may or may not already exist in the table
        for that dataset.
        """
        return upsert(self._table_def_for_dataset(dataset))

    @classmethod
    def get(
        cls,
        repo,
        *,
        allow_uncreated=False,
        allow_invalid_state=False,
        allow_unconnectable=False,
    ):
        """
        Get the working copy associated with this Kart repo, as specified in the repo config.
        Note that the working copy specified in the repo config may or may not exist or be in a valid state.
        An instance of this class can represent a working copy that doesn't exist or is in an invalid state,
        (similar to how pathlib.Path can point to files that may or may not exist).
        If allow_uncreated is True, a non-existant working copy may be returned - otherwise, only an existing
        working copy will be returned, and None will be returned if no working copy is found.
        If allow_invalid_state is True, an invalid-state working copy may be returned - otherwise, only a valid
        working copy will be returned, and a NotFound(NO_WORKING_COPY) will be raised if it is in an invalid state.
        If allow_unconnectable is True, a working copy that cannot be connected to may be returned - otherwise,
        only a working copy that can be connected to will be returned, and a DbConnectionError will be raised otherwise.
        """
        location = repo.workingcopy_location
        if not location:
            return None

        return cls.get_at_location(
            repo,
            location,
            allow_uncreated=allow_uncreated,
            allow_invalid_state=allow_invalid_state,
            allow_unconnectable=allow_unconnectable,
        )

    @classmethod
    def get_at_location(
        cls,
        repo,
        location,
        *,
        allow_uncreated=False,
        allow_invalid_state=False,
        allow_unconnectable=False,
    ):
        if not location:
            return None

        wc_type = TableWorkingCopyType.from_location(
            location, allow_invalid=allow_invalid_state
        )
        if not wc_type:
            return None
        wc = wc_type.class_(repo, location)

        if allow_uncreated and allow_invalid_state and allow_unconnectable:
            return wc

        status = wc.status(allow_unconnectable=allow_unconnectable)
        if not allow_invalid_state:
            wc.check_valid_state(status)

        if not allow_uncreated and not (status & TableWorkingCopyStatus.WC_EXISTS):
            wc = None

        return wc

    @classmethod
    def ensure_config_exists(cls, repo):
        if repo.workingcopy_location is None and not repo.is_bare:
            cls.write_config(repo, None, False)

    @classmethod
    def write_config(cls, repo, location=None, bare=False):
        location_key = repo.WORKINGCOPY_LOCATION_KEY

        if bare:
            repo.del_config(location_key)
            return

        if location is None:
            location = cls.default_location(repo)
        else:
            location = cls.normalise_location(location, repo)

        repo.config[location_key] = str(location)

    @classmethod
    def subclass_from_location(cls, wc_location):
        wct = TableWorkingCopyType.from_location(wc_location)
        if wct.class_ is cls:
            raise RuntimeError(
                f"No subclass found - don't call subclass_from_location on concrete implementation {cls}."
            )
        return wct.class_

    @classmethod
    def check_valid_creation_location(cls, wc_location, repo):
        """
        Given a user-supplied string describing where to put the working copy, ensures it is a valid location,
        and nothing already exists there that prevents us from creating it. Raises InvalidOperation if it is not.
        Doesn't check if we have permissions to create a working copy there.
        """
        if not wc_location:
            wc_location = cls.default_location(repo)
        cls.subclass_from_location(wc_location).check_valid_creation_location(
            wc_location, repo
        )

    @classmethod
    def clearly_doesnt_exist(cls, wc_location, repo):
        """
        Given a user-supplied string describing where to put the working copy, returns True if there is clearly
        no working copy or other database already there. This call should return in a fraction of a second.
        Generally returns False since, we don't attempt to connect to a database using username+password to check
        if something "clearly doesn't exist" - this could fail for network or authentication reasons, and can
        take a long time before it fails, which violates the contract of this method.
        """
        if not wc_location:
            wc_location = cls.default_location(repo)
        return cls.subclass_from_location(wc_location).clearly_doesnt_exist(
            wc_location, repo
        )

    @classmethod
    def check_valid_location(cls, wc_location, repo):
        """
        Given a user-supplied string describing where to put the working copy, ensures it is a valid location,
        and nothing already exists there that prevents us from creating it. Raises InvalidOperation if it is not.
        Doesn't check if we have permissions to create a working copy there.
        """
        cls.subclass_from_location(wc_location).check_valid_location(wc_location, repo)

    def check_valid_state(self, status=None):
        if status is None:
            status = self.status()

        wc_exists = status & TableWorkingCopyStatus.WC_EXISTS
        if wc_exists and not (status & TableWorkingCopyStatus.INITIALISED):
            message = [
                f"Working copy at {self} is not yet fully initialised",
                "Try `kart create-workingcopy --delete-existing` to delete and recreate working copy if problem persists",
            ]
            if status & TableWorkingCopyStatus.HAS_DATA:
                message.append(
                    f"But beware: {self} already seems to contain data, make sure it is backed up"
                )
            raise NotFound("\n".join(message), exit_code=NO_WORKING_COPY)

    @classmethod
    def default_location(cls, repo):
        """Returns `example.gpkg` for a Kart repo in a directory named `example`."""
        stem = repo.workdir_path.stem
        return f"{stem}.gpkg"

    @classmethod
    def normalise_location(cls, wc_location, repo):
        """If the path is in a non-standard form, normalise it to the equivalent standard form."""
        return cls.subclass_from_location(wc_location).normalise_location(
            wc_location, repo
        )

    @contextlib.contextmanager
    def session(self):
        """
        Context manager for database sessions, yields a connection object inside a transaction

        Calling again yields the _same_ session, the transaction/etc only happen in the outer one.
        """
        L = logging.getLogger(f"{self.__class__.__qualname__}.session")

        if hasattr(self, "_session"):
            # Inner call - reuse existing session.
            L.debug("session: existing...")
            yield self._session
            L.debug("session: existing/done")
            return

        L.debug("session: new...")
        self._session = self.sessionmaker()
        try:
            # TODO - use tidier syntax for opening transactions from sqlalchemy.
            yield self._session
            self._session.commit()
        except Exception:
            self._session.rollback()
            raise
        finally:
            self._session.close()
            del self._session
            L.debug("session: new/done")

    def status(self, check_if_dirty=False, allow_unconnectable=False):
        """
        Returns a union of TableWorkingCopyStatus values.
        """
        raise NotImplementedError()

    def create_and_initialise(self):
        """Create the database container or database schema if required, and the Kart tables."""
        raise NotImplementedError()

    def delete(self, keep_db_schema_if_possible=False):
        """
        Delete the entire working copy.

        keep_db_schema_if_possible - set to True if the WC is being recreated in the same location
        in a moment's time. It's possible we lack permission to recreate the db schema, so in this
        case, better not to delete it.
        """
        raise NotImplementedError()

    def get_kart_state_value(self, table_name, key):
        """Returns the hex tree ID from the state table."""
        kart_state = self.kart_tables.kart_state
        with self.session() as sess:
            return sess.scalar(
                sa.select([kart_state.c.value]).where(
                    sa.and_(
                        kart_state.c.table_name == table_name, kart_state.c.key == key
                    )
                )
            )

    def tracking_changes_count(self, dataset=None):
        """
        Returns the total number of changes tracked in kart_track,
        or the number of changes tracked for the given dataset.
        """
        kart_track = self.kart_tables.kart_track
        with self.session() as sess:
            if dataset is not None:
                return sess.scalar(
                    sa.select([sa.func.count()])
                    .select_from(kart_track)
                    .where(kart_track.c.table_name == dataset.table_name)
                )
            else:
                return sess.scalar(sa.select([sa.func.count()]).select_from(kart_track))

    def is_dirty(self):
        """
        Returns True if there are uncommitted changes in the working copy,
        or False otherwise.
        """
        if self.get_tree_id() is None:
            return False
        try:
            self.diff_repo_to_working_copy(raise_if_dirty=True)
            return False
        except WorkingCopyDirty:
            return True

    def diff_repo_to_working_copy(
        self, repo_filter=RepoKeyFilter.MATCH_ALL, raise_if_dirty=False
    ):
        """
        Generates a diff between a working copy DB and the underlying repository tree,
        for every dataset in the given repository structure.
        """
        with self.session():
            repo_diff = RepoDiff()
            for dataset in self.repo.datasets(
                self.get_tree_id(),
                repo_key_filter=repo_filter,
                filter_dataset_type="table",
            ):
                if not self._is_dataset_supported(dataset):
                    continue
                ds_diff = self.diff_dataset_to_working_copy(
                    dataset,
                    ds_filter=repo_filter[dataset.path],
                    raise_if_dirty=raise_if_dirty,
                )
                repo_diff[dataset.path] = ds_diff
            repo_diff.prune()
            return repo_diff

    def _is_dataset_supported(self, dataset):
        """
        Returns False if the given dataset cannot be created in this working copy.
        Generally returns True since we do our best to create all datasets, even if not 100% accurately.
        """
        return True

    def diff_dataset_to_working_copy(
        self, dataset, ds_filter=DatasetKeyFilter.MATCH_ALL, raise_if_dirty=False
    ):
        """
        Generates a diff between a working copy DB and the underlying repository tree,
        for a single dataset only.
        """
        if not self._is_dataset_supported(dataset):
            return DatasetDiff()

        feature_filter = ds_filter.get("feature", ds_filter.child_type())
        with self.session():
            meta_diff = self.diff_dataset_to_working_copy_meta(dataset, raise_if_dirty)
            feature_diff = self.diff_dataset_to_working_copy_feature(
                dataset, feature_filter, meta_diff, raise_if_dirty
            )

        ds_diff = DatasetDiff()
        ds_diff["meta"] = meta_diff
        ds_diff["feature"] = feature_diff
        return ds_diff

    def diff_dataset_to_working_copy_meta(self, dataset, raise_if_dirty=False):
        """
        Returns a DeltaDiff showing all the changes of metadata between the dataset and this working copy.
        """
        ds_meta_items = self.adapter.remove_empty_values(dataset.meta_items())
        wc_meta_items = self.meta_items(dataset)
        self._remove_hidden_meta_diffs(dataset, ds_meta_items, wc_meta_items)
        result = DeltaDiff.diff_dicts(
            ds_meta_items, wc_meta_items, delta_flags=WORKING_COPY_EDIT
        )
        if raise_if_dirty and result:
            raise WorkingCopyDirty()
        return result

    # Subclasses should override this function if there are certain types they cannot represent perfectly.
    @classmethod
    def try_align_schema_col(cls, old_col_dict, new_col_dict):
        return DefaultRoundtripContext.try_align_schema_col(old_col_dict, new_col_dict)

    @classmethod
    def _remove_hidden_numeric_diffs(
        cls, old_col_dict, new_col_dict, default_prec, default_scale=0
    ):
        """
        If we create an unadorned NUMERIC in a certain database type, it will roundtrip as
        NUMERIC(default precision, default scale). Hide this spurious diff when it shows up.
        """
        for name, default_val in ("precision", default_prec), ("scale", default_scale):
            if old_col_dict.get(name) is None and new_col_dict.get(name) == default_val:
                new_col_dict[name] = None

    def _is_supported_meta_item(self, meta_item_name):
        if meta_item_name in self.SUPPORTED_META_ITEMS:
            return True
        for definition in self.SUPPORTED_META_ITEMS:
            if not isinstance(definition, meta_items.MetaItemDefinition):
                continue
            if definition.matches(meta_item_name):
                return True
        return False

    def _remove_hidden_meta_diffs(self, dataset, ds_meta_items, wc_meta_items):
        """
        Remove any meta diffs that can't or shouldn't be committed, and so shouldn't be shown to the user.
        For all WC's, this means re-adding the column-IDs to schema.json since no WC can store column IDs.
        Subclasses can override and make more changes, depending on the WC's limitations - for instance, if the WC
        can't store the dataset description, then that should be removed from the diff.
        """

        def _safe_del(dict_, key):
            dict_.pop(key, None)

        ds_meta_keys = list(ds_meta_items.keys())
        for key in ds_meta_keys:
            if not self._is_supported_meta_item(key):
                _safe_del(ds_meta_items, key)

        # A dataset should have at most ONE of "metadata.xml" or "metadata/dataset.json".
        # The XML file is newer and supercedes the JSON file.
        # The GPKG adapter generates both, so we delete one so as to match the dataset.
        if "metadata/dataset.json" in ds_meta_items:
            _safe_del(wc_meta_items, "metadata.xml")
        else:
            _safe_del(wc_meta_items, "metadata/dataset.json")

        # Make sure schema IDs are preserved, even though these cannot be roundtripped through the working copy:
        if "schema.json" in ds_meta_items and "schema.json" in wc_meta_items:
            ds_schema = ds_meta_items["schema.json"]
            wc_schema = wc_meta_items["schema.json"]
            Schema.align_schema_cols(ds_schema, wc_schema, roundtrip_ctx=self)

        # Remove any spurious diffs caused by the WC having built-in CRS's that we can't / shouldn't modify:
        self._remove_builtin_crs_diffs(ds_meta_items, wc_meta_items)

    def _remove_builtin_crs_diffs(self, ds_meta_items, wc_meta_items):
        """
        All WC implementations that can contain CRS definitions come with one or more built-in CRS definitions
        (eg EPSG:4326 in GPKG). These definitions - although copied from an official standard - may differ in
        unimportant ways from the user-supplied version of the same standard. (We assume the differences are
        unimportant, or they would not both accurately represent the same standard.)
        For example - one might include more detail about units, or have axes in the opposite order (which
        is usually unimportant since most software ignores the specified ordering and makes its own choices).

        The user cannot action these diffs - they can't or shouldn't modify the built-in definitions in the WC
        by overwriting them from the dataset, nor should their choice of WC cause them to commit a new WC
        definition to the dataset (this would make collaborating between different WC types more difficult).
        So, we simply hide these diffs. There is no real difference anyway - everyone agrees that the chosen
        standard (eg EPSG:4326) should be used.
        """
        for key in ds_meta_items.keys() & wc_meta_items.keys():
            if not key.startswith("crs/"):
                continue
            old_is_standard = self._is_builtin_crs(ds_meta_items[key])
            new_is_standard = self._is_builtin_crs(wc_meta_items[key])
            if old_is_standard and new_is_standard:
                del ds_meta_items[key]
                del wc_meta_items[key]
            # If either definition is custom, we keep the diff, since it could be important.

    def _is_builtin_crs(self, crs):
        """
        Returns True if this WC implementation has (some definition of) the given CRS stored as a built-in.
        For instance, GPKG would return True if given a definition EPSG:4326, which is built into every GPKG.
        """
        return False

    def meta_items(self, dataset):
        """
        Extract all the metadata for this table and convert to dataset V2 format.
        Note that the extracted schema will not be aligned to any existing schema
        - the generated column IDs are stable, but do not necessarily match the ones in the dataset.
        Calling Schema.align_* is required to find how the columns matches the existing schema.
        """

        # Column IDs are generated deterministically from the column contents and the current state.
        # That way, they don't vary at random if the same command is run twice in a row, but
        # they will vary as the repo state changes so that we don't accidentally generate the same ID twice
        # for two unrelated columns.
        id_salt = f"{self.engine.url} {self.db_schema} {dataset.table_name} {self.get_tree_id()}"

        with self.session() as sess:
            return self.adapter.all_v2_meta_items(
                sess,
                self.db_schema,
                dataset.table_name,
                id_salt=id_salt,
            )

    def diff_dataset_to_working_copy_feature(
        self, dataset, feature_filter, meta_diff, raise_if_dirty=False
    ):
        schema_delta = meta_diff.get("schema.json")

        if schema_delta and schema_delta.type == "delete":
            # The entire table has been deleted - add delete deltas for every feature.
            return dataset.all_features_diff(
                delta_type=Delta.delete, flags=WORKING_COPY_EDIT
            )
        if (
            schema_delta
            and schema_delta.type == "update"
            and not is_schema_delta_pk_compatible(schema_delta)
        ):
            return dataset.all_features_diff(
                delta_type=Delta.delete, flags=WORKING_COPY_EDIT
            ) + self.all_features_diff(dataset, meta_diff, delta_type=Delta.delete)

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

                repo_obj = self._get_dataset_feature_and_fetch_if_needed(
                    dataset, track_pk
                )

                if repo_obj == db_obj:
                    # DB was changed and then changed back - eg INSERT then DELETE.
                    # TODO - maybe delete track_pk from tracking table?
                    continue

                if raise_if_dirty:
                    raise WorkingCopyDirty()

                if db_obj and not repo_obj:  # INSERT
                    insert_count += 1
                    delta = Delta.insert((db_obj[pk_field], db_obj))

                elif repo_obj and not db_obj:  # DELETE
                    delete_count += 1
                    delta = Delta.delete((repo_obj[pk_field], repo_obj))

                else:  # UPDATE
                    pk = db_obj[pk_field]
                    delta = Delta.update((pk, repo_obj), (pk, db_obj))

                delta.flags = WORKING_COPY_EDIT
                feature_diff.add_delta(delta)

        if find_renames and (insert_count + delete_count) <= 400:
            self.find_renames(feature_diff, dataset)

        return feature_diff

    def all_features_diff(self, dataset, meta_diff, delta_type=Delta.insert):
        assert delta_type in (Delta.insert, Delta.delete)
        pk_field = self._get_dataset_schema(dataset, meta_diff).pk_columns[0].name

        feature_diff = DeltaDiff()
        with self.session() as sess:
            r = self._execute_all_rows_query(sess, dataset, meta_diff)
            for row in r:
                db_obj = {k: row[k] for k in row.keys()}
                delta = Delta.insert((db_obj[pk_field], db_obj))
                delta.flags = WORKING_COPY_EDIT
                feature_diff.add_delta(delta)

        return feature_diff

    def _get_dataset_feature_and_fetch_if_needed(self, dataset, feature_pk):
        try:
            return dataset.get_feature(feature_pk)
        except KeyError as e:
            # Couldn't find the feature.
            subcode = getattr(e, "subcode", 0)

            if subcode == LibgitSubcode.ENOSUCHPATH:
                # There is no such feature. This is okay: it just means the user has inserted
                # the feature into the working copy and it has not yet been committed.
                return None
            elif subcode == LibgitSubcode.EOBJECTPROMISED:
                # A feature with this PK exists, but we don't have it locally right now. Fetch it.
                # Note that this means the feature presumably doesn't match the user's spatial filter,
                # so it was probably a mistake by the user that they have reused the existing feature's PK.
                dataset.fetch_missing_dirty_features(self)
                return dataset.get_feature(feature_pk)
            else:
                # Some other error has happened, or no subcode was found. Re-raise the error.
                raise

    @property
    def _tracking_table_requires_cast(self):
        """
        True if the tracking table requires a SQL cast from the PK type.
        In reality only False for GPKG with its loose relationship with types.
        """
        return True

    def _get_dataset_schema(self, dataset, meta_diff):
        """
        Returns the new value of the schema from the meta diff, or the schema from the dataset if
        there is no change to the schema in the meta diff.
        """
        if (
            meta_diff
            and "schema.json" in meta_diff
            and meta_diff["schema.json"].new_value
        ):
            return Schema.from_column_dicts(meta_diff["schema.json"].new_value)
        else:
            return dataset.schema

    def _execute_dirty_rows_query(
        self, sess, dataset, feature_filter=FeatureKeyFilter.MATCH_ALL, meta_diff=None
    ):
        """
        Does a join on the tracking table and the table for the given dataset, and returns a result
        containing all the rows that have been inserted / updated / deleted.
        """
        schema = self._get_dataset_schema(dataset, meta_diff)

        kart_track = self.kart_tables.kart_track
        table = self._table_def_for_schema(schema, dataset.table_name)

        cols_to_select = [kart_track.c.pk.label(".__track_pk"), *table.columns]
        pk_column = table.columns[schema.pk_columns[0].name]
        tracking_col_type = kart_track.c.pk.type

        if self._tracking_table_requires_cast:
            pk_expr = kart_track.c.pk == sa.cast(pk_column, tracking_col_type)
        else:
            pk_expr = kart_track.c.pk == pk_column

        base_query = sa.select(columns=cols_to_select).select_from(
            kart_track.outerjoin(
                table,
                pk_expr,
            )
        )

        if feature_filter.match_all:
            query = base_query.where(kart_track.c.table_name == dataset.table_name)
        else:
            pks = list(feature_filter)
            query = base_query.where(
                sa.and_(
                    kart_track.c.table_name == dataset.table_name,
                    kart_track.c.pk.in_(pks),
                )
            )

        return sess.execute(query)

    def _execute_all_rows_query(self, sess, dataset, meta_diff=None):
        """
        Does a join on the tracking table and the table for the given dataset, and returns a result
        containing all the rows that have been inserted / updated / deleted.
        """
        schema = self._get_dataset_schema(dataset, meta_diff)
        table = self._table_def_for_schema(schema, dataset.table_name)
        return sess.execute(sa.select(table.columns).select_from(table))

    def get_dirty_pks(self, dataset):
        kart_track = self.kart_tables.kart_track
        with self.session() as sess:
            r = sess.execute(
                sa.select(columns=[kart_track.c.pk])
                .select_from(kart_track)
                .where(kart_track.c.table_name == dataset.table_name)
            )
            return (row[0] for row in r)

    def get_feature(self, dataset, pk, allow_schema_diff=True):
        """
        Returns the feature that has the given primary key value.
        If the primary key value does not exist in the dataset, returns None.
        """
        meta_diff = self.diff_dataset_to_working_copy_meta(dataset)
        if (not allow_schema_diff) and meta_diff.recursive_get(["meta", "schema.json"]):
            raise ValueError("Feature in WC doesn't conform to required schema")
        schema = self._get_dataset_schema(dataset, meta_diff)
        table = self._table_def_for_schema(schema, dataset.table_name)
        pk_column = table.columns[schema.pk_columns[0].name]

        with self.session() as sess:
            r = sess.execute(sa.select(table).where(pk_column == pk))
            for row in r:
                return {col.name: row[col.name] for col in dataset.schema}
            return None

    def _mark_as_clean(self, sess, repo_key_filter):
        """Delete the rows from the tracking table that match the given filter."""
        if not repo_key_filter:
            return

        kart_track = self.kart_tables.kart_track
        if repo_key_filter.match_all:
            sess.execute(sa.delete(kart_track))
            return

        for dataset_path, dataset_filter in repo_key_filter.items():
            table_name = TableDataset.dataset_path_to_table_name(dataset_path)
            feature_filter = dataset_filter.get("feature", dataset_filter.child_type())
            self._mark_as_clean_for_table(sess, table_name, feature_filter)

    def _mark_as_clean_for_table(self, sess, table_name, feature_filter):
        kart_track = self.kart_tables.kart_track
        if feature_filter.match_all:
            sess.execute(
                sa.delete(kart_track).where(kart_track.c.table_name == table_name)
            )
        else:
            pks = list(feature_filter)
            sess.execute(
                sa.delete(kart_track).where(
                    sa.and_(
                        kart_track.c.table_name == table_name,
                        kart_track.c.pk.in_(pks),
                    )
                )
            )

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
            self._update_state_table_tree(sess, tree_id)

    def _update_state_table_tree(self, sess, tree_id):
        """
        Write the given tree ID to the state table.

        sess - sqlalchemy session.
        tree_id - str, the hex SHA of the tree at HEAD.
        """
        r = sess.execute(
            upsert(self.kart_tables.kart_state),
            {"table_name": "*", "key": "tree", "value": tree_id or ""},
        )
        return r.rowcount

    def _update_state_table_spatial_filter_hash(self, sess, spatial_filter_hash):
        """
        Write the given spatial filter hash to the state table.

        sess - sqlalchemy session.
        spatial_filter_hash - str, a hash of the spatial filter.
        """
        kart_state = self.kart_tables.kart_state
        if spatial_filter_hash:
            r = sess.execute(
                upsert(kart_state),
                {
                    "table_name": "*",
                    "key": "spatial-filter-hash",
                    "value": spatial_filter_hash,
                },
            )
        else:
            r = sess.execute(
                sa.delete(kart_state).where(kart_state.c.key == "spatial-filter-hash")
            )
        return r.rowcount

    @contextlib.contextmanager
    def _suspend_triggers(self, sess, dataset):
        """Context manager that temporarily disables the triggers that track dirty rows for the given dataset."""
        raise NotImplementedError()

    def _track_changes_as_dirty(self, sess, dataset, track_changes_as_dirty):
        """Context manager that temporarily disables the triggers that track dirty rows - iff track_changes_as_dirty is False."""
        if not track_changes_as_dirty:
            return self._suspend_triggers(sess, dataset)
        else:
            return contextlib.nullcontext()

    def write_full(self, commit_or_tree, *datasets):
        """
        Writes a full layer into a working-copy table.
        Only writes features that match the repo's spatial filter.

        Use for new working-copy checkouts.
        """
        L = logging.getLogger(f"{self.__class__.__qualname__}.write_full")
        target_commit, target_tree = peel_to_commit_and_tree(commit_or_tree)

        self.repo.odb.refresh()
        with pause_refreshing(self.repo.odb), self.session() as sess:
            dataset_count = len(datasets)
            for i, dataset in enumerate(datasets):
                click.echo(
                    f"Writing features for dataset {i+1} of {dataset_count}: {dataset.path}",
                    err=True,
                )

                try:
                    # Create the table
                    self._write_meta(sess, dataset)
                    self._create_table_for_dataset(sess, dataset)
                except NotYetImplemented as e:
                    click.secho(
                        f"Couldn't write {dataset.table_name} to working copy:\n{e}",
                        err=True,
                        fg="red",
                    )
                    continue

                if dataset.has_geometry:
                    self._create_spatial_index_pre(sess, dataset)

                L.info("Creating features...")
                sql = self.insert_into_dataset_cmd(dataset)
                t0 = time.monotonic()

                CHUNK_SIZE = 10000

                for row_dicts in chunk(
                    dataset.features_with_crs_ids(
                        self.repo.spatial_filter, show_progress=True
                    ),
                    CHUNK_SIZE,
                ):
                    sess.execute(sql, row_dicts)

                if dataset.has_geometry:
                    self._create_spatial_index_post(sess, dataset)

                if not dataset.feature_path_encoder.DISTRIBUTED_FEATURES:
                    # Set up a sequence so that the user doesn't have to supply the next int PK.
                    self._initialise_sequence(sess, dataset)

                self._create_triggers(sess, dataset)
                self._update_last_write_time(sess, dataset, target_commit)

                t1 = time.monotonic()
                L.info(
                    "Wrote dataset %d of %d in %.1fs: %s",
                    i + 1,
                    dataset_count,
                    t1 - t0,
                    dataset.path,
                )

            self._update_state_table_tree(sess, target_tree.hex)
            self._update_state_table_spatial_filter_hash(
                sess, self.repo.spatial_filter.hexhash
            )

    def _write_meta(self, sess, dataset):
        """
        Write any non-feature data relating to dataset that is stored _outside_ the dataset table itself.
        This could be title, description, CRS definitions, or XML metadata.
        This is called before the table is created, in case the table needs it to be in place when it is
        created (for example, custom CRS definitions in MySQL must exist before a table can reference them).
        Data should not be overwritten if it comes standard with the WC implementation (eg, built-in CRS defitions).
        """
        raise NotImplementedError()

    def _create_table_for_dataset(self, sess, dataset):
        """
        Create the working-copy table for checking out the given dataset.
        Creates a table in the appropriate place with the appropriate columns, but doesn't write any rows.
        """
        raise NotImplementedError

    def _create_spatial_index_pre(self, sess, dataset):
        """
        Creates a spatial index for the table for the given dataset.
        This function comes in a pair - _pre is called before features are written, and _post is called afterwards.
        Once both are called, the index must contain all the features currently in the table, and, be
        configured such that any further writes cause the index to be updated automatically.
        """

        # Note that the simplest implementation is to add a trigger here so that any further writes update
        # the index. Then _create_spatial_index_post needn't be implemented.
        pass

    def _create_spatial_index_post(self, sess, dataset):
        """Like _create_spatial_index_pre, but runs AFTER the bulk of features have been written."""

        # Being able to create the index after the bulk of features have been written could be useful for two reasons:
        # 1. It might be more efficient to write the features first, then index afterwards.
        # 2. Certain working copies are not able to create an index without first knowing a rough bounding box.
        pass

    def _drop_spatial_index(self, sess, dataset):
        """Inverse of _create_spatial_index_* - deletes the spatial index."""
        pass

    def _initialise_sequence(self, sess, dataset):
        """Sets up an auto-increment integer PK sequence so that a useful default PK value is provided."""
        pass

    def _drop_sequence(self, sess, dataset):
        """Drop the PK auto-increment sequence associated with this dataset."""
        # Subclasses only need to override if the sequence is not automatically cleaned up by dropping the table.
        pass

    def _update_last_write_time(self, sess, dataset, commit=None):
        """Hook for updating the last-modified timestamp stored for a particular dataset, if there is one."""
        pass

    def _write_features_from_dataset(
        self, sess, dataset, pk_list, *, ignore_missing=False
    ):
        """Write the features from the dataset with the given PKs to the table for the dataset."""
        if not pk_list:
            return 0

        sql = self.insert_or_replace_into_dataset_cmd(dataset)
        feat_count = 0
        CHUNK_SIZE = 10000
        for row_dicts in chunk(
            dataset.get_features_with_crs_ids(
                pk_list,
                ignore_missing=ignore_missing,
                spatial_filter=self.repo.spatial_filter,
            ),
            CHUNK_SIZE,
        ):
            sess.execute(sql, row_dicts)
            feat_count += len(row_dicts)

        return feat_count

    def delete_features(self, sess, repo_key_filter, track_changes_as_dirty=True):
        """Deletes the features that match the given repo_key_filter."""
        if not repo_key_filter:
            return

        base_tree_id = self.get_tree_id()
        base_datasets = self.repo.datasets(base_tree_id, filter_dataset_type="table")

        if repo_key_filter.match_all:
            for base_ds in base_datasets:
                with self._track_changes_as_dirty(
                    sess, base_ds, track_changes_as_dirty
                ):
                    self._delete_features_from_dataset(
                        sess,
                        base_ds,
                        repo_key_filter.recursive_get(base_ds.path, "feature"),
                    )
            return

        for dataset_path, dataset_filter in repo_key_filter.items():
            base_ds = base_datasets.get(dataset_path)
            if not base_ds:
                continue
            feature_filter = dataset_filter.get("feature", dataset_filter.child_type())
            with self._track_changes_as_dirty(sess, base_ds, track_changes_as_dirty):
                self._delete_features_from_dataset(sess, base_ds, feature_filter)

    def _delete_features_from_dataset(self, sess, dataset, features_to_delete):
        """
        Delete all of the features with the given PKs in the table for the dataset.
        features_to_delete should be a FeatureKeyFilter, or a list of primary key values.
        """
        if not features_to_delete:
            return 0

        if isinstance(features_to_delete, FeatureKeyFilter):
            if features_to_delete.match_all:
                r = sess.execute(f"DELETE FROM {self.table_identifier(dataset)};")
                return r.rowcount
            pk_list = list(features_to_delete)
        else:
            pk_list = features_to_delete

        pk_column = self.preparer.quote(dataset.primary_key)
        sql = f"""DELETE FROM {self.table_identifier(dataset)} WHERE {pk_column} IN :pks;"""
        stmt = sa.text(sql).bindparams(sa.bindparam("pks", expanding=True))
        feat_count = 0
        CHUNK_SIZE = 100
        for pks in chunk(pk_list, CHUNK_SIZE):
            r = sess.execute(stmt, {"pks": pks})
            feat_count += r.rowcount

        return feat_count

    def drop_tables(self, commit_or_tree, *datasets):
        """Drop the tables for all the given datasets."""
        with self.session() as sess:
            for dataset in datasets:
                if dataset.has_geometry:
                    self._drop_spatial_index(sess, dataset)

                sess.execute(f"DROP TABLE IF EXISTS {self.table_identifier(dataset)};")
                self._delete_meta(sess, dataset)

                kart_track = self.kart_tables.kart_track
                sess.execute(
                    sa.delete(kart_track).where(
                        kart_track.c.table_name == dataset.table_name
                    )
                )
                self._drop_sequence(sess, dataset)

    def _delete_meta(self, sess, dataset):
        """
        Delete any non-feature data relating to the dataset that is stored outside the dataset table itself.
        This is called after the table itself is deleted.
        Data should not be deleted if it is still being used by another table that still exists,
        Data should not be deleted if it comes standard with the WC implementation (even if this dataset table was
        the last table that was actually referencing it).
        Data should not be deleted if it is not clear if Kart created it or not.
        """
        raise NotImplementedError()

    def reset(
        self,
        commit_or_tree,
        *,
        repo_key_filter=RepoKeyFilter.MATCH_ALL,
        track_changes_as_dirty=False,
        rewrite_full=False,
        quiet=False,
    ):
        """
        Resets the working copy to the given target-tree (or the tree pointed to by the given target-commit).

        Any existing changes which match the repo_key_filter will be discarded. Existing changes which do not
        match the repo_key_filter will be kept.

        If track_changes_as_dirty=False (the default) the tree ID in the kart_state table gets set to the
        new tree ID and the tracking table is left empty. If it is True, the old tree ID is kept and the
        tracking table is used to record all the changes, so that they can be committed later.

        If rewrite_full is True, then every dataset currently being tracked will be dropped, and all datasets
        present at commit_or_tree will be written from scratch using write_full.
        Since write_full honours the current repo spatial filter, this also ensures that the working copy spatial
        filter is up to date.
        """
        if rewrite_full:
            # These aren't supported when we're doing a full rewrite.
            assert repo_key_filter.match_all and not track_changes_as_dirty

        L = logging.getLogger(f"{self.__class__.__qualname__}.reset")
        if commit_or_tree is None:
            target_commit = target_tree = None
        else:
            target_commit, target_tree = peel_to_commit_and_tree(commit_or_tree)

        target_tree_id = target_tree.hex if target_tree is not None else None

        # base_tree is the tree the working copy is based on.
        # If the working copy exactly matches base_tree, it is clean and has an empty tracking table.
        base_tree_id = self.get_tree_id()
        base_tree = self.repo[base_tree_id] if base_tree_id else None
        repo_tree_id = self.repo.head_tree.hex if self.repo.head_tree else None

        L.debug(
            "reset(): WorkingCopy base_tree:%s, Repo HEAD has tree:%s. Resetting working copy to tree: %s",
            base_tree_id,
            repo_tree_id,
            target_tree_id,
        )
        L.debug("reset(): track_changes_as_dirty=%s", track_changes_as_dirty)

        base_datasets = self.repo.datasets(
            base_tree,
            repo_key_filter=repo_key_filter,
            filter_dataset_type="table",
        ).datasets_by_path()
        if base_tree == target_tree:
            target_datasets = base_datasets
        else:
            target_datasets = self.repo.datasets(
                target_tree,
                repo_key_filter=repo_key_filter,
                filter_dataset_type="table",
            ).datasets_by_path()

        ds_inserts = target_datasets.keys() - base_datasets.keys()
        ds_deletes = base_datasets.keys() - target_datasets.keys()
        ds_updates = base_datasets.keys() & target_datasets.keys()
        ds_updates_unsupported = set()

        if rewrite_full:
            # No updates are "supported" since we are rewriting everything.
            ds_updates_unsupported.update(ds_updates)
        else:
            for ds_path in ds_updates:
                base_ds = base_datasets[ds_path]

                # Do we support changing the WC metadata to back to base_ds metadata?
                rev_wc_meta_diff = self.diff_dataset_to_working_copy_meta(base_ds)
                update_supported = self._is_meta_update_supported(rev_wc_meta_diff)

                # And, do we support then changing it from base_ds metadata to target_ds metadata?
                target_ds = target_datasets[ds_path]
                if target_ds != base_ds:
                    rev_rev_meta_diff = DeltaDiff(base_ds.diff_meta(target_ds))
                    update_supported = (
                        update_supported
                        and self._is_meta_update_supported(rev_rev_meta_diff)
                    )

                if not update_supported:
                    ds_updates_unsupported.add(ds_path)

        for ds_path in ds_updates_unsupported:
            ds_updates.remove(ds_path)
            ds_inserts.add(ds_path)
            ds_deletes.add(ds_path)

        L.debug(
            "reset(): ds_inserts: %s, ds_deletes: %s, ds_updates %s",
            ds_inserts,
            ds_deletes,
            ds_updates,
        )

        structural_changes = ds_inserts | ds_deletes
        is_new_target_tree = base_tree != target_tree
        self._check_for_unsupported_structural_changes(
            structural_changes,
            is_new_target_tree,
            track_changes_as_dirty,
            repo_key_filter,
        )

        with self.session() as sess:
            # Delete old tables
            if ds_deletes:
                self.drop_tables(
                    commit_or_tree, *[base_datasets[d] for d in ds_deletes]
                )
            # Write new tables
            if ds_inserts:
                self.write_full(
                    commit_or_tree, *[target_datasets[d] for d in ds_inserts]
                )

            # Update tables that can be updated in place.
            for ds_path in ds_updates:
                base_ds = base_datasets[ds_path]
                target_ds = target_datasets[ds_path]
                self._update_table(
                    sess,
                    base_ds,
                    target_ds,
                    target_commit,
                    ds_filter=repo_key_filter[ds_path],
                    track_changes_as_dirty=track_changes_as_dirty,
                )

            if not track_changes_as_dirty:
                self._update_state_table_tree(sess, target_tree_id)

    def _update_table(
        self,
        sess,
        base_ds,
        target_ds,
        commit=None,
        *,
        ds_filter=DatasetKeyFilter.MATCH_ALL,
        track_changes_as_dirty=False,
    ):
        """
        Update the given table in working copy from its current state to target_ds.
        The table must exist in the working copy in the source and continue to exist in the destination,
        and not have any unsupported meta changes - see _is_meta_update_supported.

        sess - sqlalchemy session.
        base_ds - the dataset that this working copy table is currently based on.
        target_ds - the target desired state for this working copy table.
        commit - the commit that contains target_ds, if any.
        ds_filter - a DatasetKeyFilter controlling what to update.
        track_changes_if_dirty - whether to track changes made from base_ds -> target_ds as WC edits.
        """
        feature_filter = ds_filter.get("feature", ds_filter.child_type())

        self._apply_meta_diff(
            sess, base_ds, ~self.diff_dataset_to_working_copy_meta(base_ds)
        )
        # WC now has base_ds structure and so we can write base_ds features to WC.
        self._reset_dirty_rows(sess, base_ds, feature_filter)

        if target_ds != base_ds:
            self._apply_meta_diff(sess, target_ds, base_ds.diff_meta(target_ds))
            # WC now has target_ds structure and so we can write target_ds features to WC.
            self._apply_feature_diff(
                sess, base_ds, target_ds, feature_filter, track_changes_as_dirty
            )

        if not target_ds.feature_path_encoder.DISTRIBUTED_FEATURES:
            # Set up a sequence so that the user doesn't have to supply the next int PK.
            self._initialise_sequence(sess, target_ds)

        self._update_last_write_time(sess, target_ds, commit)

    def _apply_feature_diff(
        self,
        sess,
        base_ds,
        target_ds,
        feature_filter=FeatureKeyFilter.MATCH_ALL,
        track_changes_as_dirty=False,
    ):
        """
        Change the features of this working copy from their current state, base_ds, to the desired state, target_ds,
        as long as their keys match the feature filter. Used by reset() to move dataset tables that currently have one
        one commit checked out such that they have a different commit checked out - doesn't actually support applying
        arbitrary feature diffs to a table.

        sess - sqlalchemy session.
        base_ds - the dataset that contains the features in their current state
        target_ds - the dataset that contains the features in their desired state.
        feature_filter - features are only modified where their PKs match this filter.
        track_changes_as_dirty - whether to track these changes as working-copy edits in the tracking table.
        """

        feature_diff = DeltaDiff(base_ds.diff_feature(target_ds, feature_filter))

        if not feature_diff:
            return

        pks = list(feature_diff.keys())
        L.debug("Applying feature diff: about %s changes", len(pks))

        with self._track_changes_as_dirty(sess, target_ds, track_changes_as_dirty):
            self._delete_features_from_dataset(sess, target_ds, pks)
            self._write_features_from_dataset(sess, target_ds, pks, ignore_missing=True)

    def _is_meta_update_supported(self, meta_diff):
        """
        Returns True if the given meta-diff is supported *without* dropping and rewriting the table.
        (Any meta change is supported if we drop and rewrite the table, but of course it is less efficient).

        meta_diff - DeltaDiff object containing the meta changes.
        """
        if not meta_diff:
            return True

        # CRS changes complicated enough that we always drop and rewrite for now.
        if any(key.startswith("crs/") for key in meta_diff.keys()):
            return False

        # If CRS is not changed and schema is not changed, we can do an update.
        if "schema.json" not in meta_diff:
            return True

        # If schema is changed, can we do an update? Sometimes.
        return self._is_schema_update_supported(meta_diff["schema.json"])

    def _is_schema_update_supported(self, schema_delta):
        """
        Returns True if the given change to the schema is supported *without* dropping and rewriting the table.
        (Any schema change is supported if we drop and rewrite the table, but of course it is less efficient).

        schema_delta - the Delta object containing the change to schema.json
        """
        # Always return false -> always drop and rewrite. Subclasses can override for efficiency.
        return False

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
                raise RuntimeError(
                    f"CRS changes not supported by update - should be drop + re-write_full: {key}"
                )
            func_key = key.replace("/", "_").replace(".", "_")
            func = getattr(self, f"_apply_meta_{func_key}", None)
            if func is not None:
                delta = meta_diff[key]
                func(sess, target_ds, delta.old_value, delta.new_value)

    def apply_meta_title(self, sess, dataset, src_value, dest_value):
        raise RuntimeError(
            f"Title change not supported by update for {self.WORKING_COPY_TYPE_NAME} - should be drop + re-write_full"
        )

    def _apply_meta_description(self, sess, dataset, src_value, dest_value):
        pass  # This is a no-op for most WC types, which don't store descriptions.

    def _apply_meta_metadata_dataset_json(self, sess, dataset, src_value, dest_value):
        pass  # This is a no-op for most WC types, which don't store metadata JSON.

    def _apply_meta_metadata_xml(self, sess, dataset, src_value, dest_value):
        pass  # This is a no-op for most WC types, which don't store metadata XML.

    def _reset_dirty_rows(
        self, sess, base_ds, feature_filter=FeatureKeyFilter.MATCH_ALL
    ):
        """
        Reset the dirty rows recorded in the tracking table to match the originals from the dataset.

        sess - sqlalchemy session.
        base_ds - the dataset this WC table is based on.
        """
        kart_track = self.kart_tables.kart_track
        r = sess.execute(
            sa.select([kart_track.c.pk]).where(
                kart_track.c.table_name == base_ds.table_name
            )
        )
        dirty_pk_list = [row[0] for row in r]
        if not feature_filter.match_all:
            dirty_pk_list = [pk for pk in dirty_pk_list if pk in feature_filter]

        track_count = len(dirty_pk_list)
        if not dirty_pk_list:
            return

        # We're resetting the dirty rows so we don't track these changes in the tracking table.
        with self._suspend_triggers(sess, base_ds):
            # todo: suspend/remove spatial index
            L.debug("Cleaning up dirty rows...")

            count = self._delete_features_from_dataset(sess, base_ds, dirty_pk_list)
            L.debug(
                f"_reset_dirty_rows(): removed {count} features, tracking Δ count={track_count}"
            )
            count = self._write_features_from_dataset(
                sess, base_ds, dirty_pk_list, ignore_missing=True
            )
            L.debug(
                f"_reset_dirty_rows(): wrote {count} features, tracking Δ count={track_count}"
            )

            self._mark_as_clean_for_table(sess, base_ds.table_name, feature_filter)

    def soft_reset_after_commit(
        self,
        commit_or_tree,
        *,
        mark_as_clean=None,
        now_outside_spatial_filter=None,
        committed_diff=None,
    ):
        """
        Marks the working copy as now being based on the given target tree or commit.
        mark_as_clean - a RepoKeyFilter - deletes these PKs from the tracking table.
        now_outside_spatial_filter - a RepoKeyFilter - drops any features that match now_outside_spatial_filter altogether.
        """
        tree_id = commit_or_tree.peel(pygit2.Tree).hex if commit_or_tree else None
        with self.session() as sess:
            self._update_state_table_tree(sess, tree_id)
            self._mark_as_clean(sess, mark_as_clean)
            self.delete_features(
                sess, now_outside_spatial_filter, track_changes_as_dirty=False
            )


@contextlib.contextmanager
def pause_refreshing(odb):
    old_flags = odb.lookup_flags()
    odb.set_lookup_flags(pygit2.GIT_ODB_LOOKUP_NO_REFRESH)
    try:
        yield
    finally:
        odb.set_lookup_flags(old_flags)
