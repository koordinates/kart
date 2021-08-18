import json
import logging

import pygit2
from sqlalchemy.exc import OperationalError

from .db import annotations_session, KartAnnotation

L = logging.getLogger(__name__)


class DiffAnnotations:
    def __init__(self, repo):
        self.repo = repo

    def _object_id(self, base, target):
        # this is actually symmetric, so we can marginally increase hit rate by sorting first
        base = base.peel(pygit2.Tree)
        target = target.peel(pygit2.Tree)
        tree_ids = sorted(r.id for r in (base, target))
        return f"{tree_ids[0]}...{tree_ids[1]}"

    def store(self, *, base, target, annotation_type, data):
        """
        Stores a diff annotation to the repo's sqlite database,
        and returns the annotation itself.

        base: base Tree or Commit object for this diff (revA in a 'revA...revB' diff)
        target: target Tree or Commit object for this diff (revB in a 'revA...revB' diff)
        """
        assert isinstance(data, dict)
        object_id = self._object_id(base, target)
        data = json.dumps(data)
        with annotations_session(self.repo) as session:
            if session.is_readonly:
                L.info("Can't store annotation; annotations.db is read-only")
            else:
                L.debug(
                    "storing: %s for %s: %s",
                    annotation_type,
                    object_id,
                    data,
                )
                try:
                    session.add(
                        KartAnnotation(
                            object_id=object_id,
                            annotation_type=annotation_type,
                            data=data,
                        )
                    )
                except OperationalError as e:
                    # ignore errors from readonly databases.
                    # this can happen if the db already existed, with tables already created,
                    # since annotations_session() wouldn't have run any CREATE commands in that
                    # case (and hence we don't yet know if the session is readonly)
                    if "readonly database" in str(e):
                        L.info("Can't store annotation; annotations.db is read-only")
                    else:
                        raise
        return data

    def get(self, *, base, target, annotation_type):
        """
        Returns a diff annotation from the sqlite database.
        Returns None if it isn't found.

        base: base Tree or Commit object for this diff (revA in a 'revA...revB' diff)
        target: target Tree or Commit object for this diff (revB in a 'revA...revB' diff)
        """
        with annotations_session(self.repo) as session:
            object_id = self._object_id(base, target)
            try:
                annotations = list(
                    session.query(KartAnnotation).filter(
                        KartAnnotation.annotation_type == annotation_type,
                        KartAnnotation.object_id == object_id,
                    )
                )
            except OperationalError as e:
                # this can happen if the db exists but is readonly and doesn't
                # contain the table yet...
                if "no such table: kart_annotations" in str(e):
                    # can't add the table to a readonly db
                    L.warning("no such table: kart_annotations")
                    return None
                else:
                    raise

            for annotation in annotations:
                data = annotation.json
                L.debug(
                    "retrieved: %s for %s: %s",
                    annotation_type,
                    object_id,
                    data,
                )
                return data
            else:
                L.debug(
                    "missing: %s for %s",
                    annotation_type,
                    object_id,
                )
                return None
