import json
import logging
from .db import annotations_session, KartAnnotation

L = logging.getLogger(__name__)


class DiffAnnotations:
    def __init__(self, repo):
        self.repo = repo

    def _object_id(self, base_rs, target_rs):
        # this is actually symmetric, so we can marginally increase hit rate by sorting first
        if base_rs.tree.id < target_rs.tree.id:
            return f"{base_rs.tree.id}...{target_rs.tree.id}"
        else:
            return f"{target_rs.tree.id}...{base_rs.tree.id}"

    def store(self, *, base_rs, target_rs, annotation_type, data):
        """
        Stores a diff annotation to the repo's sqlite database,
        and returns the annotation itself.
        """
        assert isinstance(data, dict)
        with annotations_session(self.repo) as session:
            object_id = self._object_id(base_rs, target_rs)
            data = json.dumps(data)
            L.debug(
                "storing: %s for %s: %s",
                annotation_type,
                object_id,
                data,
            )
            session.add(
                KartAnnotation(
                    object_type="diff",
                    object_id=object_id,
                    annotation_type=annotation_type,
                    data=data,
                )
            )
            return data

    def get(self, *, base_rs, target_rs, annotation_type):
        """
        Returns a diff annotation from the sqlite database.
        Returns None if it isn't found.
        """
        with annotations_session(self.repo) as session:
            object_id = self._object_id(base_rs, target_rs)
            for annotation in session.query(KartAnnotation).filter(
                KartAnnotation.object_type == "diff",
                KartAnnotation.annotation_type == annotation_type,
                KartAnnotation.object_id == object_id,
            ):
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
                    "failed to retrieve: %s for %s",
                    annotation_type,
                    object_id,
                )
                return None
