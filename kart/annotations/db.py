import contextlib
import json
import threading
from sqlalchemy import Column, Integer, Text
from sqlalchemy.exc import OperationalError
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

from kart.sqlalchemy.create_engine import sqlite_engine

Base = declarative_base()


class KartAnnotation(Base):
    __tablename__ = "kart_annotations"
    id = Column(Integer, nullable=False, primary_key=True)
    # TODO: look into what indexes we should have here
    object_type = Column(Text, nullable=False)
    object_id = Column(Text, nullable=False)
    annotation_type = Column(Text, nullable=False)
    data = Column(Text, nullable=False)

    def __repr__(self):
        return f"<KartAnnotation({self.annotation_type})>"

    @property
    def json(self):
        return json.loads(self.data)


_local = threading.local()


@contextlib.contextmanager
def annotations_session(repo):
    s = getattr(_local, "session", None)
    if s:
        # make this contextmanager reentrant
        yield s
    else:
        annotations_path = repo.gitdir_path / "annotations.db"

        engine = sqlite_engine(annotations_path)
        sm = sessionmaker(bind=engine)
        with sm() as s:
            Base.metadata.create_all(engine)
            _local.session = s
            try:
                yield s
                s.commit()
            finally:
                _local.session = None


def is_db_writable(session):
    try:
        session.execute("PRAGMA user_version=0;")
    except OperationalError as e:
        if "readonly database" in str(e):
            return False
        else:
            raise
    else:
        return True
