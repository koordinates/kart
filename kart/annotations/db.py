import contextlib
import json
import logging
import threading

from kart.sqlalchemy.sqlite import sqlite_engine
from sqlalchemy import Column, Integer, Text, UniqueConstraint
from sqlalchemy.exc import OperationalError
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.schema import CreateTable

L = logging.getLogger(__name__)
Base = declarative_base()


class KartAnnotation(Base):
    __tablename__ = "kart_annotations"
    id = Column(Integer, nullable=False, primary_key=True)
    object_id = Column(Text, nullable=False)
    annotation_type = Column(Text, nullable=False)
    data = Column(Text, nullable=False)
    __table_args__ = (
        UniqueConstraint(
            "annotation_type",
            "object_id",
            name="kart_annotations_multicol",
            sqlite_on_conflict="REPLACE",
        ),
    )

    def __repr__(self):
        return f"<KartAnnotation({self.annotation_type})>"

    @property
    def json(self):
        return json.loads(self.data)


_local = threading.local()


@contextlib.contextmanager
def ignore_readonly_db(session):
    try:
        yield
    except OperationalError as e:
        # ignore errors from readonly databases.
        if "readonly database" in str(e):
            L.info("Can't store annotation; annotations.db is read-only")
            session.rollback()
        else:
            raise


@contextlib.contextmanager
def _annotations_session(db_path):
    engine = sqlite_engine(db_path)
    sm = sessionmaker(bind=engine)
    with sm() as s:
        s.is_readonly = None
        try:
            s.execute(CreateTable(KartAnnotation.__table__, if_not_exists=True))
        except OperationalError as e:
            # ignore errors from readonly databases.
            if "readonly database" in str(e):
                L.info("Can't create tables; annotations.db is read-only")
                s.rollback()
                s.is_readonly = True
            else:
                raise

        _local.session = s
        try:
            yield s
            s.commit()
        finally:
            del s.is_readonly
            _local.session = None


@contextlib.contextmanager
def annotations_session(repo):
    s = getattr(_local, "session", None)
    if s:
        # make this contextmanager reentrant
        yield s
    else:
        ctx = _annotations_session(str(repo.gitdir_path / "annotations.db"))
        with contextlib.ExitStack() as stack:
            try:
                s = stack.enter_context(ctx)
            except OperationalError as e:
                if "unable to open database file" in str(e):
                    # can't create a database in a readonly dir.
                    # but we still need _some_ sqlalchemy session to yield,
                    # otherwise all the annotations code will have to handle this specifically.
                    # so we create a in-memory database and make it look readonly
                    L.info(
                        "Failed to create database file; falling back to in-memory storage"
                    )
                    with _annotations_session(":memory:") as s:
                        s.is_readonly = True
                        yield s
                else:
                    raise
            else:
                yield s


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
