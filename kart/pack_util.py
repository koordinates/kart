import contextlib
import functools
import logging

import pygit2

L = logging.getLogger("kart.pack_util")


@functools.lru_cache(maxsize=1)
def pygit2_supports_mempack():
    """Returns True if pygit2 has the OdbBackendMemPack type."""
    try:
        return bool(pygit2.OdbBackendMemPack)
    except AttributeError:
        L.info(
            "pygit2.OdbBackendMemPack not found - Kart will write loose objects, not packfiles"
        )
        return False


@contextlib.contextmanager
def write_to_packfile(repo, mark_as_promisor=None):
    """
    As long as this contextmanager is active, any objects written to the repository will be buffered into a "MemPack"
    ODB backend. As soon as the contextmanager is closed, all the objects buffered in the MemPack are written to a
    packfile, the MemPack is discarded, and the backend ODB configuration is returned to how it was before this call
    (which generally means objects written to the repository will be written as loose objects - this is the default
    behaviour).

    repo - the repository that the objects / packfile are to be written to.
    mark_as_promisor - if True, the packfile will also be marked as a "promisor" packfile. This may be necessary in
        the case that the objects written refer to missing+promised objects from a pre-existing promisor packfile.
        If not set, the default is that the packfile will be marked as promisor if the repo is a partial clone.
    """
    if not pygit2_supports_mempack():
        yield
        return

    if mark_as_promisor is None:
        mark_as_promisor = repo.is_partial_clone

    original_odb = repo.odb
    objects_path = repo.gitdir_path / "objects"

    modified_odb = pygit2.Odb(str(objects_path))
    mempack_backend = pygit2.OdbBackendMemPack(False)
    modified_odb.add_backend(mempack_backend, 1000)
    repo.set_odb(modified_odb)

    yield

    pack_filename = mempack_backend.dump_to_pack_dir(repo)
    if mark_as_promisor:
        packfile_path = objects_path / "pack" / pack_filename
        packfile_path.with_suffix(".promisor").touch()

    repo.set_odb(original_odb)


@contextlib.contextmanager
def packfile_object_builder(repo, initial_root_tree, mark_as_promisor=None):
    """
    Yields an ObjectBuilder which writes all changes a packfile, instead of as loose objects.
    Note that this affects the given repo's backend ODB for as long as this contextmanager is active -
    avoid doing unrelated ODB writes while this contextmanger is active (unless you want them to be in
    the packfile too).
    """
    from .object_builder import ObjectBuilder

    with write_to_packfile(repo, mark_as_promisor=mark_as_promisor):
        yield ObjectBuilder(repo, initial_root_tree)
