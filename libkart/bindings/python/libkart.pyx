# cython: c_string_type=unicode, c_string_encoding=utf8
from cython.operator cimport dereference as deref, preincrement as preinc, address
from libcpp.memory cimport unique_ptr, make_unique
from libcpp.string cimport string
from libcpp.utility cimport move

from libkart_ cimport *
from enum import Enum

# FIXME: In *theory* the `cpdef enum class` in libkart_.pxd should mean we can just use `object_type`,
# because it should be usable in a Python context.
# However, in practice it doesn't seem to work at all.
# I think the `cimport` above just imports the C-ish symbol and not the Python one.
# And you can't `import` from libkart_.pxd. So I'm not sure how you're meant to use the cpdef enum.
# So here we work around it by just defining a python-style enum and using it below.
class ObjectType(Enum):
    # cppgit2::object::object_type
    any = -2
    invalid = -1
    commit = 1
    tree = 2
    blob = 3
    tag = 4
    ofs_delta = 6
    ref_delta = 7

cdef class Oid:
    cdef cppgit2_oid cpp

    def __str__(self):
        return self.cpp.to_hex_string()

    def __repr__(self):
        return f'<Oid: {self}>'

    @staticmethod
    cdef Oid _wrap(cppgit2_oid cpp):
        cdef Oid x = Oid()
        x.cpp = cpp
        return x


cdef class TreeEntry:
    cdef CppTreeEntry cpp
    @property
    def filename(self):
        return self.cpp.filename()
    @property
    def id(self):
        return Oid._wrap(self.cpp.id())
    @property
    def type(self):
        return ObjectType(self.cpp.type())
    @property
    def path(self):
        return self.cpp.path()

    def __repr__(self):
        return f"<TreeEntry: {self.path}>"

    @staticmethod
    cdef TreeEntry _wrap(CppTreeEntry cpp):
        cdef TreeEntry x = TreeEntry()
        x.cpp = cpp
        return x


cdef class Tree:
    cdef unique_ptr[CppTree] thisptr
    @property
    def id(self):
        return Oid._wrap(deref(self.thisptr).id())

    def __repr__(self):
        return f"<Tree: {self.id}>"

    def entries(self):
        entries = deref(self.thisptr).entries()
        return [
            TreeEntry._wrap(e) for e in entries
        ]

    @staticmethod
    cdef Tree _wrap(unique_ptr[CppTree] cpp):
        cdef Tree x = Tree()
        x.thisptr = move(cpp)
        return x



cdef class TreeWalker:
    cdef unique_ptr[CppTreeWalker] thisptr
    @staticmethod
    cdef TreeWalker _wrap(unique_ptr[CppTreeWalker] cpp):
        cdef TreeWalker x = TreeWalker()
        x.thisptr = move(cpp)
        return x

    def __iter__(self):
        cdef CppTreeEntryIterator it = deref(self.thisptr).begin()
        while it != deref(self.thisptr).end():
            yield TreeEntry._wrap(deref(it))
            preinc(it)


cdef class Blob:
    cdef unique_ptr[CppBlob] thisptr

    @property
    def id(self):
        return Oid._wrap(deref(self.thisptr).id())

    def get_size(self):
        return deref(self.thisptr).raw_size()

    def get_contents(self):
        # TODO: memoize this. how? cython seems to make this pretty damn hard
        cdef size_t size = deref(self.thisptr).raw_size()
        cdef const void* raw_contents = deref(self.thisptr).raw_contents()
        # FIXME: this does a copy; see below for TODO
        cdef string cpp_string = string(<char*>raw_contents, size)
        return <bytes>cpp_string

    # TODO: use the buffer interface to prevent copying.
    # This doesn't quite compile because `raw_contents()` returns a `const void *`,
    # and `buffer.buf` wants a `void *`
    #     def __getbuffer__(self, Py_buffer *buffer, int flags):
    #         contents = deref(self.thisptr).raw_contents()
    #         size = deref(self.thisptr).raw_size()
    #
    #         buffer.buf = contents
    #         buffer.format = 'c'
    #         buffer.internal = NULL
    #         buffer.itemsize = 1
    #         buffer.len = size;
    #         buffer.ndim = 1
    #         buffer.obj = self
    #         buffer.readonly = 1
    #         buffer.shape = [size]
    #         buffer.strides = [1]
    #         buffer.suboffsets = NULL
    #
    #     def __releasebuffer__(self, Py_buffer *buffer):
    #         pass


    def __repr__(self):
        return f"<Blob: {self.id}>"

    @staticmethod
    cdef Blob _wrap(unique_ptr[CppBlob] cpp):
        cdef Blob x = Blob()
        x.thisptr = move(cpp)
        return x


cdef class BlobWalker:
    cdef unique_ptr[CppBlobWalker] thisptr
    @staticmethod
    cdef BlobWalker _wrap(unique_ptr[CppBlobWalker] cpp):
        cdef BlobWalker x = BlobWalker()
        x.thisptr = move(cpp)
        return x

    def __iter__(self):
        cdef CppBlobIterator it = deref(self.thisptr).begin()
        while it != deref(self.thisptr).end():
            yield Blob._wrap(make_unique[CppBlob](deref(it)))
            preinc(it)


cdef class Dataset3:
    cdef CppDataset3* thisptr

    @property
    def path(self):
        return deref(self.thisptr).path

    def __repr__(self):
        return f"<libkart.Dataset3: {self.path}>"

    @property
    def tree(self):
        return Tree._wrap(deref(self.thisptr).get_tree())

    @property
    def features_tree(self):
        return Tree._wrap(deref(self.thisptr).get_features_tree())

    def feature_blobs(self):
        return BlobWalker._wrap(
            deref(self.thisptr).feature_blobs()
        )


    @staticmethod
    cdef Dataset3 _wrap(CppDataset3 *cpp):
        cdef Dataset3 x = Dataset3()
        x.thisptr = cpp
        return x
    def __dealloc__(self):
        if self.thisptr:
            del self.thisptr


cdef class RepoStructure:
    cdef unique_ptr[CppRepoStructure] thisptr

    # FIXME: RepoStructure doesn't really *need* a constructor.
    # The only way to create one is to call KartRepo.structure(...)
    # That method needs to wrap a CppRepoStructure object with a RepoStructure object,
    # But there appears to be no way to do that without this weird static wrap method.
    # In addition this implies the default constructor with no arguments,
    # which isn't really what we want - in fact ideally we'd *prevent* that being called.
    #
    # https://groups.google.com/g/cython-users/c/6I2HMUTPT6o
    # TODO: figure out how to improve this.

    @staticmethod
    cdef RepoStructure _wrap(unique_ptr[CppRepoStructure] cpp):
        cdef RepoStructure rs = RepoStructure()
        rs.thisptr = move(cpp)
        return rs

    def datasets(self):
        datasets = deref(deref(self.thisptr).GetDatasets())
        return [Dataset3._wrap(ds) for ds in datasets]


cdef class KartRepo:
    cdef CppKartRepo* thisptr

    def __cinit__(self, path: str):
        self.thisptr = new CppKartRepo(path)

    @property
    def version(self):
        return deref(self.thisptr).Version()

    def structure(self, treeish: str = "HEAD"):
        structure = deref(self.thisptr).Structure(treeish)
        return RepoStructure._wrap(move(structure))

    def walk_tree(self, root: Tree):
        return TreeWalker._wrap(
            deref(self.thisptr).walk_tree(address(deref(root.thisptr)))
        )

    def __dealloc__(self):
        if self.thisptr:
            del self.thisptr
