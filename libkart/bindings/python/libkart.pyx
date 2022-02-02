# cython: c_string_type=unicode, c_string_encoding=utf8

from cython.operator cimport dereference as deref, preincrement as preinc, address
from libcpp.memory cimport unique_ptr
from libcpp.string cimport string
from libcpp.utility cimport move

from libkart_ cimport *


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
    cdef cppgit2_tree_entry cpp
    @property
    def filename(self):
        return self.cpp.filename()
    @property
    def id(self):
        return Oid._wrap(self.cpp.id())

    def __repr__(self):
        return f"<TreeEntry: {self.filename}>"

    @staticmethod
    cdef TreeEntry _wrap(cppgit2_tree_entry cpp):
        cdef TreeEntry x = TreeEntry()
        x.cpp = cpp
        return x

cdef class TreeEntryWithPath:
    cdef CppTreeEntryWithPath cpp
    @property
    def filename(self):
        return self.cpp.filename()
    @property
    def id(self):
        return Oid._wrap(self.cpp.id())
    @property
    def rel_path(self):
        return self.cpp.rel_path

    def __repr__(self):
        return f"<TreeEntryWithPath: {self.rel_path}>"

    @staticmethod
    cdef TreeEntryWithPath _wrap(CppTreeEntryWithPath cpp):
        cdef TreeEntryWithPath x = TreeEntryWithPath()
        x.cpp = cpp
        return x


cdef class Tree:
    cdef unique_ptr[cppgit2_tree] thisptr
    @property
    def id(self):
        return Oid._wrap(deref(self.thisptr).id())

    def __repr__(self):
        return f"<Tree: {self.id}>"

    @staticmethod
    cdef Tree _wrap(unique_ptr[cppgit2_tree] cpp):
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
            yield TreeEntryWithPath._wrap(deref(it))
            it = preinc(it)


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
