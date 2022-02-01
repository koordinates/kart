# cython: c_string_type=unicode, c_string_encoding=utf8

from cython.operator cimport dereference as deref
from libcpp.memory cimport unique_ptr
from libcpp.string cimport string
from libcpp.utility cimport move

from libkart_ cimport *


cdef class Dataset3:
    cdef CppDataset3* thisptr

    @property
    def path(self):
        return deref(self.thisptr).path

    def __repr__(self):
        return f"<libkart.Dataset3: {self.path}>"

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

    def __dealloc__(self):
        if self.thisptr:
            del self.thisptr
