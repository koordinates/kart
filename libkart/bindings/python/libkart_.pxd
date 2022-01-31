from libcpp.memory cimport shared_ptr
from libcpp.string cimport string
from libcpp.vector cimport vector

cdef extern from "kart.hpp" namespace "kart":
    cdef cppclass CppDataset3 "kart::Dataset3":
        const string path
    cdef cppclass CppRepoStructure "kart::RepoStructure":
        vector[CppDataset3*]* GetDatasets();

    cdef cppclass CppKartRepo "kart::KartRepo":
        CppKartRepo(const char *path)
        int Version()
        shared_ptr[CppRepoStructure] Structure(string treeish)


    # KartRepo* kart_open_repository(const char *path)
    # void kart_close_repository(KartRepo* repo)
    # int kart_repo_version(KartRepo* repo)
