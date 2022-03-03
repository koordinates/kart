from libcpp.memory cimport unique_ptr
from libcpp.string cimport string
from libcpp.vector cimport vector

cdef extern from "cppgit2/object.hpp" namespace "cppgit2::object":
    cpdef enum class object_type "cppgit2::object::object_type":
        # cppgit2::object::object_type
        any = -2
        invalid = -1
        commit = 1
        tree = 2
        blob = 3
        tag = 4
        ofs_delta = 6
        ref_delta = 7


cdef extern from "kart.hpp" namespace "kart":
    # cppgit stuff

    cdef cppclass cppgit2_oid "cppgit2::oid":
        string to_hex_string()
    cdef cppclass cppgit2_tree_entry "cppgit2::tree::entry":
        string filename()
        cppgit2_oid id()
        object_type type()
    cdef cppclass cppgit2_tree "cppgit2::tree":
        cppgit2_oid id()
        vector[cppgit2_tree_entry] entries()

    # actual libkart stuff
    cdef cppclass CppTreeEntryWithPath "kart::TreeEntryWithPath":
        string rel_path()
        string filename()
        cppgit2_oid id()
        object_type type()

    cdef cppclass CppTreeEntryIterator "kart::TreeEntryIterator":
        CppTreeEntryWithPath operator*()
        CppTreeEntryIterator operator++()
        bint operator==(CppTreeEntryIterator)
        bint operator!=(CppTreeEntryIterator)

    cdef cppclass CppTreeWalker "kart::TreeWalker":
        CppTreeEntryIterator begin()
        CppTreeEntryIterator end()

    cdef cppclass CppDataset3 "kart::Dataset3":
        const string path
        unique_ptr[cppgit2_tree] get_tree()
        unique_ptr[cppgit2_tree] get_features_tree()


    cdef cppclass CppRepoStructure "kart::RepoStructure":
        vector[CppDataset3*]* GetDatasets()

    cdef cppclass CppKartRepo "kart::KartRepo":
        CppKartRepo(const char *path)
        int Version()
        unique_ptr[CppRepoStructure] Structure(string treeish)
        unique_ptr[CppTreeWalker] walk_tree(cppgit2_tree* root)


    # KartRepo* kart_open_repository(const char *path)
    # void kart_close_repository(KartRepo* repo)
    # int kart_repo_version(KartRepo* repo)
