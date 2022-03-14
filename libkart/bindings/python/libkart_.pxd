from cython cimport size_t
from libc.stdint cimport int64_t
from libcpp.memory cimport unique_ptr
from libcpp.string cimport string
from libcpp.vector cimport vector


cdef extern from "kart.hpp" namespace "kart":
    cpdef enum class ObjectType "kart::ObjectType":
        any = -2
        invalid = -1
        commit = 1
        tree = 2
        blob = 3
        tag = 4
        ofs_delta = 6
        ref_delta = 7

    cdef cppclass CppOid "kart::Oid":
        string to_hex_string()

    cdef cppclass CppTreeEntry "kart::TreeEntry":
        string path()
        string filename()
        CppOid id()
        ObjectType type()
    cdef cppclass CppBlob "kart::Blob":
        CppOid id()
        string path()
        string filename()
        void* raw_contents()
        int64_t raw_size()
    cdef cppclass CppTree "kart::Tree":
        CppOid id()
        string path()
        string filename()
        CppTreeEntry get_entry_by_path(const string)
        CppTreeEntry get_entry_by_index(size_t index)
        size_t size()
    cdef cppclass CppCommit "kart::Commit":
        CppOid id()
    cdef cppclass CppObject "kart::Object":
        CppOid id()
        ObjectType type()
        string path()
        string filename()

        CppBlob as_blob()
        CppTree as_tree()
        CppCommit as_commit()

    cdef cppclass CppTreeEntryIterator "kart::TreeEntryIterator":
        CppTreeEntry operator*()
        CppTreeEntryIterator operator++()
        bint operator==(CppTreeEntryIterator)
        bint operator!=(CppTreeEntryIterator)

    cdef cppclass CppTreeWalker "kart::TreeWalker":
        CppTreeEntryIterator begin()
        CppTreeEntryIterator end()

    cdef cppclass CppBlobIterator "kart::BlobIterator":
        CppBlob operator*()
        CppBlobIterator operator++()
        bint operator==(CppBlobIterator)
        bint operator!=(CppBlobIterator)

    cdef cppclass CppBlobWalker "kart::BlobWalker":
        CppBlobIterator begin()
        CppBlobIterator end()

    cdef cppclass CppDataset3 "kart::Dataset3":
        unique_ptr[CppTree] get_tree() except +
        unique_ptr[CppTree] get_feature_tree() except +
        unique_ptr[CppBlobWalker] feature_blobs() except +


    cdef cppclass CppRepoStructure "kart::RepoStructure":
        vector[CppDataset3*]* GetDatasets()

    cdef cppclass CppKartRepo "kart::KartRepo":
        CppKartRepo(const char *path)
        int Version()
        unique_ptr[CppRepoStructure] Structure(string treeish)
        unique_ptr[CppTreeWalker] walk_tree(CppTree* root)


    # KartRepo* kart_open_repository(const char *path)
    # void kart_close_repository(KartRepo* repo)
    # int kart_repo_version(KartRepo* repo)
