#pragma once

#include <algorithm>
#include <exception>
#include <string>
#include <memory>
#include <cppgit2/repository.hpp>

using namespace std;
namespace kart
{
    class KartRepo;
    class Object;
    class Tree;
    class TreeEntry
    {
    public:
        // default constructor
        TreeEntry();
        // construct with no actual tree entry available (for object lookup by OID, and commit/tag objects)
        TreeEntry(KartRepo *repo);
        // construct from a tree::entry
        TreeEntry(const cppgit2::tree::entry &e, KartRepo *repo, string path__);
        Object get_object();
        // accessors
        cppgit2::oid id() const;
        string filename() const;
        cppgit2::object::object_type type() const;
        string path() const;
        KartRepo *repo() const;

    private:
        friend class Tree;
        cppgit2::tree::entry wrapped;
        KartRepo *repo_;
        string path_;
    };

    class BaseObject
    {
    public:
        BaseObject() : entry_(){};
        BaseObject(const TreeEntry &e) : entry_(e){};
        inline string filename() const
        {
            return entry_.filename();
        };
        inline string path() const
        {
            return entry_.path();
        };

    protected:
        TreeEntry entry_;
    };
    class Tree : public BaseObject
    {
    public:
        Tree(const TreeEntry &e, const cppgit2::tree &x);
        cppgit2::oid id() const;
        std::vector<TreeEntry> entries();
        TreeEntry lookup_entry_by_path(const std::string &path) const;
        TreeEntry lookup_entry_by_name(const std::string &name) const;
        TreeEntry lookup_entry_by_index(size_t index) const;
        void walk(cppgit2::tree::traversal_mode mode,
                  std::function<int(const std::string &, const TreeEntry &)>
                      visitor) const;
        size_t size() const;
        inline string path_with_slash() const
        {
            auto p = path();
            return p.empty() ? p : p + "/";
        }

    private:
        cppgit2::tree wrapped;
    };
    class Blob : public BaseObject
    {
    public:
        Blob();
        Blob(const TreeEntry &e, const cppgit2::blob &x);
        cppgit2::oid id() const;
        // Get read-only buffer with raw contents of this blob
        const void *raw_contents() const;

        // Get size in bytes of the contents of this blob
        cppgit2::blob_size raw_size() const;

    private:
        cppgit2::blob wrapped;
    };
    class Commit
    {
    public:
        Commit(const cppgit2::commit &x);
        cppgit2::oid id() const;

    private:
        cppgit2::commit wrapped;
    };
    class Tag
    {
    public:
        Tag(const cppgit2::tag &x);
        cppgit2::oid id() const;

    private:
        cppgit2::tag wrapped;
    };

    class Object : public BaseObject
    {
    public:
        Object(const TreeEntry &e, const cppgit2::object &x);
        Object(const cppgit2::object &x);
        cppgit2::oid id() const;

        Object peel_until(cppgit2::object::object_type target);

        // Throws git_exception if object is not a blob
        Blob as_blob();

        // Cast to commit
        // Throws git_exception if object is not a commit
        Commit as_commit();

        // Cast to tree
        // Throws git_exception if object is not a tree
        Tree as_tree();

        // Cast to tag
        // Throws git_exception if object is not a tag
        Tag as_tag();

    private:
        cppgit2::object wrapped;
    };
};
