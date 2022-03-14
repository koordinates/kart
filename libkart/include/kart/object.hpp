#pragma once

#include <algorithm>
#include <exception>
#include <string>
#include <memory>

#include <git2.h>

#include "kart/object_type.hpp"

using namespace std;
namespace kart
{
    class KartRepo;
    class Object;
    class Tree;
    class Oid;
    class TreeEntry
    {
    public:
        // default constructor
        TreeEntry();
        // construct with no actual tree entry available (for object lookup by OID, and commit/tag objects)
        TreeEntry(KartRepo *repo);
        // construct from a git_tree_entry
        TreeEntry(git_tree_entry *e, KartRepo *repo, string path__);
        ~TreeEntry();
        // copy constructor/assignment
        TreeEntry(const TreeEntry &other);
        TreeEntry &operator=(const TreeEntry &other);
        // move constructor/assignment
        TreeEntry(TreeEntry &&other);
        TreeEntry &operator=(TreeEntry &&other);

        Object get_object();
        // accessors
        Oid id() const;
        string filename() const;
        ObjectType type() const;
        string path() const;
        KartRepo *repo() const;

    private:
        friend class Tree;
        git_tree_entry *wrapped;
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
        Tree(const TreeEntry &e, git_tree *x);
        ~Tree();
        Tree(const Tree &other);
        Tree &operator=(const Tree &other);
        Tree(Tree &&other);
        Tree &operator=(Tree &&other);
        Oid id() const;
        TreeEntry get_entry_by_path(const std::string &path) const;
        TreeEntry get_entry_by_index(size_t index) const;
        void walk(std::function<int(const std::string &, const TreeEntry &)>
                      visitor) const;
        size_t size() const;
        inline string path_with_slash() const
        {
            auto p = path();
            return p.empty() ? p : p + "/";
        }
        git_tree *wrapped;
    };
    class Blob : public BaseObject
    {
    public:
        Blob();
        Blob(const TreeEntry &e, git_blob *x);
        ~Blob();
        Blob(const Blob &other);
        Blob &operator=(const Blob &other);
        Blob(Blob &&other);
        Blob &operator=(Blob &&other);
        Oid id() const;
        // Get read-only buffer with raw contents of this blob
        const void *raw_contents() const;

        // Get size in bytes of the contents of this blob
        uint64_t raw_size() const;

        git_blob *wrapped;
    };
    class Commit
    {
    public:
        Commit(git_commit *x);
        ~Commit();
        Commit(const Commit &other);
        Commit &operator=(const Commit &other);
        Commit(Commit &&other);
        Commit &operator=(Commit &&other);
        Oid id() const;

        git_commit *wrapped;
    };
    class Tag
    {
    public:
        Tag(git_tag *x);
        ~Tag();
        Tag(const Tag &other);
        Tag &operator=(const Tag &other);
        Tag(Tag &&other);
        Tag &operator=(Tag &&other);
        Oid id() const;

        git_tag *wrapped;
    };

    class Object : public BaseObject
    {
    public:
        Object(const TreeEntry &e, git_object *x);
        Object(git_object *x);
        ~Object();
        Object(const Object &other);
        Object &operator=(const Object &other);
        Object(Object &&other);
        Object &operator=(Object &&other);
        Oid id() const;

        Object peel_until(ObjectType target);

        // Throws LibGitError if object is not a blob
        Blob as_blob();

        // Cast to commit
        // Throws LibGitError if object is not a commit
        Commit as_commit();

        // Cast to tree
        // Throws LibGitError if object is not a tree
        Tree as_tree();

        // Cast to tag
        // Throws LibGitError if object is not a tag
        Tag as_tag();
        git_object *wrapped;
    };
};
