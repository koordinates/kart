#include <exception>
#include <iostream>
#include <string>
#include <memory>

#include "kart/object.hpp"
#include "kart/repo.hpp"
#include "kart/util.hpp"

using namespace std;
using namespace kart;

TreeEntry::TreeEntry()
    : wrapped(nullptr),
      repo_(nullptr),
      path_(""){};
TreeEntry::TreeEntry(KartRepo *repo)
    : wrapped(nullptr),
      repo_(repo),
      path_(""){};
// construct from a git_tree_entry
TreeEntry::TreeEntry(git_tree_entry *e, KartRepo *repo, string path__)
    : wrapped(e),
      repo_(repo),
      path_(path__){};
TreeEntry::~TreeEntry()
{
    // it's possible to instantiate TreeEntry *without* a wrapped object
    if (wrapped)
    {
        git_tree_entry_free(wrapped);
    }
}
TreeEntry::TreeEntry(const TreeEntry &other) : repo_(other.repo_), path_(other.path_), wrapped(nullptr)
{
    if (other.wrapped)
    {
        if (git_tree_entry_dup(&wrapped, other.wrapped))
        {
            throw LibGitError();
        }
    }
}
TreeEntry &TreeEntry::operator=(const TreeEntry &other)
{
    repo_ = other.repo_;
    path_ = other.path_;
    if (wrapped)
    {
        git_tree_entry_free(wrapped);
    }
    if (other.wrapped)
    {
        if (git_tree_entry_dup(&wrapped, other.wrapped))
        {
            throw LibGitError();
        }
    }
    return *this;
}
TreeEntry::TreeEntry(TreeEntry &&other) : repo_(other.repo_), path_(other.path_), wrapped(other.wrapped)
{
    other.repo_ = nullptr;
    other.wrapped = nullptr;
}
TreeEntry &TreeEntry::operator=(TreeEntry &&other)
{
    repo_ = other.repo_;
    path_ = other.path_;
    if (wrapped)
    {
        git_tree_entry_free(wrapped);
    }
    wrapped = other.wrapped;
    other.repo_ = nullptr;
    other.wrapped = nullptr;
    return *this;
}

Object TreeEntry::get_object()
{
    Object o{*this, nullptr};
    if (git_tree_entry_to_object(&(o.wrapped), repo_->c_ptr(), wrapped))
    {
        throw LibGitError();
    }
    return o;
}
Oid TreeEntry::id() const
{
    return Oid(git_tree_entry_id(wrapped));
}
string TreeEntry::filename() const
{
    return string(git_tree_entry_name(wrapped));
}
ObjectType TreeEntry::type() const
{
    return (ObjectType)git_tree_entry_type(wrapped);
}
string TreeEntry::path() const
{
    return path_;
};
KartRepo *TreeEntry::repo() const
{
    return repo_;
}

// constructors: wrap the libgit2 objects but also keep filename/path from a TreeEntry.
Tree::Tree(const TreeEntry &e, git_tree *x) : BaseObject(e), wrapped(x) {}
Blob::Blob() : BaseObject(), wrapped() {}
Blob::Blob(const TreeEntry &e, git_blob *x) : BaseObject(e), wrapped(x) {}
Commit::Commit(git_commit *x) : wrapped(x) {}
Tag::Tag(git_tag *x) : wrapped(x) {}
Object::Object(const TreeEntry &e, git_object *x) : BaseObject(e), wrapped(x) {}

// copy constructors
Tree::Tree(const Tree &other) : BaseObject(other.entry_)
{
    if (git_tree_dup(&wrapped, other.wrapped))
    {
        throw LibGitError();
    }
}
Blob::Blob(const Blob &other) : BaseObject(other.entry_)
{
    if (git_blob_dup(&wrapped, other.wrapped))
    {
        throw LibGitError();
    }
}
Commit::Commit(const Commit &other)
{
    if (git_commit_dup(&wrapped, other.wrapped))
    {
        throw LibGitError();
    }
}
Tag::Tag(const Tag &other)
{
    if (git_tag_dup(&wrapped, other.wrapped))
    {
        throw LibGitError();
    }
}
Object::Object(const Object &other) : BaseObject(other.entry_)
{
    if (git_object_dup(&wrapped, other.wrapped))
    {
        throw LibGitError();
    }
}

// copy assignment operators
Tree &Tree::operator=(const Tree &other)
{
    if (wrapped)
    {
        git_tree_free(wrapped);
    }
    wrapped = other.wrapped;
    entry_ = other.entry_;
    return *this;
}
Blob &Blob::operator=(const Blob &other)
{
    if (wrapped)
    {
        git_blob_free(wrapped);
    }
    wrapped = other.wrapped;
    entry_ = other.entry_;
    return *this;
}
Commit &Commit::operator=(const Commit &other)
{
    if (wrapped)
    {
        git_commit_free(wrapped);
    }
    wrapped = other.wrapped;
    return *this;
}
Tag &Tag::operator=(const Tag &other)
{
    if (wrapped)
    {
        git_tag_free(wrapped);
    }
    wrapped = other.wrapped;
    return *this;
}
Object &Object::operator=(const Object &other)
{
    if (wrapped)
    {
        git_object_free(wrapped);
    }
    wrapped = other.wrapped;
    entry_ = other.entry_;
    return *this;
}

// move constructors
Tree::Tree(Tree &&other) : BaseObject(other.entry_), wrapped(other.wrapped)
{
    other.wrapped = nullptr;
}
Blob::Blob(Blob &&other) : BaseObject(other.entry_), wrapped(other.wrapped)
{
    other.wrapped = nullptr;
}
Commit::Commit(Commit &&other) : wrapped(other.wrapped)
{
    other.wrapped = nullptr;
}
Tag::Tag(Tag &&other) : wrapped(other.wrapped)
{
    other.wrapped = nullptr;
}
Object::Object(Object &&other) : BaseObject(other.entry_), wrapped(other.wrapped)
{
    other.wrapped = nullptr;
}

Tree &Tree::operator=(Tree &&other)
{
    entry_ = other.entry_;
    wrapped = other.wrapped;
    return *this;
}
Blob &Blob::operator=(Blob &&other)
{
    entry_ = other.entry_;
    wrapped = other.wrapped;
    return *this;
}
Commit &Commit::operator=(Commit &&other)
{
    wrapped = other.wrapped;
    return *this;
}
Tag &Tag::operator=(Tag &&other)
{
    wrapped = other.wrapped;
    return *this;
}
Object &Object::operator=(Object &&other)
{
    entry_ = other.entry_;
    wrapped = other.wrapped;
    return *this;
}

// destructors
Tree::~Tree()
{
    git_tree_free(wrapped);
}
Blob::~Blob()
{
    git_blob_free(wrapped);
}
Commit::~Commit()
{
    git_commit_free(wrapped);
}
Tag::~Tag()
{
    git_tag_free(wrapped);
}
Object::~Object()
{
    git_object_free(wrapped);
}

// id accessors
Oid Tree::id() const
{
    return Oid(git_tree_id(wrapped));
}
Oid Blob::id() const
{
    return Oid(git_blob_id(wrapped));
}
Oid Commit::id() const
{
    return Oid(git_commit_id(wrapped));
}
Oid Tag::id() const
{
    return Oid(git_tag_id(wrapped));
}
Oid Object::id() const
{
    return Oid(git_object_id(wrapped));
}

// Get read-only buffer with raw contents of this blob
const void *Blob::raw_contents() const
{
    const void *contents = git_blob_rawcontent(wrapped);
    if (contents == nullptr)
    {
        throw LibGitError();
    }
    return contents;
}

// Get size in bytes of the contents of this blob
uint64_t Blob::raw_size() const
{
    return git_blob_rawsize(wrapped);
}

TreeEntry Tree::get_entry_by_path(const std::string &path) const
{
    auto prefix = path_with_slash();

    TreeEntry result{nullptr, entry_.repo_, prefix + trim_trailing_slashes(path)};
    if (git_tree_entry_bypath(&result.wrapped, wrapped, path.c_str()))
    {
        throw LibGitError();
    }
    return result;
}
TreeEntry Tree::get_entry_by_index(size_t index) const
{
    auto prefix = path_with_slash();
    const git_tree_entry *e_owned_by_tree{git_tree_entry_byindex(wrapped, index)};
    git_tree_entry *e;
    if (git_tree_entry_dup(&e, e_owned_by_tree))
    {
        throw LibGitError();
    }
    return TreeEntry(e, entry_.repo_, prefix + string(git_tree_entry_name(e)));
}

void Tree::walk(std::function<int(const std::string &, const TreeEntry &)> visitor) const
{

    struct visitor_wrapper
    {
        std::function<int(const std::string &, const TreeEntry &)> fn;
        KartRepo *repo;
    };

    visitor_wrapper wrapper;
    wrapper.fn = visitor;
    wrapper.repo = entry_.repo();
    auto callback_c = [](const char *root, const git_tree_entry *const_entry,
                         void *payload)
    {
        git_tree_entry *entry;
        if (git_tree_entry_dup(&entry, const_entry))
        {
            throw LibGitError();
        }
        auto wrapper = reinterpret_cast<visitor_wrapper *>(payload);
        string parent_path = root ? std::string(root) : "";
        string entry_path = parent_path;
        if (!entry_path.empty())
        {
            entry_path += "/";
        }
        entry_path += git_tree_entry_name(entry);

        return wrapper->fn(parent_path, TreeEntry(entry, wrapper->repo, entry_path));
    };

    if (git_tree_walk(wrapped, GIT_TREEWALK_PRE, callback_c,
                      (void *)(&wrapper)))
        throw LibGitError();
}
size_t Tree::size() const
{
    return git_tree_entrycount(wrapped);
}
Object Object::peel_until(ObjectType target)
{
    Object peeled{entry_, nullptr};
    if (git_object_peel(&peeled.wrapped, wrapped, (git_object_t)target))
    {
        throw LibGitError();
    }
    return peeled;
}
Blob Object::as_blob()
{
    Blob result{entry_, nullptr};
    // TODO: can we avoid this copy? `this` is almost always a temporary...
    if (git_blob_dup(&result.wrapped, (git_blob *)wrapped))
    {
        throw LibGitError();
    }
    return result;
}
Tree Object::as_tree()
{
    Tree result{entry_, nullptr};
    // TODO: can we avoid this copy? `this` is almost always a temporary...
    if (git_tree_dup(&result.wrapped, (git_tree *)wrapped))
    {
        throw LibGitError();
    }
    return result;
}
Commit Object::as_commit()
{
    Commit result{nullptr};
    // TODO: can we avoid this copy? `this` is almost always a temporary...
    if (git_commit_dup(&result.wrapped, (git_commit *)wrapped))
    {
        throw LibGitError();
    }
    return result;
}
Tag Object::as_tag()
{
    Tag result{nullptr};
    // TODO: can we avoid this copy? `this` is almost always a temporary...
    if (git_tag_dup(&result.wrapped, (git_tag *)wrapped))
    {
        throw LibGitError();
    }
    return result;
}
