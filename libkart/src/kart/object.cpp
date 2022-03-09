#include <exception>
#include <iostream>
#include <string>
#include <memory>

#include "kart/object.hpp"
#include "kart/repo.hpp"

using namespace std;
using namespace kart;

TreeEntry::TreeEntry()
    : wrapped(),
      repo_(nullptr),
      path_(""){};
TreeEntry::TreeEntry(KartRepo *repo)
    : wrapped(),
      repo_(repo),
      path_(""){};
// construct from a cppgit2::tree::entry
TreeEntry::TreeEntry(const cppgit2::tree::entry &e, KartRepo *repo, string path__)
    : wrapped(e),
      repo_(repo),
      path_(path__){};

Object TreeEntry::get_object()
{
    return repo_->lookup_object(wrapped.id(), wrapped.type(), *this);
}
cppgit2::oid TreeEntry::id() const
{
    return wrapped.id();
}
string TreeEntry::filename() const
{
    return wrapped.filename();
}
cppgit2::object::object_type TreeEntry::type() const
{
    return wrapped.type();
}
string TreeEntry::path() const
{
    return path_;
};
KartRepo *TreeEntry::repo() const
{
    return repo_;
}

// constructors: wrap the cppgit2 objects but also keep filename/path from a TreeEntry.
Tree::Tree(const TreeEntry &e, const cppgit2::tree &x) : BaseObject(e), wrapped(x) {}
Blob::Blob() : BaseObject(), wrapped() {}
Blob::Blob(const TreeEntry &e, const cppgit2::blob &x) : BaseObject(e), wrapped(x) {}
Commit::Commit(const cppgit2::commit &x) : wrapped(x) {}
Tag::Tag(const cppgit2::tag &x) : wrapped(x) {}
Object::Object(const TreeEntry &e, const cppgit2::object &x) : BaseObject(e), wrapped(x) {}

// id accessors
cppgit2::oid Tree::id() const
{
    return wrapped.id();
}
cppgit2::oid Blob::id() const
{
    return wrapped.id();
}
cppgit2::oid Commit::id() const
{
    return wrapped.id();
}
cppgit2::oid Tag::id() const
{
    return wrapped.id();
}
cppgit2::oid Object::id() const
{
    return wrapped.id();
}

// Get read-only buffer with raw contents of this blob
const void *Blob::raw_contents() const
{
    return wrapped.raw_contents();
}

// Get size in bytes of the contents of this blob
cppgit2::blob_size Blob::raw_size() const
{
    return wrapped.raw_size();
}

std::vector<TreeEntry> Tree::entries()
{
    auto result = std::vector<TreeEntry>();
    auto prefix = path_with_slash();
    for (auto e : wrapped.entries())
    {
        result.push_back(TreeEntry(e, entry_.repo(), prefix + e.filename()));
    }
    return result;
}
TreeEntry Tree::lookup_entry_by_path(const std::string &path) const
{
    auto prefix = path_with_slash();
    auto e = wrapped.lookup_entry_by_path(path);
    return TreeEntry{e, entry_.repo(), prefix + path};
}
TreeEntry Tree::lookup_entry_by_name(const std::string &name) const
{
    auto prefix = path_with_slash();
    auto e = wrapped.lookup_entry_by_name(name);
    return TreeEntry{e, entry_.repo(), prefix + name};
}

void Tree::walk(cppgit2::tree::traversal_mode mode,
                std::function<int(const std::string &, const TreeEntry &)>
                    visitor) const
{
    struct visitor_wrapper
    {
        std::function<int(const std::string &, const TreeEntry &)> fn;
        KartRepo *repo;
    };

    visitor_wrapper wrapper;
    wrapper.fn = visitor;
    wrapper.repo = entry_.repo();

    auto callback_c = [](const char *root, const git_tree_entry *entry,
                         void *payload)
    {
        auto wrapper = reinterpret_cast<visitor_wrapper *>(payload);
        cppgit2::tree::entry cppgit2_entry{entry};
        string parent_path = root ? std::string(root) : "";
        string entry_path = parent_path;
        if (!entry_path.empty())
        {
            entry_path += "/";
        }
        entry_path += cppgit2_entry.filename();

        return wrapper->fn(parent_path, TreeEntry(cppgit2_entry, wrapper->repo, entry_path));
    };

    if (git_tree_walk(wrapped.c_ptr(), static_cast<git_treewalk_mode>(mode), callback_c,
                      (void *)(&wrapper)))
        throw git_exception();
}
Object Object::peel_until(cppgit2::object::object_type target)
{
    auto obj{wrapped.peel_until(target)};
    return Object{entry_, obj};
}
Blob Object::as_blob()
{
    auto x{wrapped.as_blob()};
    return Blob{entry_, x};
}
Tree Object::as_tree()
{
    auto x{wrapped.as_tree()};
    return Tree{entry_, x};
}
Commit Object::as_commit()
{
    auto x{wrapped.as_commit()};
    return Commit{x};
}
Tag Object::as_tag()
{
    auto x{wrapped.as_tag()};
    return Tag{x};
}
