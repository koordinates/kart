#include <exception>
#include <iostream>
#include <string>
#include <memory>

#include "kart/repo.hpp"

using namespace std;
using namespace kart;

// constructors
KartRepo::KartRepo(string path)
{
    assert(!git_libgit2_opts(GIT_OPT_ENABLE_STRICT_HASH_VERIFICATION, 0));
    assert(!git_libgit2_opts(GIT_OPT_SET_CACHE_OBJECT_LIMIT, 2, 100000));
    if (git_repository_open(&wrapped, path.c_str()))
    {
        throw LibGitError();
    }
}
KartRepo::~KartRepo()
{
}

// git wrappers

Object KartRepo::revparse_to_object(const std::string &spec)
{
    Object out{TreeEntry(this), nullptr};
    if (git_revparse_single(&out.wrapped, wrapped, spec.c_str()))
    {
        throw LibGitError();
    }
    return out;
}
Object KartRepo::lookup_object(Oid id, ObjectType type, TreeEntry entry)
{
    Object out{entry, nullptr};
    if (git_object_lookup(&out.wrapped, wrapped, id.wrapped, static_cast<git_object_t>(type)))
    {
        throw LibGitError();
    }
    return out;
}
Object KartRepo::lookup_object(Oid id, ObjectType type)
{
    return lookup_object(id, type, TreeEntry(this));
}

int kart::KartRepo::Version()
{
    auto structure = Structure("HEAD");
    return structure->Version();
}
unique_ptr<RepoStructure> KartRepo::Structure()
{
    return KartRepo::Structure("HEAD");
}
unique_ptr<RepoStructure> KartRepo::Structure(string treeish)
{
    git_object *parsed;
    git_object *tree;
    if (git_revparse_single(&parsed, wrapped, treeish.c_str()))
    {
        throw LibGitError();
    }
    if (git_object_peel(&tree, parsed, GIT_OBJECT_TREE))
    {
        throw LibGitError();
    }
    auto t = Tree(TreeEntry(this), (git_tree *)tree);
    return make_unique<RepoStructure>(this, t);
}
unique_ptr<TreeWalker> KartRepo::walk_tree(Tree *root)
{
    return make_unique<TreeWalker>(this, root);
}

git_repository *KartRepo::c_ptr()
{
    return wrapped;
}
