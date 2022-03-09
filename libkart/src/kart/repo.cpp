#include <exception>
#include <iostream>
#include <string>
#include <memory>

#include <cppgit2/repository.hpp>

#include "kart/repo.hpp"

using namespace std;
using namespace kart;

// constructors
KartRepo::KartRepo(string path)
    : wrapped(repository::open(path))
{
}

// git wrappers

Object KartRepo::revparse_to_object(const std::string &spec)
{
    auto obj = wrapped.revparse_to_object(spec);
    return Object(TreeEntry(this), obj);
}
Object KartRepo::lookup_object(cppgit2::oid id, cppgit2::object::object_type type, TreeEntry entry)
{
    auto obj = wrapped.lookup_object(id, type);
    return Object(entry, obj);
}
Object KartRepo::lookup_object(cppgit2::oid id, cppgit2::object::object_type type)
{
    return lookup_object(id, type, TreeEntry(this));
}

// kart stuff
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
    auto object = revparse_to_object(treeish);
    auto tree = object.peel_until(cppgit2::object::object_type::tree).as_tree();
    return make_unique<RepoStructure>(this, tree);
}
unique_ptr<TreeWalker> KartRepo::walk_tree(Tree *root)
{
    return make_unique<TreeWalker>(this, root);
}
