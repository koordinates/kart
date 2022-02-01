#include <exception>
#include <iostream>
#include <string>
#include <memory>

#include <cppgit2/repository.hpp>

#include "kart/repo.hpp"

using namespace std;
using namespace kart;

KartRepo::KartRepo(const char *path)
    : repo(repository::open(path))
{
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
    auto object = repo.revparse_to_object(treeish);
    auto tree = object.peel_until(object::object_type::tree).as_tree();
    return make_unique<RepoStructure>(&repo, tree);
}
