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

const int kart::KartRepo::Version() const
{
    auto head = repo.head();
    //	clog << "got head " << head.name() << "\n";
    auto head_commit = repo.lookup_commit(head.target());
    //	clog << "got commit " << head_commit.id() << "\n";
    auto head_tree = head_commit.tree();
    //	clog << "got tree " << head_tree.id() << "\n";

    auto entry = head_tree.lookup_entry_by_path(".kart.repostructure.version");
    //	clog << "got entry " << entry.type() << "\n";

    if (entry.type() != object::object_type::blob)
    {
        throw LibKartError("kart repo version didn't resolve to a blob");
    }

    auto blob = repo.tree_entry_to_object(entry).as_blob();
    //	clog << "got blob\n";
    string content = string(static_cast<const char *>(blob.raw_contents()), blob.raw_size());

    return stoi(content);
}
RepoStructure *KartRepo::Structure()
{
    return KartRepo::Structure("HEAD");
}
RepoStructure *KartRepo::Structure(string treeish)
{
    auto object = repo.revparse_to_object(treeish);
    auto tree = object.peel_until(object::object_type::tree).as_tree();
    return new RepoStructure(&repo, tree);
}
