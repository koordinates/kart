#include <exception>
#include <iostream>
#include <string>
#include <memory>

#include "kart/structure.hpp"

using namespace std;
using namespace kart;

RepoStructure::RepoStructure(KartRepo *repo, Tree root_tree_)
    : repo(repo), root_tree(root_tree_)
{
}

int kart::RepoStructure::Version()
{
    auto entry = root_tree.get_entry_by_path(".kart.repostructure.version");
    if (entry.type() != ObjectType::blob)
    {
        throw LibKartError("kart repo version didn't resolve to a blob");
    }

    auto blob = entry.get_object().as_blob();
    string content = string(static_cast<const char *>(blob.raw_contents()), blob.raw_size());

    return stoi(content);
}
vector<Dataset3 *> *RepoStructure::GetDatasets()
{
    auto result = new vector<Dataset3 *>();
    root_tree.walk(
        [&](const string &parent_path, const TreeEntry &entry)
        {
            auto type = entry.type();
            if (type == ObjectType::tree && entry.filename() == DATASET_DIRNAME)
            {
                auto oid = entry.id();
                // get parent tree; that's what the dataset uses as its root tree.
                auto parent_entry = root_tree.get_entry_by_path(parent_path);
                auto parent_tree = parent_entry.get_object().as_tree();

                result->push_back(new Dataset3(repo, parent_tree));
                return 1;
            }
            return 0;
        });
    return result;
}
