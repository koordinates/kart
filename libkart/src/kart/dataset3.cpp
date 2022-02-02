#include <exception>
#include <iostream>
#include <string>
#include <memory>

#include "kart/dataset3.hpp"

using namespace std;
using namespace cppgit2;
using namespace kart;

Dataset3::Dataset3(repository *repo, tree tree_, string path)
    : repo(repo), tree_(tree_), path(path)
{
}

unique_ptr<cppgit2::tree> Dataset3::get_tree()
{
    return make_unique<tree>(tree_);
}
unique_ptr<cppgit2::tree> Dataset3::get_features_tree()
{
    auto entry = tree_.lookup_entry_by_path(DATASET_DIRNAME + "/features");
    auto features_tree = repo->lookup_tree(entry.id());
    return make_unique<tree>(features_tree);
}
