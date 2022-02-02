#pragma once

#include <exception>
#include <string>
#include <memory>
#include <cppgit2/repository.hpp>

using namespace std;
namespace kart
{
    const string DATASET_DIRNAME = ".table-dataset";

    class Dataset3
    {
    public:
        Dataset3(cppgit2::repository *repo, cppgit2::tree tree_, string path);
        string path;

        unique_ptr<cppgit2::tree> get_tree();
        unique_ptr<cppgit2::tree> get_features_tree();

    private:
        cppgit2::repository *repo;
        cppgit2::tree tree_;
    };
}
