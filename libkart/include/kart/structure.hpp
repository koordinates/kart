#pragma once

#include <exception>
#include <string>
#include <memory>

#include "kart/dataset3.hpp"
#include "kart/errors.hpp"

using namespace std;
namespace kart
{
    class RepoStructure
    {
    public:
        RepoStructure(cppgit2::repository *repo, cppgit2::tree root_tree);

        int Version();
        // TODO: support other types of datasets (1/2)?
        // or at least throw some useful exception rather than crashing.
        vector<Dataset3 *> *GetDatasets();

    private:
        const cppgit2::tree root_tree;
        cppgit2::repository *repo;
    };
}
