#pragma once

#include <exception>
#include <string>
#include <memory>

namespace kart
{
    class RepoStructure;
}
#include "kart/dataset3.hpp"
#include "kart/errors.hpp"

using namespace std;
namespace kart
{
    class RepoStructure
    {
    public:
        RepoStructure(KartRepo *repo, Tree root_tree);

        int Version();
        // TODO: support other types of datasets (1/2)?
        // or at least throw some useful exception rather than crashing.
        vector<Dataset3 *> *GetDatasets();

    private:
        Tree root_tree;
        KartRepo *repo;
    };
}
