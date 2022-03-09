#pragma once

#include <exception>
#include <string>
#include <memory>

namespace kart
{
    class Dataset3;
    class BlobWalker;
}
#include "kart/repo.hpp"
#include "kart/blob_iterator.hpp"

using namespace std;
namespace kart
{
    const string DATASET_DIRNAME = ".table-dataset";

    class Dataset3
    {
    public:
        Dataset3(KartRepo *repo, Tree tree_);

        unique_ptr<Tree> get_tree();
        unique_ptr<Tree> get_feature_tree();

        unique_ptr<BlobWalker> feature_blobs();

    private:
        KartRepo *repo;
        Tree tree_;
    };
}
