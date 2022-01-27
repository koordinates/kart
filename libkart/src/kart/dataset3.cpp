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
