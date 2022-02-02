#include <exception>
#include <iostream>
#include <string>
#include <memory>

#include "kart/tree_walker.hpp"

using namespace std;
using namespace cppgit2;
using namespace kart;

TreeWalker::TreeWalker(cppgit2::repository *repo_, cppgit2::tree *tree_)
    : repo_(repo_), tree_(tree_)
{
}
