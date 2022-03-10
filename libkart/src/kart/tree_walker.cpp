#include <exception>
#include <iostream>
#include <string>
#include <memory>

#include "kart/tree_walker.hpp"

using namespace std;
using namespace cppgit2;
using namespace kart;

/**
 * TreeEntryIterator: An iterator over a tree's entries
 * (and entries of all subtrees) in preorder.
 */
TreeEntryIterator::TreeEntryIterator()
{
    TreeEntryIterator(nullptr, nullptr);
};
TreeEntryIterator::TreeEntryIterator(KartRepo *repo, Tree *tree_)
    : repo_(repo),
      entries_stack(vector<vector<TreeEntry>>()),
      heads(vector<size_t>())
{
    if (tree_)
    {
        _enter_tree(*tree_);
    }
};
// static member
const TreeEntryIterator TreeEntryIterator::END{nullptr, nullptr};

TreeEntryIterator::reference TreeEntryIterator::operator*() const
{
    return entries_stack.back()[heads.back()];
};
TreeEntryIterator::pointer TreeEntryIterator::operator->()
{
    return &(entries_stack.back()[heads.back()]);
};

// Prefix increment
TreeEntryIterator &TreeEntryIterator::operator++()
{
    if (!heads.size())
    {
        return *this;
    }
    // check if the previous entry was a tree. if so, we need to push the tree's entries onto the stack
    auto entry = **this;
    bool new_tree = (entry.type() == object::object_type::tree);
    if (new_tree)
    {
        _enter_tree(entry.get_object().as_tree());
    }
    else
    {
        heads.back()++;
    }
    // TODO: try and rearrange this so it's neater
    while (heads.size())
    {
        if (heads.back() < entries_stack.back().size())
        {
            return *this;
        }
        // the latest tree has been exhausted; pop from the stack and keep iterating
        // over the previous tree.
        entries_stack.pop_back();
        heads.pop_back();
        if (heads.size())
        {
            heads.back()++;
        }
    }
    // finished iteration
    return *this;
}

void TreeEntryIterator::_enter_tree(Tree tree_)
{
    entries_stack.push_back(tree_.entries());
    heads.push_back(0);
};

TreeWalker::TreeWalker(KartRepo *repo, Tree *tree_)
    : repo_(repo), tree_(tree_)
{
}
TreeEntryIterator TreeWalker::begin()
{
    return TreeEntryIterator(repo_, tree_);
}
TreeEntryIterator TreeWalker::end()
{
    return TreeEntryIterator::END;
}
