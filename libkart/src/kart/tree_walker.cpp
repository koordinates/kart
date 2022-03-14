#include <exception>
#include <iostream>
#include <string>
#include <memory>

#include "kart/tree_walker.hpp"

using namespace std;
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
      open_trees(),
      heads(vector<size_t>())
{
    if (tree_ && tree_->size())
    {
        _enter_tree(*tree_);
    }
};
// static member
const TreeEntryIterator TreeEntryIterator::END{nullptr, nullptr};

TreeEntryIterator::reference TreeEntryIterator::operator*() const
{
    return current_entry;
};
TreeEntryIterator::pointer TreeEntryIterator::operator->()
{
    return &current_entry;
};

// Prefix increment
TreeEntryIterator &TreeEntryIterator::operator++()
{
    if (!heads.size())
    {
        return *this;
    }
    // check if the previous entry was a tree. if so, we need to push the tree's children onto the stack
    if (current_entry.type() == ObjectType::tree)
    {
        _enter_tree(current_entry.get_object().as_tree());
    }
    else
    {
        ++heads.back();
    }
    // TODO: try and rearrange this so it's neater
    while (heads.size())
    {
        if (heads.back() < open_trees.back().size())
        {
            current_entry = open_trees.back().get_entry_by_index(heads.back());
            return *this;
        }
        // the latest tree has been exhausted; pop from the stack and keep iterating
        // over the previous tree.
        open_trees.pop_back();
        heads.pop_back();
        if (heads.size())
        {
            heads.back()++;
        }
    }
    return *this;
}

void TreeEntryIterator::_enter_tree(const Tree &tree_)
{
    open_trees.push_back(tree_);
    heads.push_back(0);
    current_entry = tree_.get_entry_by_index(0);
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
