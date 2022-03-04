#include <exception>
#include <iostream>
#include <string>
#include <memory>

#include "kart/tree_walker.hpp"

using namespace std;
using namespace cppgit2;
using namespace kart;

TreeEntryWithPath::TreeEntryWithPath()
    : tree::entry(),
      rel_path_(""){};
// construct from a tree::entry
TreeEntryWithPath::TreeEntryWithPath(const entry &e, string rel_path__)
    : tree::entry(e),
      rel_path_(rel_path__){};
// accessors
string TreeEntryWithPath::rel_path() const
{
    return rel_path_;
};

/**
 * TreeEntryIterator: An iterator over a tree's entries
 * (and entries of all subtrees) in preorder.
 */
TreeEntryIterator::TreeEntryIterator()
{
    TreeEntryIterator(nullptr, nullptr);
};
TreeEntryIterator::TreeEntryIterator(repository *repo, tree *tree_)
    : repo_(repo),
      entries_stack(vector<vector<TreeEntryWithPath>>()),
      heads(vector<size_t>())
{
    if (tree_)
    {
        _enter_tree(*tree_);
    }
};
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
        _enter_tree(repo_->tree_entry_to_object(entry).as_tree());
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

// Postfix increment
TreeEntryIterator TreeEntryIterator::operator++(int)
{
    TreeEntryIterator tmp = *this;
    ++(*this);
    return tmp;
}

void TreeEntryIterator::_enter_tree(tree tree_)
{
    string rel_path = "";
    if (entries_stack.size())
    {
        rel_path = (**this).rel_path() + "/";
    }
    auto entries = vector<TreeEntryWithPath>();
    for (auto e : tree_.entries())
    {
        entries.push_back(TreeEntryWithPath(e, rel_path + e.filename()));
    }
    entries_stack.push_back(entries);
    heads.push_back(0);
};

TreeWalker::TreeWalker(cppgit2::repository *repo_, cppgit2::tree *tree_)
    : repo_(repo_), tree_(tree_)
{
}
TreeEntryIterator TreeWalker::begin()
{
    return TreeEntryIterator(repo_, tree_);
}
TreeEntryIterator TreeWalker::end()
{
    return TreeEntryIterator(nullptr, nullptr);
}
