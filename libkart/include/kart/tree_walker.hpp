#pragma once

#include <algorithm>
#include <exception>
#include <string>
#include <memory>
#include <cppgit2/repository.hpp>

using namespace std;
using namespace cppgit2;
namespace kart
{
    class TreeEntryWithPath : public tree::entry
    {
    public:
        // default constructor
        TreeEntryWithPath();
        // construct from a tree::entry
        TreeEntryWithPath(const entry &e, string rel_path__);
        // accessors
        string rel_path() const;

    private:
        string rel_path_;
    };
    /**
     * TreeEntryIterator: An iterator over a tree's entries
     * (and entries of all subtrees) in preorder.
     */
    class TreeEntryIterator
        : public iterator<
              input_iterator_tag,
              TreeEntryWithPath,
              long,
              const TreeEntryWithPath *,
              const TreeEntryWithPath &>
    {
    public:
        TreeEntryIterator();
        TreeEntryIterator(repository *repo, tree *tree_);
        reference operator*() const;
        pointer operator->();

        // Prefix increment
        TreeEntryIterator &operator++();

        // Postfix increment
        TreeEntryIterator operator++(int);
        friend bool operator==(const TreeEntryIterator &a, const TreeEntryIterator &b)
        {
            auto a_size = a.heads.size();
            auto b_size = b.heads.size();
            if (a_size != b_size)
            {
                return false;
            }
            if (!a_size)
            {
                return true;
            }
            return (a.heads.back() == b.heads.back());
        };

        friend bool operator!=(const TreeEntryIterator &a, const TreeEntryIterator &b)
        {
            return !(a == b);
        };

    private:
        void _enter_tree(tree tree_);
        repository *repo_;
        vector<vector<TreeEntryWithPath>> entries_stack;
        vector<size_t> heads;
    };

    class TreeWalker
    {
    public:
        TreeWalker(repository *repo_, tree *tree_);
        TreeEntryIterator begin();
        TreeEntryIterator end();

    private:
        repository *repo_;
        tree *tree_;
    };
} // namespace kart
