#pragma once

#include <algorithm>
#include <exception>
#include <string>
#include <memory>

namespace kart
{
    class TreeEntryIterator;
    class TreeWalker;
}
#include "kart/repo.hpp"
#include "kart/object.hpp"

using namespace std;
using namespace cppgit2;
namespace kart
{
    /**
     * TreeEntryIterator: An iterator over a tree's entries
     * (and entries of all subtrees) in preorder.
     */
    class TreeEntryIterator
        : public iterator<
              input_iterator_tag,
              TreeEntry,
              long,
              const TreeEntry *,
              const TreeEntry &>
    {
    public:
        TreeEntryIterator();
        TreeEntryIterator(KartRepo *repo, Tree *tree_);

        reference operator*() const;
        pointer operator->();

        // Prefix increment
        TreeEntryIterator &operator++();

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

        static const TreeEntryIterator END;

    private:
        void _enter_tree(Tree tree_);
        KartRepo *repo_;
        vector<vector<TreeEntry>> entries_stack;
        vector<size_t> heads;
    };

    class TreeWalker
    {
    public:
        TreeWalker(KartRepo *repo_, Tree *tree_);
        TreeEntryIterator begin();
        TreeEntryIterator end();

    private:
        KartRepo *repo_;
        Tree *tree_;
    };
} // namespace kart
