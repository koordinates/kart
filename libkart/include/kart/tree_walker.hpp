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
        TreeEntryWithPath()
            : tree::entry(),
              rel_path_(""){};
        // construct from a tree::entry
        TreeEntryWithPath(const entry &e, string rel_path__)
            : tree::entry(e),
              rel_path_(rel_path__){};
        // accessors
        string rel_path() const
        {
            return rel_path_;
        };

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
        TreeEntryIterator()
        {
            TreeEntryIterator(nullptr, nullptr);
        }
        TreeEntryIterator(repository *repo, tree *tree_)
            : repo_(repo),
              entries_stack(vector<vector<TreeEntryWithPath>>()),
              heads(vector<size_t>())
        {
            if (tree_)
            {
                _enter_tree(*tree_);
            }
        }
        reference operator*() const
        {
            return entries_stack.back()[heads.back()];
        }
        pointer operator->()
        {
            return &(entries_stack.back()[heads.back()]);
        }

        // Prefix increment
        TreeEntryIterator &operator++()
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
        TreeEntryIterator operator++(int)
        {
            TreeEntryIterator tmp = *this;
            ++(*this);
            return tmp;
        }
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
        }

        friend bool operator!=(const TreeEntryIterator &a, const TreeEntryIterator &b)
        {
            return !(a == b);
        };

    private:
        void _enter_tree(tree tree_)
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
        repository *repo_;
        vector<vector<TreeEntryWithPath>> entries_stack;
        vector<size_t> heads;
    };

    class TreeWalker
    {
    public:
        TreeWalker(repository *repo_, tree *tree_);
        TreeEntryIterator begin()
        {
            return TreeEntryIterator(repo_, tree_);
        }
        TreeEntryIterator end()
        {
            return TreeEntryIterator(nullptr, nullptr);
        }

    private:
        repository *repo_;
        tree *tree_;
    };
} // namespace kart
