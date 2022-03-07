#pragma once

#include <algorithm>
#include <exception>
#include <string>
#include <memory>
#include <cppgit2/repository.hpp>

#include "kart/tree_walker.hpp"

using namespace std;
using namespace cppgit2;
namespace kart
{
    /**
     * BlobIterator: An iterator over all blobs in the given tree hierarchy.
     **/
    class BlobIterator
        : public iterator<
              input_iterator_tag,
              cppgit2::blob,
              long,
              const cppgit2::blob *,
              const cppgit2::blob &>
    {
    public:
        BlobIterator();
        BlobIterator(repository *repo, cppgit2::tree *tree);

        reference operator*() const;
        pointer operator->();

        // Prefix increment
        BlobIterator &operator++();

        // Postfix increment
        BlobIterator operator++(int);
        friend bool operator==(const BlobIterator &a, const BlobIterator &b)
        {
            return a.tree_entry_iterator_ == b.tree_entry_iterator_;
        };

        friend bool operator!=(const BlobIterator &a, const BlobIterator &b)
        {
            return a.tree_entry_iterator_ != b.tree_entry_iterator_;
        };

    private:
        repository *repo_;
        TreeEntryIterator tree_entry_iterator_;
        cppgit2::blob current_blob;
        inline void _next_blob();
    };

    class BlobWalker
    {
    public:
        BlobWalker(cppgit2::repository *repo, unique_ptr<cppgit2::tree> tree)
            : repo_(repo), tree_(move(tree))
        {
        }
        BlobIterator begin()
        {
            return BlobIterator(repo_, tree_.get());
        }
        BlobIterator end()
        {
            return BlobIterator(nullptr, nullptr);
        }

    private:
        repository *repo_;
        unique_ptr<cppgit2::tree> tree_;
    };

} // namespace kart
