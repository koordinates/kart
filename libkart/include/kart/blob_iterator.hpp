#pragma once

#include <algorithm>
#include <exception>
#include <string>
#include <memory>

#include "kart/object.hpp"
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
              kart::Blob,
              long,
              const kart::Blob *,
              const kart::Blob &>
    {
    public:
        BlobIterator();
        BlobIterator(const BlobIterator &other);
        BlobIterator(KartRepo *repo, Tree *tree);

        reference operator*() const;
        pointer operator->();

        // Prefix increment
        BlobIterator &operator++();

        // Postfix increment
        BlobIterator operator++(int);

        // comparison
        friend bool operator==(const BlobIterator &a, const BlobIterator &b);
        friend bool operator!=(const BlobIterator &a, const BlobIterator &b);

        // assignment
        BlobIterator &operator=(const BlobIterator &other);

    private:
        KartRepo *repo_;
        unique_ptr<TreeEntryIterator> tree_entry_iterator_;
        Blob current_blob;
        inline void _advance_to_blob();
    };

    class BlobWalker
    {
    public:
        BlobWalker(KartRepo *repo, unique_ptr<Tree> tree)
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
        KartRepo *repo_;
        unique_ptr<Tree> tree_;
    };

} // namespace kart
