#include <algorithm>
#include <exception>
#include <string>
#include <memory>
#include <cppgit2/repository.hpp>

#include "kart/blob_iterator.hpp"

using namespace std;
using namespace cppgit2;
namespace kart
{
    /**
     * BlobIterator: An iterator over all blobs in the given tree hierarchy.
     **/
    BlobIterator::BlobIterator() : BlobIterator::BlobIterator(nullptr, nullptr){};
    BlobIterator::BlobIterator(repository *repo, cppgit2::tree *tree) : repo_(repo), tree_entry_iterator_(TreeEntryIterator(repo, tree))
    {
        _next_blob();
    }
    BlobIterator::reference BlobIterator::operator*() const
    {
        return current_blob;
    }
    BlobIterator::pointer BlobIterator::operator->()
    {
        return &current_blob;
    }

    // Prefix increment
    BlobIterator &BlobIterator::operator++()
    {
        tree_entry_iterator_++;
        _next_blob();
        return *this;
    }

    // Postfix increment
    BlobIterator BlobIterator::operator++(int)
    {
        BlobIterator tmp = *this;
        ++(*this);
        return tmp;
    }

    inline void BlobIterator::_next_blob()
    {
        while (tree_entry_iterator_ != TreeEntryIterator::END)
        {
            if (tree_entry_iterator_->type() == object::object_type::blob)
            {
                cppgit2::tree::entry entry(*tree_entry_iterator_);
                current_blob = repo_->lookup_blob(entry.id());
                break;
            }
            else
            {
                tree_entry_iterator_++;
            }
        }
    }

} // namespace kart
