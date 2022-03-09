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
    // default constructor
    BlobIterator::BlobIterator() : repo_(nullptr), tree_entry_iterator_(make_unique<TreeEntryIterator>(nullptr, nullptr))
    {
    }
    // copy constructor
    BlobIterator::BlobIterator(const BlobIterator &other) : repo_(other.repo_), tree_entry_iterator_(make_unique<TreeEntryIterator>(*other.tree_entry_iterator_)), current_blob(other.current_blob)
    {
    }
    // normal constructor
    BlobIterator::BlobIterator(KartRepo *repo, Tree *tree) : repo_(repo), tree_entry_iterator_(make_unique<TreeEntryIterator>(repo, tree))
    {
        _advance_to_blob();
    }
    // copy-assignment operator
    BlobIterator &BlobIterator::operator=(const BlobIterator &other)
    {

        // Guard self assignment
        if (this == &other)
            return *this;

        // copy the tree entry iterator
        tree_entry_iterator_ = make_unique<TreeEntryIterator>(*other.tree_entry_iterator_);
        // other fields
        repo_ = other.repo_;
        current_blob = other.current_blob;
        return *this;
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
        (*tree_entry_iterator_)++;
        _advance_to_blob();
        return *this;
    }

    // Postfix increment
    BlobIterator BlobIterator::operator++(int)
    {
        BlobIterator tmp = *this;
        ++(*this);
        return tmp;
    }
    bool operator==(const BlobIterator &a, const BlobIterator &b)
    {
        return *(a.tree_entry_iterator_) == *(b.tree_entry_iterator_);
    };

    bool operator!=(const BlobIterator &a, const BlobIterator &b)
    {
        return *(a.tree_entry_iterator_) != *(b.tree_entry_iterator_);
    };

    inline void BlobIterator::_advance_to_blob()
    {
        while ((*tree_entry_iterator_) != TreeEntryIterator::END)
        {
            if ((*tree_entry_iterator_)->type() == object::object_type::blob)
            {
                TreeEntry entry{*(*tree_entry_iterator_)};
                current_blob = entry.get_object().as_blob();
                break;
            }
            else
            {
                (*tree_entry_iterator_)++;
            }
        }
    }

} // namespace kart
