#pragma once

#include <string>
#include <git2.h>

namespace kart
{
    class Oid
    {
    public:
        // constructors
        Oid();
        Oid(const std::string &hex_string);
        Oid(git_oid *c_ptr);
        Oid(const git_oid *c_ptr);
        ~Oid();

        // comparison
        bool operator==(const Oid &rhs) const;
        bool operator==(const std::string &rhs) const;

        // conversion
        std::string to_hex_string(size_t n = GIT_OID_HEXSZ) const;

        // guts
        git_oid *c_ptr();

    private:
        friend class KartRepo;
        git_oid *wrapped;
    };

    std::ostream &
    operator<<(std::ostream &strm, const Oid &id);
} // namespace kart
