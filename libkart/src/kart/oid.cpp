#include "kart/oid.hpp"
#include "kart/errors.hpp"

namespace kart
{
    // constructors
    Oid::Oid() : wrapped(new git_oid()) {}
    Oid::Oid(const std::string &hex) : wrapped(new git_oid())
    {
        if (git_oid_fromstr(wrapped, hex.c_str()))
        {
            throw LibGitError();
        }
    }
    Oid::Oid(git_oid *c_ptr) : wrapped{c_ptr} {}
    Oid::Oid(const git_oid *c_ptr) : wrapped(new git_oid())
    {
        // Some of the libgit2 functions (git_tree_entry_id) return consts.
        // There doesn't seem to be a `git_oid_dup` function to turn them into not-const
        // so that we can store them internally.
        // So we convert via string and back... :)

        char hex[GIT_OID_HEXSZ + 1];
        if (!git_oid_tostr(hex, GIT_OID_HEXSZ + 1, c_ptr))
            throw LibGitError();
        if (git_oid_fromstr(wrapped, hex))
        {
            throw LibGitError();
        }
    }
    Oid::~Oid()
    {
        delete wrapped;
    }

    // comparison
    bool Oid::operator==(const Oid &rhs) const
    {
        return git_oid_equal(wrapped, rhs.wrapped);
    }
    bool Oid::operator==(const std::string &rhs) const
    {
        return git_oid_streq(wrapped, rhs.c_str());
    }

    // conversion
    string Oid::to_hex_string(size_t n) const
    {
        string out(n, '0');
        if (!git_oid_tostr(const_cast<char *>(out.c_str()), n + 1, wrapped))
            throw LibGitError();
        return out;
    }

    // guts
    git_oid *Oid::c_ptr()
    {
        return wrapped;
    }

    std::ostream &
    operator<<(std::ostream &strm, const Oid &id)
    {
        return strm << id.to_hex_string();
    }
} // namespace kart
