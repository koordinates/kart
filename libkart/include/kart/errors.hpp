#pragma once

#include <exception>
#include <git2.h>

using namespace std;

namespace kart
{

    class LibKartError : public runtime_error
    {
    public:
        LibKartError(const char *message) : runtime_error(message){};
    };
    class LibGitError : public exception
    {
    public:
        LibGitError()
        {
            auto error = git_error_last();
            message_ = error ? error->message : "unknown error";
        }
        LibGitError(const char *message) : message_(message) {}
        LibGitError(const std::string &message) : message_(message.c_str()) {}
        virtual ~LibGitError() throw() {}
        virtual const char *what() const throw() { return message_; }

        static void clear() { git_error_clear(); }

    protected:
        const char *message_;
    };
}
