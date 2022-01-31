#pragma once

#include <exception>

using namespace std;

namespace kart
{

    class LibKartError : public runtime_error
    {
    public:
        LibKartError(const char *message) : runtime_error(message){};
    };
}
