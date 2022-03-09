#pragma once

#include <string>
using namespace std;
namespace kart
{

    static inline string trim_trailing_slashes(string s)
    {
        while (s.back() == '/')
        {
            s.pop_back();
        }
        return s;
    }

} // namespace kart
