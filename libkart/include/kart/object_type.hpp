#pragma once

namespace kart
{
    enum class ObjectType
    {
        any = -2,
        invalid = -1,
        commit = 1,
        tree = 2,
        blob = 3,
        tag = 4,
        ofs_delta = 6,
        ref_delta = 7
    };
}
