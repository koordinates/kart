#pragma once

#include <exception>
#include <string>
#include <memory>
#include <cppgit2/repository.hpp>

using namespace std;
namespace kart
{

    class Dataset3
    {
    public:
        Dataset3(cppgit2::repository *repo, cppgit2::tree tree_, string path);
        const string path;

    private:
        cppgit2::repository *repo;
        const cppgit2::tree tree_;
    };
}
