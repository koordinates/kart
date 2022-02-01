#pragma once

#include <exception>
#include <string>
#include <memory>

#include <cppgit2/repository.hpp>

#include "kart/structure.hpp"
#include "kart/structure.hpp"

using namespace std;
using namespace cppgit2;

namespace kart
{
	class KartRepo
	{
	public:
		KartRepo(const char *path);
		~KartRepo(){};
		int Version();
		unique_ptr<RepoStructure> Structure();
		unique_ptr<RepoStructure> Structure(string treeish);

	private:
		repository repo;
	};

}

extern "C"
{
	kart::KartRepo *kart_open_repository(const char *path);
	void kart_close_repository(kart::KartRepo *repo);
	int kart_repo_version(kart::KartRepo *repo);
}
