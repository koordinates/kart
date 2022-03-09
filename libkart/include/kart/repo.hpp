#pragma once

#include <exception>
#include <string>
#include <memory>

#include <cppgit2/repository.hpp>

// allows dataset3.hpp to reference KartRepo
namespace kart
{
	class KartRepo;
}
#include "kart/tree_walker.hpp"
#include "kart/structure.hpp"

using namespace std;
using namespace cppgit2;

namespace kart
{
	class KartRepo
	{
	public:
		// constructors
		KartRepo(const char *path);
		~KartRepo(){};

		// git wrappers
		Object revparse_to_object(const std::string &spec);
		Object lookup_object(cppgit2::oid id, cppgit2::object::object_type type, TreeEntry entry);
		Object lookup_object(cppgit2::oid id, cppgit2::object::object_type type);

		// kart stuff
		int Version();
		unique_ptr<RepoStructure> Structure();
		unique_ptr<RepoStructure> Structure(string treeish);

		unique_ptr<TreeWalker> walk_tree(Tree *root);

	private:
		repository wrapped;
	};

}

extern "C"
{
	kart::KartRepo *kart_open_repository(const char *path);
	void kart_close_repository(kart::KartRepo *repo);
	int kart_repo_version(kart::KartRepo *repo);
}
