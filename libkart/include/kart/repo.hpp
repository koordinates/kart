#pragma once

#include <exception>
#include <string>
#include <memory>

#include <git2.h>

// allows dataset3.hpp to reference KartRepo
namespace kart
{
	class KartRepo;
}
#include "kart/ownership.hpp"
#include "kart/errors.hpp"
#include "kart/oid.hpp"
#include "kart/object_type.hpp"
#include "kart/tree_walker.hpp"
#include "kart/structure.hpp"

using namespace std;

namespace kart
{
	class KartRepo
	{
	public:
		// constructors
		KartRepo(string path);
		~KartRepo();

		// git wrappers
		Object revparse_to_object(const std::string &spec);
		Object lookup_object(Oid id, ObjectType type, TreeEntry entry);
		Object lookup_object(Oid id, ObjectType type);

		// kart stuff
		int Version();
		unique_ptr<RepoStructure> Structure();
		unique_ptr<RepoStructure> Structure(string treeish);

		unique_ptr<TreeWalker> walk_tree(Tree *root);

		git_repository *c_ptr();

	private:
		git_repository *wrapped;
	};

}

extern "C"
{
	kart::KartRepo *kart_open_repository(const char *path);
	void kart_close_repository(kart::KartRepo *repo);
	int kart_repo_version(kart::KartRepo *repo);
}
