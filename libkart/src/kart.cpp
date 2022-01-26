#include <exception>
#include <iostream>
#include <string>
#include <memory>

#include <cppgit2/repository.hpp>

#include "kart.hpp"

using namespace std;
using namespace cppgit2;
using namespace kart;


std::ostream& operator<<(std::ostream &strm, const oid &id) {
  return strm << id.to_hex_string();
}
std::ostream& operator<<(std::ostream &strm, const object::object_type &otyp) {
  return strm << object::object_type_to_string(otyp);
}


KartRepo::KartRepo(const char *path)
	: repo (repository::open(path))
{}

const int KartRepo::GetVersion() const {
	auto head = repo.head();
//	clog << "got head " << head.name() << "\n";
	auto head_commit = repo.lookup_commit(head.target());
//	clog << "got commit " << head_commit.id() << "\n";
	auto head_tree = head_commit.tree();
//	clog << "got tree " << head_tree.id() << "\n";

	auto entry = head_tree.lookup_entry_by_path(".kart.repostructure.version");
//	clog << "got entry " << entry.type() << "\n";

	if (entry.type() != object::object_type::blob) {
		throw LibKartError("kart repo version didn't resolve to a blob");
	}

	auto blob = repo.tree_entry_to_object(entry).as_blob();
//	clog << "got blob\n";
	string content = string(static_cast<const char*>(blob.raw_contents()), blob.raw_size());

	return stoi(content);
}

extern "C"
{
	KartRepo* kart_open_repository(const char *path)
	{
		try {
			return new KartRepo(path);
		} catch (git_exception& e) {
			clog << "error opening repository " << path << ": " << e.what() << "\n";
			return nullptr;
		}
	}

	void kart_close_repository(KartRepo* repo)
	{
		delete repo;
	}

	const int kart_repo_version(const KartRepo* repo) {
		try {
			return repo->GetVersion();
		} catch(exception& e) {
			clog << "error getting repo version: " << e.what() << "\n";
			return 0;
		}
	}
}
