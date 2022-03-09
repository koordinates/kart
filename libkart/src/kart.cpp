#include <exception>
#include <iostream>
#include <string>
#include <memory>

#include <cppgit2/repository.hpp>

#include "kart.hpp"

using namespace std;
using namespace kart;

/*
 * cppgit2 extensions:
 * TODO: move this into our cppgit2 fork, once things have stabilised.
 *
 * Make it easier to debug cppgit2 OID objects.
*/
std::ostream &operator<<(std::ostream &strm, const cppgit2::oid &id)
{
	return strm << id.to_hex_string();
}
std::ostream &operator<<(std::ostream &strm, const cppgit2::object::object_type &otyp)
{
	return strm << cppgit2::object::object_type_to_string(otyp);
}

/*
 * libkart C API
 */
extern "C"
{
	KartRepo *kart::kart_open_repository(const char *path)
	{
		string path_s{path};
		try
		{
			return new KartRepo(path_s);
		}
		catch (git_exception &e)
		{
			clog << "error opening repository " << path_s << ": " << e.what() << "\n";
			return nullptr;
		}
	}

	void kart::kart_close_repository(KartRepo *repo)
	{
		delete repo;
	}

	int kart::kart_repo_version(KartRepo *repo)
	{
		try
		{
			return repo->Version();
		}
		catch (exception &e)
		{
			clog << "error getting repo version: " << e.what() << "\n";
			return 0;
		}
	}
}
