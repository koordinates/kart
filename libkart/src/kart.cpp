#include <exception>
#include <iostream>
#include <string>
#include <memory>

#include "kart.hpp"

using namespace std;
using namespace kart;

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
		catch (LibGitError &e)
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
