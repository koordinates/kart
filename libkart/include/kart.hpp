#pragma once

#include <exception>
#include <string>
#include <memory>

#include <cppgit2/repository.hpp>

using namespace std;
using namespace cppgit2;

namespace kart {

class LibKartError : public runtime_error {
	public:
		LibKartError(const char *message) : runtime_error(message) {};
};
class KartRepo
{
	public:
		KartRepo(const char *path);
		~KartRepo() {};
		const int GetVersion() const;
	private:
		const repository repo;
};

}

extern "C"
{
	kart::KartRepo *kart_open_repository(const char *path);
	void kart_close_repository(kart::KartRepo* repo);
	const int kart_repo_version(const kart::KartRepo* repo);
}
