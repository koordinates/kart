from libcpp.string cimport string

cdef extern from "kart.hpp" namespace "kart":
	cdef cppclass CppKartRepo "kart::KartRepo":
		CppKartRepo(const char *path)
		int GetVersion()

	# KartRepo* kart_open_repository(const char *path)
	# void kart_close_repository(KartRepo* repo)
	# int kart_repo_version(KartRepo* repo)
