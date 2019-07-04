class Snowdrop < Formula
  include Language::Python::Virtualenv

  desc "Distributed version-control for datasets"
  homepage "https://github.com/koordinates/snowdrop"

  head do
    url "git@github.com:koordinates/snowdrop.git", :branch => 'master', :using => :git

    resource "libgit2" do
      # kx-0.28 branch
      url "https://github.com/koordinates/libgit2/archive/7a39d0d1aad41d92cf0e3f980ddbb7d4ea88373c.tar.gz"
      sha256 "caa6e64e4c09dc9cb728a6cfcc4e7466e6e6ec032f0dea72ca10a2f7aafd8186"
    end

    resource "pygit2" do
      # kx-0.28 branch
      url "https://github.com/koordinates/pygit2/archive/fd9d9d336d9379841a6a3818097e13a9955fc5e5.tar.gz"
      sha256 "fba9a55a93d27b2091d567a3c238971431bc6b3395dbe004747a765598c0012a"
    end
  end

  depends_on "python"  # Python3
  depends_on "git"
  depends_on "sqlite3"
  depends_on "libspatialite"
  depends_on "gdal"

  # depends_on "libgit2"
  # do this manually for libgit2 support
  depends_on "cmake" => [:build]
  depends_on "pkg-config" => [:build]
  depends_on "libssh2"

  def install
    # https://docs.brew.sh/Python-for-Formula-Authors
    # except we'd prefer just to use `pip install` rather than faffing with dependencies here too

    # Create a virtualenv in `libexec`.
    venv_root = libexec
    venv = virtualenv_create(venv_root, "python3")

    # Install the resources declared on the formula into the virtualenv.
    ENV["LIBGIT2"] = venv_root

    resource("libgit2").stage {
      cmake_args = std_cmake_args
      cmake_args << "-DCMAKE_INSTALL_PREFIX=#{venv_root}"
      cmake_args << "-DBUILD_EXAMPLES=NO"
      cmake_args << "-DBUILD_CLAR=NO"

      mkdir "build" do
        system "cmake", *cmake_args, ".."
        system "make", "install"
      end
    }

    gdal_version = `gdal-config --version`.chomp()
    system "#{venv_root}/bin/pip", "install",
      "-v", "--no-deps", "pygdal==#{gdal_version}.*"

    # Kx: install requirements.txt dependencies
    # Total hack that works only by coincidence:
    # venv.pip_install "--requirement=requirements.txt --no-binary=:none:"
    system "#{venv_root}/bin/pip", "install",
      "-v", "--no-deps",
      "--requirement=requirements.txt"

    resource("pygit2").stage {
      ENV["LDFLAGS"] = "-Wl,-rpath,'#{venv_root}/lib' #{ENV['LDFLAGS']}"
      venv.pip_install resources[2]  # pygit2
    }

    # `pip_install_and_link` takes a look at the virtualenv's bin directory
    # before and after installing its argument. New scripts will be symlinked
    # into `bin`. `pip_install_and_link buildpath` will install the package
    # that the formula points to, because buildpath is the location where the
    # formula's tarball was unpacked.
    venv.pip_install_and_link buildpath
  end
end
