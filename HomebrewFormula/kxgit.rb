class Kxgit < Formula
  include Language::Python::Virtualenv

  desc "Koordinates Gitlike Proof of Concept"
  homepage "https://github.com/koordinates/snowdrop"

  head do
    url "git@github.com:koordinates/snowdrop.git", :branch => 'gitlike-2019', :using => :git

    resource "libgit2" do
      url "https://github.com/libgit2/libgit2/archive/af95615faa87d3181fb5e8bc140c1aa6a8eda085.tar.gz"
      sha256 "f1c4d666555bee81d2bac1677876a2ffe4c6e7c8cb64a3e51fc7540913cf4bbc"
    end

    resource "pygit2" do
      # better-tree-nav branch
      url "https://github.com/rcoup/pygit2/archive/613a742796f8318181974fa8122ca094cb2c9bcd.tar.gz"
      sha256 "5cade50e8e237939017ea3af9955edf439fabe149f955408d14dcbadf49c2503"
    end
  end

  depends_on "python" # Python3
  depends_on "git"
  depends_on "sqlite3"
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
      cmake_args[2] = "-DCMAKE_INSTALL_PREFIX=#{venv_root}"

      system "cmake", ".", *cmake_args
      system "make", "install"
    }

    resource("pygit2").stage {
      ENV["LDFLAGS"] = "-Wl,-rpath,'#{venv_root}/lib' #{ENV['LDFLAGS']}"
      venv.pip_install resources[2]  # pygit2
    }

    # Kx: install requirements.txt dependencies
    # Total hack that works only by coincidence:
    # venv.pip_install "--requirement=requirements.txt --no-binary=:none:"
    system "#{venv_root}/bin/pip", "install",
      "-v", "--no-deps", "--ignore-installed",
      "--requirement=requirements.txt"

    Pathname.glob("#{venv_root}/lib/python*/no-global-site-packages.txt").each {|p| p.delete}

    # `pip_install_and_link` takes a look at the virtualenv's bin directory
    # before and after installing its argument. New scripts will be symlinked
    # into `bin`. `pip_install_and_link buildpath` will install the package
    # that the formula points to, because buildpath is the location where the
    # formula's tarball was unpacked.
    venv.pip_install_and_link buildpath
  end
end
