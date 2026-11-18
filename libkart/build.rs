fn main() {
    // libgit2's rand.c uses CryptAcquireContext/CryptReleaseContext (advapi32), but
    // libgit2-sys doesn't emit a link flag for it and modern rustc no longer links
    // advapi32 transitively via std (std uses raw-dylib imports).
    if std::env::var_os("CARGO_CFG_WINDOWS").is_some() {
        println!("cargo:rustc-link-lib=advapi32");
    }
}
