//! Repository-level operations: open, dataset-format version, dataset discovery.

use std::path::Path;

use git2::{Repository, RepositoryOpenFlags, Tree};

use crate::error::{Error, Result};

/// Candidate git-dir names within a Kart repo root (`.kart` preferred, `.sno` legacy).
const KART_DIR_NAMES: [&str; 2] = [".kart", ".sno"];

/// Git's well-known empty-tree object id.
const EMPTY_TREE_OID: &str = "4b825dc642cb6eb9a060e54bf8d69288fbee4904";

/// Regex-equivalent check: a dataset inner-dir name fullmatches `\.[^/]*-dataset[^/]*`.
fn is_dataset_dirname(name: &str) -> bool {
    if !name.starts_with('.') {
        return false;
    }
    if name.contains('/') {
        return false;
    }
    // Must contain "-dataset" somewhere after the leading dot.
    name[1..].contains("-dataset")
}

/// Map an inner dataset-dir name to a dataset type, if supported.
pub(crate) fn dataset_type_for_dirname(name: &str) -> Option<&'static str> {
    match name {
        ".table-dataset" | ".sno-dataset" => Some("table"),
        ".point-cloud-dataset.v1" => Some("point-cloud"),
        ".raster-dataset.v1" => Some("raster"),
        _ => {
            if is_dataset_dirname(name) {
                Some("unsupported")
            } else {
                None
            }
        }
    }
}

/// A handle to an open Kart repository (wraps a libgit2 repository).
pub struct Repo {
    pub(crate) git: Repository,
}

impl Repo {
    /// Open the Kart repository rooted at `path`.
    pub fn open(path: &str) -> Result<Repo> {
        let root = Path::new(path);
        // Kart keeps git data under `.kart/` (or legacy `.sno/`); fall back to the path
        // itself if neither child dir exists (already-bare repo).
        let git_dir = KART_DIR_NAMES
            .iter()
            .map(|name| root.join(name))
            .find(|p| p.is_dir())
            .unwrap_or_else(|| root.to_path_buf());

        let flags = RepositoryOpenFlags::BARE | RepositoryOpenFlags::FROM_ENV;
        let git = Repository::open_ext(&git_dir, flags, std::iter::empty::<&str>())?;
        Ok(Repo { git })
    }

    /// Resolve a refish to its tree, returning the git empty tree if HEAD is unborn or
    /// the refish names an empty state.
    pub(crate) fn resolve_tree(&self, refish: &str) -> Result<Tree<'_>> {
        if refish.is_empty() || refish == "[EMPTY]" {
            return self.empty_tree();
        }
        if refish == "HEAD" {
            match self.git.head() {
                Ok(head) => return Ok(head.peel_to_tree()?),
                Err(_) => return self.empty_tree(),
            }
        }
        let obj = self.git.revparse_single(refish)?;
        Ok(obj.peel_to_tree()?)
    }

    fn empty_tree(&self) -> Result<Tree<'_>> {
        let oid = git2::Oid::from_str(EMPTY_TREE_OID)?;
        Ok(self.git.find_tree(oid)?)
    }

    /// The Kart table-dataset format version for this repo (e.g. 3).
    pub fn table_dataset_version(&self) -> Result<i32> {
        // 1) Try the version blob in the HEAD root tree.
        if let Ok(tree) = self.resolve_tree("HEAD") {
            for (path, _version) in [
                (".kart.repostructure.version", 3),
                (".sno.repository.version", 2),
            ] {
                if let Some(entry) = tree.get_name(path) {
                    let obj = entry.to_object(&self.git)?;
                    if let Some(blob) = obj.as_blob() {
                        let text = std::str::from_utf8(blob.content())?;
                        let v: i32 = text.trim().parse().map_err(|_| {
                            Error::Format(format!("invalid version blob contents: {text:?}"))
                        })?;
                        return Ok(v);
                    }
                }
            }
        }

        // 2) Fall back to git config.
        let cfg = self.git.config()?;
        if let Ok(v) = cfg.get_i32("kart.repostructure.version") {
            return Ok(v as i32);
        }
        if let Ok(v) = cfg.get_i32("sno.repository.version") {
            return Ok(v as i32);
        }
        // 3) Default.
        Ok(3)
    }

    /// Paths of all datasets present at `refish` (default "HEAD").
    pub fn list_datasets(&self, refish: &str) -> Result<Vec<String>> {
        let tree = self.resolve_tree(refish)?;
        let mut datasets = Vec::new();
        self.collect_datasets(&tree, "", &mut datasets)?;
        datasets.sort();
        Ok(datasets)
    }

    /// Recursively walk `tree`, skipping dot-prefixed (hidden) trees. A tree is a dataset
    /// path if any direct child tree's name matches the dataset-dir pattern.
    fn collect_datasets(&self, tree: &Tree<'_>, path: &str, out: &mut Vec<String>) -> Result<()> {
        // Is this tree itself a dataset (has a `.*-dataset*` child)?
        let mut is_dataset = false;
        for entry in tree.iter() {
            if entry.kind() == Some(git2::ObjectType::Tree) {
                if let Some(name) = entry.name() {
                    if is_dataset_dirname(name) {
                        is_dataset = true;
                        break;
                    }
                }
            }
        }
        if is_dataset {
            if !path.is_empty() {
                out.push(path.to_string());
            }
            return Ok(());
        }

        // Otherwise recurse into non-hidden subtrees.
        for entry in tree.iter() {
            if entry.kind() != Some(git2::ObjectType::Tree) {
                continue;
            }
            let name = match entry.name() {
                Some(n) => n,
                None => continue,
            };
            if name.starts_with('.') {
                continue;
            }
            let child_path = if path.is_empty() {
                name.to_string()
            } else {
                format!("{path}/{name}")
            };
            let obj = entry.to_object(&self.git)?;
            if let Some(child_tree) = obj.as_tree() {
                self.collect_datasets(child_tree, &child_path, out)?;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::disable_owner_validation;
    use std::process::Command;

    const POINTS_TGZ: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/../tests/data/points.tgz");
    const AU_CENSUS_TGZ: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/../tests/data/au-census.tgz");

    /// Extract a fixture tgz into a fresh temp dir, returning the repo root path.
    /// `tag` is a per-test label so parallel tests on the same fixture don't collide.
    fn extract_fixture(tgz: &str, subdir: &str, tag: &str) -> std::path::PathBuf {
        disable_owner_validation();
        let base = std::env::temp_dir().join(format!(
            "libkart-repotest-{}-{}-{}",
            tag,
            subdir,
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&base);
        std::fs::create_dir_all(&base).unwrap();
        let status = Command::new("tar")
            .arg("xzf")
            .arg(tgz)
            .arg("-C")
            .arg(&base)
            .status()
            .expect("run tar");
        assert!(status.success(), "tar failed for {tgz}");
        base.join(subdir)
    }

    #[test]
    fn test_is_dataset_dirname() {
        assert!(is_dataset_dirname(".table-dataset"));
        assert!(is_dataset_dirname(".point-cloud-dataset.v1"));
        assert!(is_dataset_dirname(".sno-dataset"));
        // Not a dataset dir: no leading dot, no "-dataset", or contains a slash.
        assert!(!is_dataset_dirname("table-dataset"));
        assert!(!is_dataset_dirname(".meta"));
        assert!(!is_dataset_dirname(".kart/-dataset"));
    }

    #[test]
    fn test_open_succeeds_on_fixture() {
        let root = extract_fixture(POINTS_TGZ, "points", "open");
        let repo = Repo::open(root.to_str().unwrap()).unwrap();
        // HEAD resolves to a non-empty tree.
        assert!(repo.resolve_tree("HEAD").unwrap().len() > 0);
        let _ = std::fs::remove_dir_all(root.parent().unwrap());
    }

    #[test]
    fn test_open_nonexistent_path_errors() {
        let missing =
            std::env::temp_dir().join(format!("libkart-repotest-missing-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&missing);
        assert!(Repo::open(missing.to_str().unwrap()).is_err());
    }

    #[test]
    fn test_open_non_repo_path_errors() {
        // An existing directory that is not a git/Kart repo must fail to open.
        let dir =
            std::env::temp_dir().join(format!("libkart-repotest-nonrepo-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        assert!(Repo::open(dir.to_str().unwrap()).is_err());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_table_dataset_version_is_3() {
        let root = extract_fixture(POINTS_TGZ, "points", "version");
        let repo = Repo::open(root.to_str().unwrap()).unwrap();
        assert_eq!(repo.table_dataset_version().unwrap(), 3);
        let _ = std::fs::remove_dir_all(root.parent().unwrap());
    }

    #[test]
    fn test_list_datasets_single() {
        let root = extract_fixture(POINTS_TGZ, "points", "single");
        let repo = Repo::open(root.to_str().unwrap()).unwrap();
        let datasets = repo.list_datasets("HEAD").unwrap();
        assert_eq!(datasets, vec!["nz_pa_points_topo_150k".to_string()]);
        let _ = std::fs::remove_dir_all(root.parent().unwrap());
    }

    #[test]
    fn test_list_datasets_multi_sorted() {
        let root = extract_fixture(AU_CENSUS_TGZ, "au-census", "multi");
        let repo = Repo::open(root.to_str().unwrap()).unwrap();
        let datasets = repo.list_datasets("HEAD").unwrap();
        // au-census has two top-level datasets, returned sorted ascending.
        assert_eq!(
            datasets,
            vec![
                "census2016_sdhca_ot_ra_short".to_string(),
                "census2016_sdhca_ot_sos_short".to_string(),
            ]
        );
        assert!(datasets.len() > 1);
        let _ = std::fs::remove_dir_all(root.parent().unwrap());
    }

    #[test]
    fn test_empty_refish_resolves_to_empty_tree() {
        let root = extract_fixture(POINTS_TGZ, "points", "empty");
        let repo = Repo::open(root.to_str().unwrap()).unwrap();
        // Both "" and "[EMPTY]" resolve to the git empty tree => no datasets.
        for refish in ["", "[EMPTY]"] {
            let tree = repo.resolve_tree(refish).unwrap();
            assert_eq!(tree.id().to_string(), EMPTY_TREE_OID);
            assert_eq!(tree.len(), 0);
            assert!(repo.list_datasets(refish).unwrap().is_empty());
        }
        let _ = std::fs::remove_dir_all(root.parent().unwrap());
    }

    #[test]
    fn test_hidden_top_level_trees_not_reported() {
        // The points fixture's HEAD tree contains a dot-prefixed
        // `.kart.repostructure.version` blob alongside the dataset; dot-prefixed
        // entries are never reported as datasets, only `nz_pa_points_topo_150k` is.
        let root = extract_fixture(POINTS_TGZ, "points", "hidden");
        let repo = Repo::open(root.to_str().unwrap()).unwrap();
        let datasets = repo.list_datasets("HEAD").unwrap();
        assert!(datasets.iter().all(|d| !d.starts_with('.')));
        let _ = std::fs::remove_dir_all(root.parent().unwrap());
    }

    // Note: none of the available fixtures (points, polygons, au-census) nest
    // datasets under a subdirectory — all datasets sit at the repo root — so the
    // nested-path discovery case is not exercised here.
}
