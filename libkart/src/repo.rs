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
    fn collect_datasets(
        &self,
        tree: &Tree<'_>,
        path: &str,
        out: &mut Vec<String>,
    ) -> Result<()> {
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
