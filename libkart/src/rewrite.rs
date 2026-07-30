//! Generic history rewrite with blob substitution.
//!
//! Given a map of `blob oid -> replacement content`, rewrite every commit reachable
//! from the repo's refs so that no tree references the old blobs, preserving commit
//! identity for untouched history, then atomically flip the refs to the rewritten
//! tips. Optional backup refs keep the pre-rewrite tips reachable so a bad rewrite
//! can be undone without data loss.

use std::collections::HashMap;

use git2::{ObjectType, Oid, Sort};

use crate::error::{Error, Result};
use crate::repo::Repo;

/// Statistics returned by [`rewrite_history`].
#[derive(Debug)]
pub struct RewriteStats {
    /// Number of new commit objects created.
    pub commits_rewritten: usize,
    /// Number of substitution blobs written to the object database.
    pub blobs_written: usize,
    /// Original ref names whose tips were moved to rewritten commits.
    pub refs_updated: Vec<String>,
    /// Backup ref names created (empty when no prefix was given or nothing changed).
    pub backup_refs: Vec<String>,
}

/// Rewrite all commits reachable from the repo's direct refs, replacing every tree
/// entry whose blob oid is a key of `substitutions` with a new blob holding the
/// mapped content (filemode preserved). Commits whose tree and parents are unchanged
/// keep their original oid. When `backup_ref_prefix` is given (it must start with
/// `refs/`), a backup ref `{prefix}/{original_ref_name}` pointing at the old tip is
/// created for each changed ref, and refs under the prefix are excluded from the
/// rewrite. Backup creation and ref flips happen in ONE atomic ref transaction, so
/// either all backups exist and all refs are flipped, or nothing changed. Errors
/// before mutating any ref if a needed backup ref already exists, or if any ref
/// points to an annotated tag (which would silently keep old history reachable).
pub fn rewrite_history(
    repo: &Repo,
    substitutions: &HashMap<Oid, Vec<u8>>,
    backup_ref_prefix: Option<&str>,
) -> Result<RewriteStats> {
    // Backup refs must live under refs/ or git reachability (and gc) would ignore
    // them, silently defeating their whole purpose.
    let backup_prefix = backup_ref_prefix.map(|p| p.trim_end_matches('/'));
    if let Some(prefix) = backup_prefix {
        if !prefix.starts_with("refs/") {
            return Err(Error::Format(format!(
                "backup ref prefix '{prefix}' must start with 'refs/'"
            )));
        }
    }

    // 1. Write the replacement blobs up front (object writes are additive/safe).
    let odb = repo.git.odb()?;
    let mut blob_map: HashMap<Oid, Oid> = HashMap::with_capacity(substitutions.len());
    for (&old_oid, content) in substitutions {
        let new_oid = odb.write(ObjectType::Blob, content)?;
        blob_map.insert(old_oid, new_oid);
    }
    let blobs_written = blob_map.len();

    // 2. Collect the direct refs to rewrite. Backup refs (from this run's prefix,
    // including any created by prior runs) are excluded; symbolic refs (e.g. HEAD)
    // follow their target so they need no rewriting of their own.
    let mut ref_tips: Vec<(String, Oid)> = Vec::new();
    for reference in repo.git.references()? {
        let reference = reference?;
        if reference.kind() != Some(git2::ReferenceType::Direct) {
            continue;
        }
        let name = match reference.name() {
            Some(n) => n.to_string(),
            None => {
                return Err(Error::Format(
                    "cannot rewrite history: ref with non-UTF-8 name".to_string(),
                ))
            }
        };
        if let Some(prefix) = backup_prefix {
            if name == prefix || name.starts_with(&format!("{prefix}/")) {
                continue;
            }
        }
        let target = reference
            .target()
            .expect("direct reference always has a target");
        match repo.git.find_object(target, None)?.kind() {
            Some(ObjectType::Commit) => ref_tips.push((name, target)),
            Some(ObjectType::Tag) => {
                return Err(Error::Format(format!(
                    "cannot rewrite history: ref '{name}' points to an annotated tag, \
                     which is not supported"
                )))
            }
            kind => {
                return Err(Error::Format(format!(
                    "cannot rewrite history: ref '{name}' points to a {} object, \
                     not a commit",
                    kind.map_or("unknown", |k| k.str())
                )))
            }
        }
    }

    // 3. Walk every commit parents-first, rewriting trees and remapping parents.
    let mut walk = repo.git.revwalk()?;
    walk.set_sorting(Sort::TOPOLOGICAL | Sort::REVERSE)?;
    for (_, tip) in &ref_tips {
        walk.push(*tip)?;
    }

    let mut tree_memo: HashMap<Oid, Oid> = HashMap::new();
    // old commit oid -> new commit oid; absent means the commit kept its identity.
    let mut commit_map: HashMap<Oid, Oid> = HashMap::new();
    let mut commits_rewritten = 0usize;

    for oid in walk {
        let oid = oid?;
        let commit = repo.git.find_commit(oid)?;
        let new_tree_oid = rewrite_tree(repo, commit.tree_id(), &blob_map, &mut tree_memo)?;
        let new_parents: Vec<Oid> = commit
            .parent_ids()
            .map(|p| commit_map.get(&p).copied().unwrap_or(p))
            .collect();
        let unchanged =
            new_tree_oid == commit.tree_id() && new_parents.iter().copied().eq(commit.parent_ids());
        if unchanged {
            continue;
        }

        // git2's commit-creation API only takes &str messages, so a non-UTF-8
        // message (or a non-UTF-8 encoding header) can't be preserved faithfully.
        if let Some(encoding) = commit.message_encoding() {
            if !encoding.eq_ignore_ascii_case("utf-8") {
                return Err(Error::Format(format!(
                    "cannot rewrite commit {oid}: unsupported message encoding '{encoding}'"
                )));
            }
        }
        let message = std::str::from_utf8(commit.message_raw_bytes()).map_err(|_| {
            Error::Format(format!("cannot rewrite commit {oid}: non-UTF-8 message"))
        })?;

        let tree = repo.git.find_tree(new_tree_oid)?;
        let parent_commits: Vec<git2::Commit<'_>> = new_parents
            .iter()
            .map(|p| repo.git.find_commit(*p))
            .collect::<std::result::Result<_, git2::Error>>()?;
        let parent_refs: Vec<&git2::Commit<'_>> = parent_commits.iter().collect();
        // No ref update yet (update_ref=None); refs flip atomically below.
        // GPG signatures are dropped, as is standard for history rewrites.
        let new_oid = repo.git.commit(
            None,
            &commit.author(),
            &commit.committer(),
            message,
            &tree,
            &parent_refs,
        )?;
        commit_map.insert(oid, new_oid);
        commits_rewritten += 1;
    }

    // 4. Which ref tips changed?
    let changed: Vec<(String, Oid, Oid)> = ref_tips
        .into_iter()
        .filter_map(|(name, tip)| commit_map.get(&tip).map(|&new| (name, tip, new)))
        .collect();
    if changed.is_empty() {
        // Noop: no backup refs are created and no refs move.
        return Ok(RewriteStats {
            commits_rewritten,
            blobs_written,
            refs_updated: Vec::new(),
            backup_refs: Vec::new(),
        });
    }

    // 5a. Pre-check EVERY backup name before creating (or flipping) anything: a
    // prior backup must never be silently clobbered, and erroring here — before
    // any ref mutation — keeps a retry with a fresh prefix fully clean.
    let backups: Vec<(String, Oid)> = match backup_prefix {
        Some(prefix) => changed
            .iter()
            .map(|(name, old_tip, _)| (format!("{prefix}/{name}"), *old_tip))
            .collect(),
        None => Vec::new(),
    };
    for (backup_name, _) in &backups {
        if repo.git.find_reference(backup_name).is_ok() {
            return Err(Error::Format(format!(
                "backup ref '{backup_name}' already exists; refusing to overwrite it"
            )));
        }
    }

    // 5b. Create the backup refs and flip the changed refs in ONE atomic ref
    // transaction (locking a nonexistent name and set_target-ing it creates the
    // ref at commit time). All-or-nothing: either every backup exists and every
    // ref is flipped, or nothing changed.
    let mut tx = repo.git.transaction()?;
    for (backup_name, _) in &backups {
        tx.lock_ref(backup_name)?;
    }
    for (name, _, _) in &changed {
        tx.lock_ref(name)?;
    }
    for (backup_name, old_tip) in &backups {
        tx.set_target(
            backup_name,
            *old_tip,
            None,
            "libkart rewrite_history: backup of pre-rewrite tip",
        )?;
    }
    for (name, _, new_tip) in &changed {
        tx.set_target(name, *new_tip, None, "libkart rewrite_history")?;
    }
    tx.commit()?;
    let backup_refs: Vec<String> = backups.into_iter().map(|(name, _)| name).collect();

    Ok(RewriteStats {
        commits_rewritten,
        blobs_written,
        refs_updated: changed.into_iter().map(|(name, _, _)| name).collect(),
        backup_refs,
    })
}

/// Rewrite `tree_oid` applying `blob_map`, returning the new tree oid — or the
/// ORIGINAL oid when nothing inside changed, so untouched (sub)trees keep their
/// identity and the memo stays effective across commits.
fn rewrite_tree(
    repo: &Repo,
    tree_oid: Oid,
    blob_map: &HashMap<Oid, Oid>,
    memo: &mut HashMap<Oid, Oid>,
) -> Result<Oid> {
    if let Some(&mapped) = memo.get(&tree_oid) {
        return Ok(mapped);
    }
    let tree = repo.git.find_tree(tree_oid)?;
    // (name, new oid, raw filemode) for entries that change.
    let mut replacements: Vec<(Vec<u8>, Oid, i32)> = Vec::new();
    for entry in tree.iter() {
        let new_id = match entry.kind() {
            Some(ObjectType::Blob) => blob_map.get(&entry.id()).copied().unwrap_or(entry.id()),
            Some(ObjectType::Tree) => rewrite_tree(repo, entry.id(), blob_map, memo)?,
            // Submodule commits etc. are left untouched.
            _ => entry.id(),
        };
        if new_id != entry.id() {
            replacements.push((entry.name_bytes().to_vec(), new_id, entry.filemode_raw()));
        }
    }
    let new_oid = if replacements.is_empty() {
        tree_oid
    } else {
        let mut builder = repo.git.treebuilder(Some(&tree))?;
        for (name, id, filemode) in &replacements {
            builder.insert(name.as_slice(), *id, *filemode)?;
        }
        builder.write()?
    };
    memo.insert(tree_oid, new_oid);
    Ok(new_oid)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::{find_feature_blobs, FeatureQuery};
    use git2::ObjectType;
    use rmpv::Value as MpValue;
    use std::collections::HashSet;
    use std::process::Command;

    const EDITING_TGZ: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/../tests/data/editing.tgz");

    fn extract_fixture(tgz: &str, subdir: &str, label: &str) -> std::path::PathBuf {
        crate::test_support::disable_owner_validation();
        let base = std::env::temp_dir().join(format!(
            "libkart-rewritetest-{}-{}-{}",
            label,
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

    /// Editing fixture: two commits on branch `main` ("Import ..." then "R1 edits")
    /// over dataset `editing` (int pk `id`, text `value`). Returns [head, head~1].
    fn open_editing(label: &str) -> (std::path::PathBuf, Repo, [Oid; 2]) {
        let root = extract_fixture(EDITING_TGZ, "editing", label);
        let repo = Repo::open(root.to_str().unwrap()).unwrap();
        let head = repo.git.revparse_single("HEAD").unwrap().id();
        let prev = repo.git.revparse_single("HEAD~1").unwrap().id();
        (root, repo, [head, prev])
    }

    /// The blob oid + repo-relative path for the feature with the given pk, at `commit`.
    fn feature_blob_at(repo: &Repo, commit: Oid, pk: i64) -> (Oid, String) {
        let hits = find_feature_blobs(
            repo,
            &[commit],
            "editing",
            &FeatureQuery::Pk(vec![MpValue::from(pk)]),
        )
        .unwrap();
        assert_eq!(hits.len(), 1);
        (hits[0].blob_oid, hits[0].path.clone())
    }

    /// All blob oids in any tree of any commit reachable from refs NOT under
    /// `exclude_prefix`.
    fn reachable_blob_oids(repo: &Repo, exclude_prefix: &str) -> HashSet<Oid> {
        let mut walk = repo.git.revwalk().unwrap();
        for reference in repo.git.references().unwrap() {
            let reference = reference.unwrap();
            let name = reference.name().unwrap();
            if name.starts_with(exclude_prefix) {
                continue;
            }
            if let Some(target) = reference.target() {
                walk.push(target).unwrap();
            }
        }
        let mut blobs = HashSet::new();
        let mut seen_trees = HashSet::new();
        for oid in walk {
            let commit = repo.git.find_commit(oid.unwrap()).unwrap();
            collect_blobs(repo, commit.tree_id(), &mut seen_trees, &mut blobs);
        }
        blobs
    }

    fn collect_blobs(
        repo: &Repo,
        tree_oid: Oid,
        seen_trees: &mut HashSet<Oid>,
        blobs: &mut HashSet<Oid>,
    ) {
        if !seen_trees.insert(tree_oid) {
            return;
        }
        let tree = repo.git.find_tree(tree_oid).unwrap();
        for entry in tree.iter() {
            match entry.kind() {
                Some(ObjectType::Blob) => {
                    blobs.insert(entry.id());
                }
                Some(ObjectType::Tree) => collect_blobs(repo, entry.id(), seen_trees, blobs),
                _ => {}
            }
        }
    }

    fn assert_signatures_equal(a: &git2::Signature<'_>, b: &git2::Signature<'_>) {
        assert_eq!(a.name_bytes(), b.name_bytes());
        assert_eq!(a.email_bytes(), b.email_bytes());
        assert_eq!(a.when().seconds(), b.when().seconds());
        assert_eq!(a.when().offset_minutes(), b.when().offset_minutes());
    }

    #[test]
    fn test_rewrite_history_substitutes_blob() {
        let (root, repo, [head, prev]) = open_editing("subst");
        // pk 3 is untouched between the two commits: same blob in BOTH commits, so
        // both must be rewritten.
        let (old_oid, path) = feature_blob_at(&repo, head, 3);
        let new_content = b"substituted content".to_vec();

        let stats = rewrite_history(
            &repo,
            &HashMap::from([(old_oid, new_content.clone())]),
            Some("refs/backup/t1"),
        )
        .unwrap();

        assert_eq!(stats.commits_rewritten, 2);
        assert_eq!(stats.blobs_written, 1);
        assert_eq!(stats.refs_updated, vec!["refs/heads/main".to_string()]);
        assert_eq!(
            stats.backup_refs,
            vec!["refs/backup/t1/refs/heads/main".to_string()]
        );

        // 1. HEAD tree has a different blob at `path`, whose content is new_content.
        let new_head = repo.git.refname_to_id("refs/heads/main").unwrap();
        assert_ne!(new_head, head);
        let new_tree = repo.git.find_commit(new_head).unwrap().tree().unwrap();
        let entry = new_tree.get_path(std::path::Path::new(&path)).unwrap();
        assert_ne!(entry.id(), old_oid);
        let blob = repo.git.find_blob(entry.id()).unwrap();
        assert_eq!(blob.content(), new_content.as_slice());

        // 2. old_oid is NOT reachable from any ref outside refs/backup/.
        assert!(!reachable_blob_oids(&repo, "refs/backup/").contains(&old_oid));
        // ... but IS still reachable via the backup namespace.
        assert!(reachable_blob_oids(&repo, "refs/nosuchprefix/").contains(&old_oid));

        // 3. Backup ref points at the old tip.
        let backup = repo
            .git
            .refname_to_id("refs/backup/t1/refs/heads/main")
            .unwrap();
        assert_eq!(backup, head);

        // 4. Rewritten HEAD commit preserves author/committer/message; parent remapped.
        let old_commit = repo.git.find_commit(head).unwrap();
        let new_commit = repo.git.find_commit(new_head).unwrap();
        assert_signatures_equal(&old_commit.author(), &new_commit.author());
        assert_signatures_equal(&old_commit.committer(), &new_commit.committer());
        assert_eq!(
            old_commit.message_raw_bytes(),
            new_commit.message_raw_bytes()
        );
        assert_eq!(new_commit.parent_count(), 1);
        // The blob was in both commits, so the parent ("Import") was rewritten too.
        let new_parent = new_commit.parent(0).unwrap();
        assert_ne!(new_parent.id(), prev);
        let old_parent = repo.git.find_commit(prev).unwrap();
        assert_signatures_equal(&old_parent.author(), &new_parent.author());
        assert_eq!(
            old_parent.message_raw_bytes(),
            new_parent.message_raw_bytes()
        );
        assert_eq!(new_parent.parent_count(), 0);
        // The rewritten parent's tree also has the substitution.
        let parent_entry = new_parent
            .tree()
            .unwrap()
            .get_path(std::path::Path::new(&path))
            .unwrap();
        assert_eq!(parent_entry.id(), entry.id());

        let _ = std::fs::remove_dir_all(root.parent().unwrap());
    }

    #[test]
    fn test_rewrite_preserves_untouched_commit_identity() {
        let (root, repo, [head, prev]) = open_editing("identity");
        // pk 8 was added by "R1 edits": present at HEAD only, so the root commit
        // ("Import") contains neither the blob nor a rewritten ancestor and must
        // keep its original oid.
        let (old_oid, _path) = feature_blob_at(&repo, head, 8);

        let stats = rewrite_history(
            &repo,
            &HashMap::from([(old_oid, b"changed".to_vec())]),
            Some("refs/backup/t1"),
        )
        .unwrap();

        assert_eq!(stats.commits_rewritten, 1);
        let new_head = repo.git.refname_to_id("refs/heads/main").unwrap();
        assert_ne!(new_head, head);
        let new_commit = repo.git.find_commit(new_head).unwrap();
        assert_eq!(new_commit.parent_id(0).unwrap(), prev);
        assert!(!reachable_blob_oids(&repo, "refs/backup/").contains(&old_oid));

        let _ = std::fs::remove_dir_all(root.parent().unwrap());
    }

    #[test]
    fn test_rewrite_history_empty_map_is_noop() {
        let (root, repo, [head, _prev]) = open_editing("empty");
        let stats = rewrite_history(&repo, &HashMap::new(), Some("refs/backup/t1")).unwrap();
        assert_eq!(stats.commits_rewritten, 0);
        assert_eq!(stats.blobs_written, 0);
        assert!(stats.refs_updated.is_empty());
        assert!(stats.backup_refs.is_empty());
        // Refs untouched, no backup ref created.
        assert_eq!(repo.git.refname_to_id("refs/heads/main").unwrap(), head);
        assert!(repo
            .git
            .find_reference("refs/backup/t1/refs/heads/main")
            .is_err());
        let _ = std::fs::remove_dir_all(root.parent().unwrap());
    }

    #[test]
    fn test_rewrite_is_idempotent() {
        let (root, repo, [head, _prev]) = open_editing("idem");
        let (old_oid, _path) = feature_blob_at(&repo, head, 3);
        let subs = HashMap::from([(old_oid, b"substituted".to_vec())]);

        let stats1 = rewrite_history(&repo, &subs, Some("refs/backup/t1")).unwrap();
        assert!(stats1.commits_rewritten >= 1);
        let tip_after_first = repo.git.refname_to_id("refs/heads/main").unwrap();

        // Second call with the same map: old_oid is no longer present anywhere
        // outside the backup namespace -> noop, refs unchanged, no backup-exists
        // error since noop is detected before backups are created.
        let stats2 = rewrite_history(&repo, &subs, Some("refs/backup/t1")).unwrap();
        assert_eq!(stats2.commits_rewritten, 0);
        assert!(stats2.refs_updated.is_empty());
        assert!(stats2.backup_refs.is_empty());
        assert_eq!(
            repo.git.refname_to_id("refs/heads/main").unwrap(),
            tip_after_first
        );
        // Backup still points at the original tip from the first run.
        assert_eq!(
            repo.git
                .refname_to_id("refs/backup/t1/refs/heads/main")
                .unwrap(),
            head
        );
        let _ = std::fs::remove_dir_all(root.parent().unwrap());
    }

    #[test]
    fn test_rewrite_preexisting_backup_blocks_all_mutation() {
        let (root, repo, [head, _prev]) = open_editing("prebackup");
        // A second branch, so more than one ref needs a backup.
        repo.git
            .reference("refs/heads/other", head, false, "test")
            .unwrap();
        let (old_oid, _path) = feature_blob_at(&repo, head, 3);
        // Pre-existing backup for ONE of the refs.
        repo.git
            .reference("refs/backup/t1/refs/heads/other", head, false, "test")
            .unwrap();

        let err = rewrite_history(
            &repo,
            &HashMap::from([(old_oid, b"x".to_vec())]),
            Some("refs/backup/t1"),
        )
        .unwrap_err();
        assert!(err.to_string().contains("already exists"), "{err}");

        // Nothing was mutated: no backup created for the OTHER ref, no flips.
        assert!(repo
            .git
            .find_reference("refs/backup/t1/refs/heads/main")
            .is_err());
        assert_eq!(repo.git.refname_to_id("refs/heads/main").unwrap(), head);
        assert_eq!(repo.git.refname_to_id("refs/heads/other").unwrap(), head);

        let _ = std::fs::remove_dir_all(root.parent().unwrap());
    }

    #[test]
    fn test_rewrite_backup_prefix_must_start_with_refs() {
        let (root, repo, [head, _prev]) = open_editing("badprefix");
        let (old_oid, _path) = feature_blob_at(&repo, head, 3);
        for prefix in ["backup", "backup/t1", "refs"] {
            let err = rewrite_history(
                &repo,
                &HashMap::from([(old_oid, b"x".to_vec())]),
                Some(prefix),
            )
            .unwrap_err();
            assert!(err.to_string().contains("must start with 'refs/'"), "{err}");
        }
        // Refs untouched.
        assert_eq!(repo.git.refname_to_id("refs/heads/main").unwrap(), head);
        let _ = std::fs::remove_dir_all(root.parent().unwrap());
    }

    #[test]
    fn test_rewrite_errors_on_annotated_tag() {
        let (root, repo, [head, _prev]) = open_editing("anntag");
        let (old_oid, _path) = feature_blob_at(&repo, head, 3);
        let obj = repo.git.find_object(head, None).unwrap();
        let sig = git2::Signature::now("tagger", "tagger@example.com").unwrap();
        repo.git.tag("v1", &obj, &sig, "a tag", false).unwrap();

        let err = rewrite_history(
            &repo,
            &HashMap::from([(old_oid, b"x".to_vec())]),
            Some("refs/backup/t1"),
        )
        .unwrap_err();
        assert!(err.to_string().contains("annotated tag"), "{err}");
        // Refs untouched.
        assert_eq!(repo.git.refname_to_id("refs/heads/main").unwrap(), head);
        let _ = std::fs::remove_dir_all(root.parent().unwrap());
    }
}
