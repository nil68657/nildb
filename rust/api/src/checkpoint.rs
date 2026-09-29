//! File helpers the engines' `checkpoint` calls share. A checkpoint of `dir`
//! is built in `dir.tmp` and renamed to `dir` once every file in it is
//! synced, so a crash leaves either no `dir` or a complete one.

use std::ffi::OsString;
use std::fs::{self, File, OpenOptions};
use std::path::{Path, PathBuf};

use crate::error::{Error, IoContext, Result};

/// The temporary directory a checkpoint of `dir` is built in.
pub fn tmp_path(dir: &Path) -> PathBuf {
    let mut name = OsString::from(dir.as_os_str());
    name.push(".tmp");
    PathBuf::from(name)
}

/// Starts a checkpoint into `dir`: `dir` must not exist and its parent must.
/// Removes a temporary directory an earlier failed attempt left behind and
/// returns a fresh, empty one.
pub fn begin(dir: &Path) -> Result<PathBuf> {
    if dir.as_os_str().is_empty() {
        return Err(Error::invalid("checkpoint directory is empty"));
    }
    if fs::symlink_metadata(dir).is_ok() {
        return Err(Error::invalid(format!(
            "checkpoint directory {} already exists",
            dir.display()
        )));
    }
    if let Some(parent) = dir.parent().filter(|p| !p.as_os_str().is_empty())
        && !parent.is_dir()
    {
        return Err(Error::NotFound(format!(
            "parent of checkpoint directory {} does not exist",
            dir.display()
        )));
    }
    let tmp = tmp_path(dir);
    if fs::symlink_metadata(&tmp).is_ok() {
        fs::remove_dir_all(&tmp).ctx(|| format!("remove {}", tmp.display()))?;
    }
    fs::create_dir(&tmp).ctx(|| format!("create {}", tmp.display()))?;
    Ok(tmp)
}

/// Ends a checkpoint: syncs `tmp` and every directory below it, renames it
/// to `dir` and syncs the parent. Files must already be synced.
pub fn finish(tmp: &Path, dir: &Path) -> Result<()> {
    sync_tree(tmp)?;
    fs::rename(tmp, dir).ctx(|| format!("rename {} to {}", tmp.display(), dir.display()))?;
    match dir.parent().filter(|p| !p.as_os_str().is_empty()) {
        Some(parent) => sync_dir(parent),
        None => sync_dir(Path::new(".")),
    }
}

/// Removes the temporary directory of a checkpoint that failed.
pub fn abandon(tmp: &Path) {
    let _ = fs::remove_dir_all(tmp);
}

fn sync_tree(dir: &Path) -> Result<()> {
    for e in fs::read_dir(dir).ctx(|| format!("list {}", dir.display()))? {
        let e = e.ctx(|| format!("list {}", dir.display()))?;
        if e.file_type()
            .ctx(|| format!("stat {}", e.path().display()))?
            .is_dir()
        {
            sync_tree(&e.path())?;
        }
    }
    sync_dir(dir)
}

/// Hard-links `src` to `dst`, or copies it (and syncs the copy) when the
/// link fails, for example because `dst` is on another file system.
pub fn link_or_copy(src: &Path, dst: &Path) -> Result<()> {
    if fs::hard_link(src, dst).is_ok() {
        return Ok(());
    }
    copy_synced(src, dst).map(|_| ())
}

/// Copies `src` to `dst` and syncs `dst`. On macOS `fs::copy` clones the
/// file on APFS (`fclonefileat`), which takes the contents at one instant,
/// and falls back to copying bytes elsewhere.
pub fn copy_synced(src: &Path, dst: &Path) -> Result<u64> {
    let n = fs::copy(src, dst).ctx(|| format!("copy {} to {}", src.display(), dst.display()))?;
    sync_file(dst)?;
    Ok(n)
}

/// Syncs a file's data (`F_FULLFSYNC` on macOS).
pub fn sync_file(path: &Path) -> Result<()> {
    OpenOptions::new()
        .write(true)
        .open(path)
        .and_then(|f| f.sync_data())
        .ctx(|| format!("sync {}", path.display()))
}

/// Syncs a directory so the names created in it survive a crash.
pub fn sync_dir(dir: &Path) -> Result<()> {
    let d = File::open(dir).ctx(|| format!("open directory {}", dir.display()))?;
    match d.sync_all() {
        Ok(()) => Ok(()),
        // Some file systems refuse fsync on a directory descriptor.
        Err(e) if e.kind() == std::io::ErrorKind::InvalidInput || e.raw_os_error() == Some(45) => {
            Ok(())
        }
        Err(e) => Err(Error::io(format!("sync directory {}", dir.display()), &e)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn begin_refuses_existing_and_orphaned_paths() {
        let base = std::env::temp_dir().join(format!("nil-ckpt-{}", std::process::id()));
        let _ = fs::remove_dir_all(&base);
        fs::create_dir_all(&base).unwrap();
        let dir = base.join("cp");
        let tmp = begin(&dir).unwrap();
        assert_eq!(tmp, base.join("cp.tmp"));
        fs::write(tmp.join("f"), b"x").unwrap();
        // A second attempt starts over in a fresh temporary directory.
        let tmp = begin(&dir).unwrap();
        assert!(!tmp.join("f").exists());
        fs::write(tmp.join("f"), b"x").unwrap();
        sync_file(&tmp.join("f")).unwrap();
        finish(&tmp, &dir).unwrap();
        assert_eq!(fs::read(dir.join("f")).unwrap(), b"x");
        assert!(!tmp.exists());
        assert!(matches!(begin(&dir), Err(Error::InvalidArgument(_))));
        assert!(matches!(
            begin(&base.join("missing/cp")),
            Err(Error::NotFound(_))
        ));
        let _ = fs::remove_dir_all(&base);
    }
}
