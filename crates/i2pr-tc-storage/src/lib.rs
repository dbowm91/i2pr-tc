//! Filesystem and resume primitives rooted in an explicitly authorized directory.
#![forbid(unsafe_code)]
use i2pr_tc_core::{metainfo::TorrentFile, InfoHashV1};
use serde::{Deserialize, Serialize};
use sha1::{Digest, Sha1};
use std::{
    fs::{self, File, OpenOptions},
    io::{Read, Seek, SeekFrom, Write},
    path::{Component, Path, PathBuf},
};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum StorageError {
    #[error("invalid relative path")]
    Path,
    #[error("arithmetic overflow")]
    Overflow,
    #[error("file layout does not match torrent")]
    Layout,
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error("invalid resume state")]
    Resume,
}

pub struct Storage {
    root: PathBuf,
}
impl Storage {
    pub fn open(root: impl AsRef<Path>) -> Result<Self, StorageError> {
        fs::create_dir_all(root.as_ref())?;
        let root = fs::canonicalize(root)?;
        Ok(Self { root })
    }
    pub fn root(&self) -> &Path {
        &self.root
    }
    fn safe_path(&self, parts: &[String]) -> Result<PathBuf, StorageError> {
        if parts.is_empty() {
            return Err(StorageError::Path);
        }
        let mut p = self.root.clone();
        for part in parts {
            let candidate = Path::new(part);
            if candidate.components().count() != 1
                || !matches!(candidate.components().next(), Some(Component::Normal(_)))
                || part == "."
                || part == ".."
                || part.contains('\\')
                || part.contains('/')
                || part.contains(':')
                || part.contains('\0')
            {
                return Err(StorageError::Path);
            }
            p.push(candidate);
        }
        Ok(p)
    }
    pub fn prepare(&self, files: &[TorrentFile]) -> Result<(), StorageError> {
        for f in files {
            let p = self.safe_path(&f.path)?;
            self.reject_symlink_ancestors(&p)?;
            if let Some(parent) = p.parent() {
                fs::create_dir_all(parent)?;
            }
            if p.exists() {
                let m = fs::symlink_metadata(&p)?;
                if m.file_type().is_symlink() || !m.is_file() || m.len() != f.length {
                    return Err(StorageError::Layout);
                }
            } else {
                let file = OpenOptions::new()
                    .read(true)
                    .write(true)
                    .create_new(true)
                    .open(&p)?;
                file.set_len(f.length)?;
            }
        }
        Ok(())
    }
    pub fn write_piece(
        &self,
        files: &[TorrentFile],
        piece_length: u32,
        piece_index: u32,
        bytes: &[u8],
    ) -> Result<(), StorageError> {
        if piece_length == 0 {
            return Err(StorageError::Layout);
        }
        let offset = (piece_index as u64)
            .checked_mul(piece_length as u64)
            .ok_or(StorageError::Overflow)?;
        let total = files
            .iter()
            .try_fold(0u64, |n, f| n.checked_add(f.length))
            .ok_or(StorageError::Overflow)?;
        if offset >= total
            || bytes.len() as u64 > total - offset
            || bytes.len() as u64 > piece_length as u64
        {
            return Err(StorageError::Layout);
        }
        let mut global = offset;
        let mut consumed = 0usize;
        for f in files {
            if global >= f.length {
                global -= f.length;
                continue;
            }
            let take = ((f.length - global) as usize).min(bytes.len() - consumed);
            if take == 0 {
                break;
            }
            let p = self.safe_path(&f.path)?;
            self.reject_symlink_ancestors(&p)?;
            let mut file = OpenOptions::new().write(true).open(p)?;
            file.seek(SeekFrom::Start(global))?;
            file.write_all(&bytes[consumed..consumed + take])?;
            consumed += take;
            global = 0;
            if consumed == bytes.len() {
                break;
            }
        }
        if consumed != bytes.len() {
            return Err(StorageError::Layout);
        }
        Ok(())
    }
    pub fn read_piece(
        &self,
        files: &[TorrentFile],
        piece_length: u32,
        index: u32,
        len: usize,
    ) -> Result<Vec<u8>, StorageError> {
        if piece_length == 0 {
            return Err(StorageError::Layout);
        }
        if len > piece_length as usize {
            return Err(StorageError::Layout);
        }
        let offset = (index as u64)
            .checked_mul(piece_length as u64)
            .ok_or(StorageError::Overflow)?;
        let mut global = offset;
        let mut out = vec![0; len];
        let mut done = 0;
        for f in files {
            if global >= f.length {
                global -= f.length;
                continue;
            }
            let take = ((f.length - global) as usize).min(len - done);
            if take == 0 {
                break;
            }
            let p = self.safe_path(&f.path)?;
            self.reject_symlink_ancestors(&p)?;
            let mut file = File::open(p)?;
            file.seek(SeekFrom::Start(global))?;
            file.read_exact(&mut out[done..done + take])?;
            done += take;
            global = 0;
            if done == len {
                break;
            }
        }
        if done != len {
            return Err(StorageError::Layout);
        }
        Ok(out)
    }
    fn reject_symlink_ancestors(&self, path: &Path) -> Result<(), StorageError> {
        let mut cur = self.root.clone();
        for c in path
            .strip_prefix(&self.root)
            .map_err(|_| StorageError::Path)?
            .components()
        {
            cur.push(c);
            if cur.exists() && fs::symlink_metadata(&cur)?.file_type().is_symlink() {
                return Err(StorageError::Path);
            }
        }
        Ok(())
    }
    pub fn recheck(
        &self,
        files: &[TorrentFile],
        piece_length: u32,
        hashes: &[[u8; 20]],
    ) -> Result<Vec<bool>, StorageError> {
        if piece_length == 0 {
            return Err(StorageError::Layout);
        }
        let mut good = Vec::with_capacity(hashes.len());
        let total = files
            .iter()
            .try_fold(0u64, |n, f| n.checked_add(f.length))
            .ok_or(StorageError::Overflow)?;
        let expected_count = if total == 0 {
            0
        } else {
            (total - 1) / piece_length as u64 + 1
        };
        if u64::try_from(hashes.len()).map_err(|_| StorageError::Overflow)? != expected_count {
            return Err(StorageError::Layout);
        }
        for (i, expected) in hashes.iter().enumerate() {
            let off = (i as u64)
                .checked_mul(piece_length as u64)
                .ok_or(StorageError::Overflow)?;
            let len = (total - off).min(piece_length as u64) as usize;
            let actual = self
                .read_piece(files, piece_length, i as u32, len)
                .ok()
                .map(|b| {
                    let digest: [u8; 20] = Sha1::digest(b).into();
                    digest
                });
            good.push(actual.as_ref() == Some(expected));
        }
        Ok(good)
    }
    pub fn remove_data(&self, files: &[TorrentFile]) -> Result<(), StorageError> {
        for f in files {
            let p = self.safe_path(&f.path)?;
            if p.exists() {
                self.reject_symlink_ancestors(&p)?;
                fs::remove_file(p)?;
            }
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResumeState {
    pub version: u32,
    pub info_hash: [u8; 20],
    pub storage_schema: u32,
    pub desired_running: bool,
    pub verified: Vec<bool>,
    pub downloaded: u64,
    pub uploaded: u64,
}
impl ResumeState {
    pub fn validate(
        &self,
        hash: InfoHashV1,
        pieces: usize,
        max_pieces: usize,
    ) -> Result<(), StorageError> {
        if self.version != 1
            || self.storage_schema != 1
            || self.info_hash != hash.0
            || pieces > max_pieces
            || self.verified.len() != pieces
        {
            return Err(StorageError::Resume);
        }
        Ok(())
    }
    pub fn save_atomic(&self, path: impl AsRef<Path>) -> Result<(), StorageError> {
        let path = path.as_ref();
        let parent = path.parent().ok_or(StorageError::Path)?;
        fs::create_dir_all(parent)?;
        let tmp = path.with_extension("resume.tmp");
        let mut f = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&tmp)?;
        serde_json::to_writer(&mut f, self).map_err(|_| StorageError::Resume)?;
        f.sync_all()?;
        fs::rename(tmp, path)?;
        if let Ok(d) = File::open(parent) {
            let _ = d.sync_all();
        }
        Ok(())
    }
    pub fn load_bounded(path: impl AsRef<Path>, max_bytes: u64) -> Result<Self, StorageError> {
        let m = fs::metadata(path.as_ref())?;
        if m.len() > max_bytes {
            return Err(StorageError::Resume);
        }
        let f = File::open(path)?;
        let s: Self = serde_json::from_reader(f).map_err(|_| StorageError::Resume)?;
        Ok(s)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn temp_root() -> PathBuf {
        static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let p = std::env::temp_dir().join(format!(
            "i2pr-tc-test-{}-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        fs::create_dir_all(&p).unwrap();
        p
    }
    #[test]
    fn writes_and_rechecks_pieces_across_files() {
        let root = temp_root();
        let s = Storage::open(&root).unwrap();
        let files = vec![
            TorrentFile {
                path: vec!["a".into()],
                length: 3,
            },
            TorrentFile {
                path: vec!["b".into()],
                length: 3,
            },
        ];
        s.prepare(&files).unwrap();
        s.write_piece(&files, 4, 0, b"abcd").unwrap();
        s.write_piece(&files, 4, 1, b"ef").unwrap();
        assert_eq!(s.read_piece(&files, 4, 0, 4).unwrap(), b"abcd");
        let hashes: [[u8; 20]; 2] = [Sha1::digest(b"abcd").into(), Sha1::digest(b"ef").into()];
        assert_eq!(s.recheck(&files, 4, &hashes).unwrap(), vec![true, true]);
        s.remove_data(&files).unwrap();
        assert!(!root.join("a").exists());
        let _ = fs::remove_dir_all(root);
    }
    #[test]
    fn rejects_escape_components_and_stale_resume() {
        let root = temp_root();
        let s = Storage::open(&root).unwrap();
        assert!(s
            .prepare(&[TorrentFile {
                path: vec!["..".into(), "outside".into()],
                length: 1
            }])
            .is_err());
        let r = ResumeState {
            version: 1,
            info_hash: [0; 20],
            storage_schema: 1,
            desired_running: false,
            verified: vec![false],
            downloaded: 0,
            uploaded: 0,
        };
        assert!(r.validate(InfoHashV1([1; 20]), 1, 10).is_err());
        let _ = fs::remove_dir_all(root);
    }
}
