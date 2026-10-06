//! Filesystem and resume primitives rooted in an explicitly authorized directory.
#![forbid(unsafe_code)]
use i2pr_tc_core::{metainfo::TorrentFile, InfoHashV1};
use serde::{Deserialize, Serialize};
use sha1::{Digest, Sha1};
use std::{
    collections::BTreeSet,
    fs::{self, File, OpenOptions},
    io::{Read, Seek, SeekFrom, Write},
    path::{Component, Path, PathBuf},
    sync::{
        atomic::AtomicBool,
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
};
use thiserror::Error;

pub mod service;
pub use service::PersistentTorrentService;
pub mod runtime;
pub use runtime::TorrentRuntime;

const MAX_RESUME_PIECES: usize = 4_000_000;
const MAX_PIECE_LENGTH: u32 = 16 * 1024 * 1024;
static RESUME_TEMP_SEQUENCE: AtomicU64 = AtomicU64::new(0);

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
    #[error("piece data does not match its expected hash")]
    PieceHash,
    #[error("storage I/O serialization lock is poisoned")]
    Concurrency,
    #[error("storage operation was cancelled")]
    Cancelled,
}

#[derive(Clone, Default)]
pub struct Cancellation(Arc<AtomicBool>);
impl Cancellation {
    pub fn cancel(&self) {
        self.0.store(true, Ordering::Release);
    }
    pub fn is_cancelled(&self) -> bool {
        self.0.load(Ordering::Acquire)
    }
}

pub struct Storage {
    root: PathBuf,
    disk: Mutex<()>,
}
impl Storage {
    pub fn open(root: impl AsRef<Path>) -> Result<Self, StorageError> {
        fs::create_dir_all(root.as_ref())?;
        let root = fs::canonicalize(root)?;
        Ok(Self {
            root,
            disk: Mutex::new(()),
        })
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
                || !portable_component(part)
            {
                return Err(StorageError::Path);
            }
            p.push(candidate);
        }
        Ok(p)
    }
    pub fn prepare(&self, files: &[TorrentFile]) -> Result<(), StorageError> {
        let _permit = self.disk.lock().map_err(|_| StorageError::Concurrency)?;
        let mut paths = BTreeSet::new();
        let mut portable_paths = BTreeSet::new();
        let mut resolved = Vec::with_capacity(files.len());
        let _total = files
            .iter()
            .try_fold(0u64, |total, file| total.checked_add(file.length))
            .ok_or(StorageError::Overflow)?;
        for f in files {
            if !paths.insert(f.path.clone()) {
                return Err(StorageError::Layout);
            }
            let portable: Vec<String> = f.path.iter().map(|part| part.to_lowercase()).collect();
            if !portable_paths.insert(portable.clone()) {
                return Err(StorageError::Layout);
            }
            for depth in 1..f.path.len() {
                if paths.contains(&f.path[..depth]) || portable_paths.contains(&portable[..depth]) {
                    return Err(StorageError::Layout);
                }
            }
            if paths
                .range(f.path.clone()..)
                .nth(1)
                .is_some_and(|next| next.starts_with(&f.path))
                || portable_paths
                    .range(portable.clone()..)
                    .nth(1)
                    .is_some_and(|next| next.starts_with(&portable))
            {
                return Err(StorageError::Layout);
            }
            let p = self.safe_path(&f.path)?;
            self.reject_symlink_ancestors(&p)?;
            if p.exists() {
                let m = fs::symlink_metadata(&p)?;
                if m.file_type().is_symlink() || !m.is_file() || m.len() != f.length {
                    return Err(StorageError::Layout);
                }
            }
            resolved.push(p);
        }
        for (f, p) in files.iter().zip(resolved) {
            if let Some(parent) = p.parent() {
                fs::create_dir_all(parent)?;
            }
            if !p.exists() {
                let file = OpenOptions::new()
                    .read(true)
                    .write(true)
                    .create_new(true)
                    .open(p)?;
                file.set_len(f.length)?;
            }
        }
        Ok(())
    }
    fn write_piece_unlocked(
        &self,
        files: &[TorrentFile],
        piece_length: u32,
        piece_index: u32,
        bytes: &[u8],
        cancellation: Option<&Cancellation>,
    ) -> Result<(), StorageError> {
        if piece_length == 0 || piece_length > MAX_PIECE_LENGTH {
            return Err(StorageError::Layout);
        }
        let offset = (piece_index as u64)
            .checked_mul(piece_length as u64)
            .ok_or(StorageError::Overflow)?;
        let total = files
            .iter()
            .try_fold(0u64, |n, f| n.checked_add(f.length))
            .ok_or(StorageError::Overflow)?;
        let expected_length = if total == 0 || offset >= total {
            return Err(StorageError::Layout);
        } else {
            (total - offset).min(piece_length as u64) as usize
        };
        if bytes.len() != expected_length
            || offset >= total
            || bytes.len() as u64 > total - offset
            || bytes.len() as u64 > piece_length as u64
        {
            return Err(StorageError::Layout);
        }
        let mut global = offset;
        let mut consumed = 0usize;
        for f in files {
            if cancellation.is_some_and(Cancellation::is_cancelled) {
                return Err(StorageError::Cancelled);
            }
            if global >= f.length {
                global -= f.length;
                continue;
            }
            let take = (f.length - global).min((bytes.len() - consumed) as u64) as usize;
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

    /// Verify piece bytes before writing them, so rejected data never reaches
    /// storage and cannot be mistaken for a completed piece by its caller.
    pub fn write_verified_piece(
        &self,
        files: &[TorrentFile],
        piece_length: u32,
        piece_index: u32,
        bytes: &[u8],
        expected_hash: [u8; 20],
    ) -> Result<(), StorageError> {
        self.write_verified_piece_cancellable(
            files,
            piece_length,
            piece_index,
            bytes,
            expected_hash,
            &Cancellation::default(),
        )
    }

    pub fn write_verified_piece_cancellable(
        &self,
        files: &[TorrentFile],
        piece_length: u32,
        piece_index: u32,
        bytes: &[u8],
        expected_hash: [u8; 20],
        cancellation: &Cancellation,
    ) -> Result<(), StorageError> {
        if cancellation.is_cancelled() {
            return Err(StorageError::Cancelled);
        }
        if piece_length == 0
            || piece_length > MAX_PIECE_LENGTH
            || bytes.len() > piece_length as usize
        {
            return Err(StorageError::Layout);
        }
        let actual: [u8; 20] = Sha1::digest(bytes).into();
        if actual != expected_hash {
            return Err(StorageError::PieceHash);
        }
        if cancellation.is_cancelled() {
            return Err(StorageError::Cancelled);
        }
        let _permit = self.disk.lock().map_err(|_| StorageError::Concurrency)?;
        self.write_piece_unlocked(files, piece_length, piece_index, bytes, Some(cancellation))
    }
    pub fn read_piece(
        &self,
        files: &[TorrentFile],
        piece_length: u32,
        index: u32,
        len: usize,
    ) -> Result<Vec<u8>, StorageError> {
        if piece_length == 0 || piece_length > MAX_PIECE_LENGTH {
            return Err(StorageError::Layout);
        }
        let _permit = self.disk.lock().map_err(|_| StorageError::Concurrency)?;
        self.read_piece_unlocked(files, piece_length, index, len)
    }

    fn read_piece_unlocked(
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
            let take = (f.length - global).min((len - done) as u64) as usize;
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
        self.recheck_cancellable(files, piece_length, hashes, &Cancellation::default())
    }

    pub fn recheck_cancellable(
        &self,
        files: &[TorrentFile],
        piece_length: u32,
        hashes: &[[u8; 20]],
        cancellation: &Cancellation,
    ) -> Result<Vec<bool>, StorageError> {
        if cancellation.is_cancelled() {
            return Err(StorageError::Cancelled);
        }
        let _permit = self.disk.lock().map_err(|_| StorageError::Concurrency)?;
        if piece_length == 0 || piece_length > MAX_PIECE_LENGTH {
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
            if cancellation.is_cancelled() {
                return Err(StorageError::Cancelled);
            }
            let off = (i as u64)
                .checked_mul(piece_length as u64)
                .ok_or(StorageError::Overflow)?;
            let len = (total - off).min(piece_length as u64) as usize;
            let index = u32::try_from(i).map_err(|_| StorageError::Overflow)?;
            let actual = self
                .read_piece_unlocked(files, piece_length, index, len)
                .ok()
                .map(|b| {
                    let digest: [u8; 20] = Sha1::digest(b).into();
                    digest
                });
            good.push(actual.as_ref() == Some(expected));
        }
        Ok(good)
    }

    pub fn save_resume(&self, resume: &ResumeState) -> Result<(), StorageError> {
        let _permit = self.disk.lock().map_err(|_| StorageError::Concurrency)?;
        resume.validate(
            InfoHashV1(resume.info_hash),
            resume.verified.len(),
            MAX_RESUME_PIECES,
        )?;
        let directory = self.root.join(".resume");
        if directory.exists() && fs::symlink_metadata(&directory)?.file_type().is_symlink() {
            return Err(StorageError::Path);
        }
        fs::create_dir_all(&directory)?;
        let name = format!("{}.json", hex_hash(resume.info_hash));
        let path = self.safe_path(&[".resume".into(), name])?;
        self.reject_symlink_ancestors(&path)?;
        let mut bytes = Vec::new();
        serde_json::to_writer(&mut bytes, resume).map_err(|_| StorageError::Resume)?;
        atomic_write(&path, &bytes)
    }

    pub fn load_resume(
        &self,
        hash: InfoHashV1,
        pieces: usize,
        max_pieces: usize,
        max_bytes: u64,
    ) -> Result<ResumeState, StorageError> {
        let _permit = self.disk.lock().map_err(|_| StorageError::Concurrency)?;
        if pieces > max_pieces || max_pieces > MAX_RESUME_PIECES {
            return Err(StorageError::Resume);
        }
        let name = format!("{}.json", hex_hash(hash.0));
        let path = self.safe_path(&[".resume".into(), name])?;
        self.reject_symlink_ancestors(&path)?;
        let file = File::open(path)?;
        if file.metadata()?.len() > max_bytes {
            return Err(StorageError::Resume);
        }
        let limit = max_bytes.checked_add(1).ok_or(StorageError::Resume)?;
        let mut bytes = Vec::new();
        file.take(limit).read_to_end(&mut bytes)?;
        if bytes.len() as u64 > max_bytes {
            return Err(StorageError::Resume);
        }
        let state: ResumeState =
            serde_json::from_slice(&bytes).map_err(|_| StorageError::Resume)?;
        state.validate(hash, pieces, max_pieces)?;
        Ok(state)
    }

    pub fn remove_data(&self, files: &[TorrentFile]) -> Result<(), StorageError> {
        let _permit = self.disk.lock().map_err(|_| StorageError::Concurrency)?;
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
}

fn hex_hash(hash: [u8; 20]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(hash.len() * 2);
    for byte in hash {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0f) as usize] as char);
    }
    out
}

fn portable_component(part: &str) -> bool {
    if part.is_empty()
        || part.len() > 255
        || part.ends_with('.')
        || part.ends_with(' ')
        || part
            .chars()
            .any(|c| c.is_control() || "<>|?*\"".contains(c))
    {
        return false;
    }
    let stem = part.split('.').next().unwrap_or("").to_ascii_uppercase();
    !matches!(stem.as_str(), "CON" | "PRN" | "AUX" | "NUL")
        && !((stem.starts_with("COM") || stem.starts_with("LPT"))
            && stem.len() == 4
            && matches!(stem.as_bytes()[3], b'1'..=b'9'))
}

fn atomic_write(path: &Path, bytes: &[u8]) -> Result<(), StorageError> {
    let parent = path.parent().ok_or(StorageError::Path)?;
    let sequence = RESUME_TEMP_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    let temp = parent.join(format!(".resume-{}-{sequence}.tmp", std::process::id()));
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&temp)?;
    let result = (|| {
        file.write_all(bytes)?;
        file.sync_all()?;
        fs::rename(&temp, path)?;
        if let Ok(directory) = File::open(parent) {
            let _ = directory.sync_all();
        }
        Ok::<(), std::io::Error>(())
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temp);
    }
    result.map_err(StorageError::Io)
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
        let first_hash: [u8; 20] = Sha1::digest(b"abcd").into();
        let second_hash: [u8; 20] = Sha1::digest(b"ef").into();
        s.write_verified_piece(&files, 4, 0, b"abcd", first_hash)
            .unwrap();
        s.write_verified_piece(&files, 4, 1, b"ef", second_hash)
            .unwrap();
        assert_eq!(s.read_piece(&files, 4, 0, 4).unwrap(), b"abcd");
        let hashes: [[u8; 20]; 2] = [Sha1::digest(b"abcd").into(), Sha1::digest(b"ef").into()];
        assert_eq!(s.recheck(&files, 4, &hashes).unwrap(), vec![true, true]);
        s.remove_data(&files).unwrap();
        assert!(!root.join("a").exists());
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn bad_piece_hash_never_changes_existing_data() {
        let root = temp_root();
        let s = Storage::open(&root).unwrap();
        let files = vec![TorrentFile {
            path: vec!["payload".into()],
            length: 4,
        }];
        s.prepare(&files).unwrap();
        let valid = Sha1::digest(b"good").into();
        s.write_verified_piece(&files, 4, 0, b"good", valid)
            .unwrap();
        assert!(matches!(
            s.write_verified_piece(&files, 4, 0, b"evil", valid),
            Err(StorageError::PieceHash)
        ));
        assert_eq!(s.read_piece(&files, 4, 0, 4).unwrap(), b"good");
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn cancellation_prevents_writes_and_aborts_recheck() {
        let root = temp_root();
        let storage = Storage::open(&root).unwrap();
        let files = vec![TorrentFile {
            path: vec!["payload".into()],
            length: 4,
        }];
        storage.prepare(&files).unwrap();
        let hash: [u8; 20] = Sha1::digest(b"data").into();
        storage
            .write_verified_piece(&files, 4, 0, b"data", hash)
            .unwrap();
        let cancel = Cancellation::default();
        cancel.cancel();
        assert!(matches!(
            storage.write_verified_piece_cancellable(&files, 4, 0, b"xxxx", hash, &cancel),
            Err(StorageError::Cancelled)
        ));
        assert_eq!(storage.read_piece(&files, 4, 0, 4).unwrap(), b"data");
        assert!(matches!(
            storage.recheck_cancellable(&files, 4, &[hash], &cancel),
            Err(StorageError::Cancelled)
        ));
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

    #[test]
    fn resume_storage_is_rooted_bounded_and_identity_checked() {
        let root = temp_root();
        let storage = Storage::open(&root).unwrap();
        let hash = InfoHashV1([7; 20]);
        let state = ResumeState {
            version: 1,
            info_hash: hash.0,
            storage_schema: 1,
            desired_running: true,
            verified: vec![true, false],
            downloaded: 9,
            uploaded: 2,
        };
        storage.save_resume(&state).unwrap();
        assert_eq!(
            storage.load_resume(hash, 2, 10, 4096).unwrap().verified,
            vec![true, false]
        );
        assert!(storage.load_resume(hash, 2, 10, 8).is_err());
        assert!(storage
            .load_resume(InfoHashV1([8; 20]), 2, 10, 4096)
            .is_err());
        assert_eq!(
            fs::read_dir(&root).unwrap().count(),
            1,
            "resume records stay under the authorized root"
        );
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn rejects_cross_platform_dangerous_path_components() {
        let root = temp_root();
        let storage = Storage::open(&root).unwrap();
        for component in ["C:", "bad\\name", "CON", "NUL.txt", "trailing.", "bad?name"] {
            assert!(
                storage
                    .prepare(&[TorrentFile {
                        path: vec![component.to_owned()],
                        length: 1,
                    }])
                    .is_err(),
                "accepted dangerous component {component:?}"
            );
        }
        let _ = fs::remove_dir_all(root);
    }

    #[cfg(unix)]
    #[test]
    fn rejects_symlinked_parent_directories() {
        use std::os::unix::fs::symlink;
        let root = temp_root();
        let outside = temp_root();
        let storage = Storage::open(&root).unwrap();
        symlink(&outside, root.join("link")).unwrap();
        assert!(storage
            .prepare(&[TorrentFile {
                path: vec!["link".into(), "escape".into()],
                length: 1,
            }])
            .is_err());
        assert!(!outside.join("escape").exists());
        let _ = fs::remove_dir_all(root);
        let _ = fs::remove_dir_all(outside);
    }
}
