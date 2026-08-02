//! Android APK asset file reading via AAssetManager.
//!
//! `std::fs::read` on Android falls back to this module when the POSIX
//! path fails, allowing transparent reading of assets bundled inside
//! the APK (typically accessed via relative paths like `"Helmet.vpackage"`).

use crate::ffi::{CStr, CString, OsStr, OsString};
use crate::io::{self, Error, ErrorKind};
use crate::path::Path;
use crate::sync::OnceLock;

/// Opaque pointer to `AAssetManager*`.
/// Set once during Android app initialization (`android_main` / `ANativeActivity`).
static ASSET_MANAGER: OnceLock<*mut std::ffi::c_void> = OnceLock::new();

/// Store the `AAssetManager*` pointer obtained from the Android app's
/// `ANativeActivity->assetManager`. Must be called before any `std::fs::read`
/// calls that need to access APK assets.
///
/// # Safety
/// `ptr` must be a valid `AAssetManager*` that outlives all reads.
pub unsafe fn set_asset_manager(ptr: *mut std::ffi::c_void) {
    ASSET_MANAGER.set(ptr).ok();
}

// ─── NDK FFI bindings ─────────────────────────────────────────────

/// AAsset reading mode: return full buffer in one shot.
const AASSET_MODE_BUFFER: i32 = 3;

#[repr(C)]
pub struct AAsset(std::ffi::c_void);

#[repr(C)]
pub struct AAssetDir(std::ffi::c_void);

extern "C" {
    fn AAssetManager_open(
        mgr: *mut std::ffi::c_void,
        filename: *const std::ffi::c_char,
        mode: i32,
    ) -> *mut AAsset;
    fn AAssetManager_openDir(
        mgr: *mut std::ffi::c_void,
        dirname: *const std::ffi::c_char,
    ) -> *mut AAssetDir;
    fn AAssetDir_getNextFileName(dir: *mut AAssetDir) -> *const std::ffi::c_char;
    fn AAssetDir_close(dir: *mut AAssetDir);
    fn AAsset_getLength(asset: *const AAsset) -> usize;
    fn AAsset_getBuffer(asset: *const AAsset) -> *const std::ffi::c_void;
    fn AAsset_close(asset: *mut AAsset);
}

/// Try to read a file from APK assets via `AAssetManager`.
///
/// This is called by `std::fs::read` when the POSIX `open()` fails
/// (e.g. because the file is only inside the APK, not on the filesystem).
///
/// Returns `Err` if no asset manager is registered, the asset doesn't
/// exist, or reading fails.
pub fn read_apk_asset(path: &Path) -> io::Result<Vec<u8>> {
    let mgr = match ASSET_MANAGER.get() {
        Some(m) => *m,
        None => return Err(Error::new(ErrorKind::Unsupported, "AAssetManager not initialized")),
    };

    // Convert path to C string for NDK API.
    // Strip leading `/` or `android_asset/` prefix if present.
    let path_str = path.as_os_str().to_str().ok_or_else(|| {
        Error::new(ErrorKind::InvalidInput, "non-UTF-8 path for APK asset")
    })?;
    let asset_path = path_str
        .trim_start_matches('/')
        .trim_start_matches("android_asset/");
    let c_path = CString::new(asset_path).map_err(|_| {
        Error::new(ErrorKind::InvalidInput, "path contains null byte")
    })?;

    // Open asset
    let asset = unsafe { AAssetManager_open(mgr, c_path.as_ptr(), AASSET_MODE_BUFFER) };
    if asset.is_null() {
        return Err(Error::new(ErrorKind::NotFound, "APK asset not found"));
    }

    // Read buffer
    let len = unsafe { AAsset_getLength(asset) };
    let buf = unsafe { AAsset_getBuffer(asset) };
    if buf.is_null() {
        unsafe { AAsset_close(asset) };
        return Err(Error::new(ErrorKind::Other, "AAsset_getBuffer returned null"));
    }

    // Copy to Vec<u8>
    let slice = unsafe { crate::slice::from_raw_parts(buf as *const u8, len) };
    let data = slice.to_vec();

    unsafe { AAsset_close(asset) };
    Ok(data)
}

/// List files in an APK assets directory via `AAssetDir`.
///
/// Called by `std::fs::read_dir` on Android when POSIX `opendir()` fails
/// (the directory only exists inside the APK, not on the filesystem).
///
/// Returns the list of file NAMES (not full paths) in the directory.
pub fn read_asset_dir(path: &Path) -> io::Result<Vec<String>> {
    let mgr = match ASSET_MANAGER.get() {
        Some(m) => *m,
        None => return Err(Error::new(ErrorKind::Other, "AAssetManager not initialized")),
    };

    let path_str = path.as_os_str().to_str().ok_or_else(|| {
        Error::new(ErrorKind::InvalidInput, "non-UTF-8 path for APK asset dir")
    })?;
    let dir_path = path_str.trim_start_matches('/').trim_start_matches("android_asset/");
    let c_path = CString::new(dir_path).map_err(|_| {
        Error::new(ErrorKind::InvalidInput, "path contains null byte")
    })?;

    let dir = unsafe { AAssetManager_openDir(mgr, c_path.as_ptr()) };
    if dir.is_null() {
        return Err(Error::new(ErrorKind::NotFound, "APK asset directory not found"));
    }

    let mut files = Vec::new();
    loop {
        let f = unsafe { AAssetDir_getNextFileName(dir) };
        if f.is_null() { break; }
        let name = unsafe { CStr::from_ptr(f) }.to_string_lossy().into_owned();
        files.push(name);
    }

    unsafe { AAssetDir_close(dir) };
    Ok(files)
}
