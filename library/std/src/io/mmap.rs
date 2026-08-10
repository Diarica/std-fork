//! Fibre Engine extension: read-only file memory mapping (`std::io::mmap`).
//!
//! `MmapFile` — 跨平台只读文件映射：Windows MapViewOfFile / Unix mmap / fallback read。
//! 定制 std 直接公开（`#[stable]`，无需 feature gate）——`use std::io::MmapFile;` 即可。

use crate::ffi::c_void;
use crate::io;
use crate::path::Path;
use crate::vec::Vec;

#[cfg(windows)]
use crate::os::windows::ffi::OsStrExt;

#[cfg(windows)]
#[stable(feature = "io_mmap", since = "1.99.0")]
pub type WinHandle = *mut c_void;

/// 只读文件映射（或 fallback 读入内存）。
/// 生命周期内 `as_slice()` 稳定有效；Drop 时释放映射。
#[stable(feature = "io_mmap", since = "1.99.0")]
#[derive(Debug)]
pub struct MmapFile {
    inner: Inner,
}

#[derive(Debug)]
enum Inner {
    #[cfg(windows)]
    Mapped { ptr: *mut u8, len: usize, _file: WinHandle, _map: WinHandle },
    #[cfg(unix)]
    Mapped { ptr: *mut u8, len: usize, _fd: crate::os::unix::io::RawFd },
    Read(Vec<u8>),
}

// SAFETY: ptr 指向的映射在 Drop 前稳定；MmapFile 不跨线程共享可变数据。
#[stable(feature = "io_mmap", since = "1.99.0")]
unsafe impl Send for MmapFile {}
#[stable(feature = "io_mmap", since = "1.99.0")]
unsafe impl Sync for MmapFile {}

impl MmapFile {
    /// 打开文件并映射（只读）。
    #[stable(feature = "io_mmap", since = "1.99.0")]
    pub fn open(path: &Path) -> io::Result<MmapFile> {
        #[cfg(windows)]
        {
            return win::open(path);
        }
        #[cfg(unix)]
        {
            return unix::open(path);
        }
        #[cfg(not(any(windows, unix)))]
        {
            let data = crate::fs::read(path)?;
            Ok(MmapFile { inner: Inner::Read(data) })
        }
    }

    /// 映射区字节视图。
    #[stable(feature = "io_mmap", since = "1.99.0")]
    pub fn as_slice(&self) -> &[u8] {
        match &self.inner {
            #[cfg(windows)]
            Inner::Mapped { ptr, len, .. } => unsafe { crate::slice::from_raw_parts(*ptr, *len) },
            #[cfg(unix)]
            Inner::Mapped { ptr, len, .. } => unsafe { crate::slice::from_raw_parts(*ptr, *len) },
            Inner::Read(v) => v.as_slice(),
        }
    }
}

#[stable(feature = "io_mmap", since = "1.99.0")]
impl Drop for MmapFile {
    fn drop(&mut self) {
        #[cfg(windows)]
        if let Inner::Mapped { ptr, len, _file, _map } = &mut self.inner {
            // SAFETY: ptr/len 来自 MapViewOfFile，映射仍有效。
            let _ = unsafe { win::unmap(*ptr, *len) };
        }
        #[cfg(unix)]
        if let Inner::Mapped { ptr, len, _fd } = &mut self.inner {
            unix::unmap(*ptr, *len);
        }
    }
}

#[cfg(windows)]
mod win {
    use super::*;

    const GENERIC_READ: u32 = 0x8000_0000;
    const FILE_SHARE_READ: u32 = 0x1;
    const OPEN_EXISTING: u32 = 3;
    const PAGE_READONLY: u32 = 0x02;
    const FILE_MAP_READ: u32 = 0x04;

    unsafe extern "C" {
        fn CreateFileW(
            name: *const u16, access: u32, share: u32, sec: *const c_void,
            disp: u32, flags: u32, tmpl: *const c_void,
        ) -> WinHandle;
        fn CreateFileMappingW(file: WinHandle, sec: *const c_void, protect: u32, high: u32, low: u32, name: *const u16) -> WinHandle;
        fn MapViewOfFile(file_map: WinHandle, access: u32, off_high: u32, off_low: u32, bytes: usize) -> *mut c_void;
        fn UnmapViewOfFile(ptr: *const c_void) -> i32;
        fn CloseHandle(h: WinHandle) -> i32;
        fn GetLastError() -> u32;
    }

    pub fn open(path: &Path) -> io::Result<MmapFile> {
        let len = crate::fs::metadata(path)?.len() as usize;
        if len == 0 {
            // 0 字节文件无法映射（MapViewOfFile 失败）——fallback 空缓冲。
            return Ok(MmapFile { inner: Inner::Read(Vec::new()) });
        }
        let wide: Vec<u16> = path.as_os_str().encode_wide().chain(crate::iter::once(0)).collect();
        unsafe {
            let file = CreateFileW(wide.as_ptr(), GENERIC_READ, FILE_SHARE_READ, crate::ptr::null(), OPEN_EXISTING, 0, crate::ptr::null_mut());
            if file.is_null() { return Err(io::Error::last_os_error()); }
            let map = CreateFileMappingW(file, crate::ptr::null(), PAGE_READONLY, 0, 0, crate::ptr::null());
            if map.is_null() {
                let e = io::Error::last_os_error();
                CloseHandle(file);
                return Err(e);
            }
            let ptr = MapViewOfFile(map, FILE_MAP_READ, 0, 0, 0);
            if ptr.is_null() {
                let e = io::Error::last_os_error();
                CloseHandle(map);
                CloseHandle(file);
                return Err(e);
            }
            Ok(MmapFile { inner: Inner::Mapped { ptr: ptr as *mut u8, len, _file: file, _map: map } })
        }
    }

    pub unsafe fn unmap(ptr: *mut u8, _len: usize) -> i32 {
        UnmapViewOfFile(ptr as *const c_void)
    }
}

#[cfg(unix)]
mod unix {
    use super::*;
    use crate::ffi::CString;
    use crate::os::unix::ffi::OsStrExt;

    const PROT_READ: i32 = 0x1;
    const MAP_SHARED: i32 = 0x01;
    const O_RDONLY: i32 = 0;

    unsafe extern "C" {
        fn open(path: *const crate::ffi::c_char, flags: i32, mode: u32) -> i32;
        fn mmap(addr: *mut c_void, len: usize, prot: i32, flags: i32, fd: i32, off: i64) -> *mut c_void;
        fn munmap(addr: *mut c_void, len: usize) -> i32;
        fn close(fd: i32) -> i32;
    }

    pub fn open(path: &Path) -> io::Result<MmapFile> {
        let cpath = CString::new(path.as_os_str().as_bytes())
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "path contains NUL"))?;
        unsafe {
            let fd = open(cpath.as_ptr(), O_RDONLY, 0);
            if fd < 0 { return Err(io::Error::last_os_error()); }
            let len = crate::fs::metadata(path)?.len() as usize;
            if len == 0 {
                close(fd);
                return Ok(MmapFile { inner: Inner::Read(Vec::new()) });
            }
            let ptr = mmap(crate::ptr::null_mut(), len, PROT_READ, MAP_SHARED, fd, 0);
            if ptr == usize::MAX as *mut c_void { close(fd); return Err(io::Error::last_os_error()); }
            close(fd);
            Ok(MmapFile { inner: Inner::Mapped { ptr: ptr as *mut u8, len, _fd: fd } })
        }
    }

    pub fn unmap(ptr: *mut u8, len: usize) {
        unsafe { munmap(ptr as *mut c_void, len); }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fs;

    #[test]
    fn mmap_roundtrip() {
        let dir = crate::env::temp_dir().join(format!("std_mmap_{}", crate::process::id()));
        fs::create_dir_all(&dir).unwrap();
        let p = dir.join("t.bin");
        let data: Vec<u8> = (0..4096u32).map(|i| (i % 251) as u8).collect();
        fs::write(&p, &data).unwrap();
        let mm = MmapFile::open(&p).unwrap();
        assert_eq!(mm.as_slice(), data.as_slice());
        drop(mm);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn mmap_empty_file() {
        let dir = crate::env::temp_dir().join(format!("std_mmap_e_{}", crate::process::id()));
        fs::create_dir_all(&dir).unwrap();
        let p = dir.join("e.bin");
        fs::write(&p, []).unwrap();
        let mm = MmapFile::open(&p).unwrap();
        assert!(mm.as_slice().is_empty());
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn mmap_missing_file_errors() {
        assert!(MmapFile::open(Path::new("/nonexistent/std_mmap_test")).is_err());
    }
}
