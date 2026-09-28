//! C interface to NilDB's storage engines; `rust/include/nilengine.h`
//! declares every function here and documents the conventions:
//!
//! - `nil_db`, `nil_batch`, `nil_snapshot` and `nil_iter` are opaque handles
//!   this library creates and frees.
//! - A function that can fail takes `char **errptr`. On failure it stores a
//!   malloc'd, NUL-terminated `"class: message"` string there, freeing any
//!   string already present; on success it leaves `*errptr` alone. `errptr`
//!   may be NULL.
//! - Returned buffers and strings are malloc'd; free them with `nil_free`.
//! - Every exported function catches Rust panics and reports them as
//!   `"panic: ..."` errors instead of unwinding into C.

#![allow(non_camel_case_types)]
#![allow(clippy::missing_safety_doc)]

use std::any::Any;
use std::ffi::{CStr, c_char, c_int, c_void};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::path::Path;
use std::ptr;

use nilengine_api::{
    DbIterator, Engine, EngineKind, Error, IterOptions, Options, Snapshot, WriteBatch, WriteOptions,
};

unsafe extern "C" {
    fn malloc(size: usize) -> *mut c_void;
    fn free(p: *mut c_void);
}

pub const NIL_ENGINE_LSM: c_int = 1;
pub const NIL_ENGINE_BTREE: c_int = 2;

pub struct nil_db {
    engine: Box<dyn Engine>,
}

pub struct nil_batch {
    batch: WriteBatch,
    /// First invalid call (a NULL pointer with a length); `nil_write`
    /// refuses the batch while it is set.
    err: Option<String>,
}

pub struct nil_snapshot {
    snap: Snapshot,
}

pub struct nil_iter {
    it: Box<dyn DbIterator>,
    /// An error the iterator itself cannot report: invalid arguments to a
    /// seek, or a caught panic.
    err: Option<Error>,
}

/// Opens an engine of `kind`; the Rust entry point behind `nil_open`.
pub fn open_engine(
    kind: EngineKind,
    dir: &Path,
    cfs: &[&str],
    opts: &Options,
) -> Result<Box<dyn Engine>, Error> {
    match kind {
        EngineKind::Lsm => nilengine_lsm::open_boxed(dir, cfs, opts),
        EngineKind::BTree => nilengine_btree::open_boxed(dir, cfs, opts),
    }
}

fn panic_message(p: Box<dyn Any + Send>) -> String {
    if let Some(s) = p.downcast_ref::<&str>() {
        (*s).to_string()
    } else if let Some(s) = p.downcast_ref::<String>() {
        s.clone()
    } else {
        "unknown panic payload".to_string()
    }
}

fn alloc(n: usize) -> *mut u8 {
    // SAFETY: plain C malloc; a NULL result means the process is out of
    // memory, which Rust treats as fatal too.
    let p = unsafe { malloc(n.max(1)) } as *mut u8;
    if p.is_null() {
        std::process::abort();
    }
    p
}

/// Copies `b` into malloc'd memory. At least one byte is allocated, so a
/// found empty value is still a non-NULL pointer.
fn malloc_bytes(b: &[u8]) -> *mut c_char {
    let p = alloc(b.len());
    // SAFETY: p has room for b.len() bytes.
    unsafe { ptr::copy_nonoverlapping(b.as_ptr(), p, b.len()) };
    p as *mut c_char
}

fn malloc_str(s: &str) -> *mut c_char {
    let p = alloc(s.len() + 1);
    // SAFETY: p has room for the bytes and the NUL.
    unsafe {
        ptr::copy_nonoverlapping(s.as_ptr(), p, s.len());
        *p.add(s.len()) = 0;
    }
    p as *mut c_char
}

fn set_err(errptr: *mut *mut c_char, msg: &str) {
    if errptr.is_null() {
        return;
    }
    // SAFETY: the caller passes a valid char ** (documented in the header).
    unsafe {
        if !(*errptr).is_null() {
            free(*errptr as *mut c_void);
        }
        *errptr = malloc_str(&msg.replace('\0', " "));
    }
}

/// Runs `f`, turning an error or a panic into an `errptr` message and the
/// fallback value.
fn guard<T>(errptr: *mut *mut c_char, fallback: T, f: impl FnOnce() -> Result<T, Error>) -> T {
    match catch_unwind(AssertUnwindSafe(f)) {
        Ok(Ok(v)) => v,
        Ok(Err(e)) => {
            set_err(errptr, &e.to_string());
            fallback
        }
        Err(p) => {
            set_err(errptr, &format!("panic: {}", panic_message(p)));
            fallback
        }
    }
}

/// Runs `f` where no error can be reported, returning the fallback on a
/// panic.
fn quiet<T>(fallback: T, f: impl FnOnce() -> T) -> T {
    catch_unwind(AssertUnwindSafe(f)).unwrap_or(fallback)
}

unsafe fn bytes<'a>(p: *const c_char, len: usize) -> Result<&'a [u8], Error> {
    if len == 0 {
        return Ok(&[]);
    }
    if p.is_null() {
        return Err(Error::invalid("NULL pointer with a nonzero length"));
    }
    // SAFETY: the caller promises len readable bytes at p.
    Ok(unsafe { std::slice::from_raw_parts(p as *const u8, len) })
}

/// A bound or range end: NULL is open, anything else (even empty) is a key.
unsafe fn opt_bytes<'a>(p: *const c_char, len: usize) -> Result<Option<&'a [u8]>, Error> {
    if p.is_null() {
        if len != 0 {
            return Err(Error::invalid("NULL pointer with a nonzero length"));
        }
        return Ok(None);
    }
    unsafe { bytes(p, len) }.map(Some)
}

unsafe fn cstr<'a>(p: *const c_char, what: &str) -> Result<&'a str, Error> {
    if p.is_null() {
        return Err(Error::invalid(format!("{what} is NULL")));
    }
    // SAFETY: the caller passes a NUL-terminated string.
    unsafe { CStr::from_ptr(p) }
        .to_str()
        .map_err(|_| Error::invalid(format!("{what} is not UTF-8")))
}

unsafe fn db_ref<'a>(db: *const nil_db) -> Result<&'a nil_db, Error> {
    // SAFETY: a non-NULL handle came from nil_open and is still open.
    unsafe { db.as_ref() }.ok_or_else(|| Error::invalid("nil_db handle is NULL"))
}

unsafe fn snap_opt<'a>(s: *const nil_snapshot) -> Option<&'a Snapshot> {
    // SAFETY: a non-NULL handle came from nil_snapshot_new.
    unsafe { s.as_ref() }.map(|s| &s.snap)
}

#[unsafe(no_mangle)]
pub extern "C" fn nil_version() -> *const c_char {
    concat!(env!("CARGO_PKG_VERSION"), "\0").as_ptr() as *const c_char
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn nil_free(p: *mut c_void) {
    if !p.is_null() {
        // SAFETY: p came from this library's malloc.
        unsafe { free(p) };
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn nil_open(
    engine: c_int,
    dir: *const c_char,
    cf_names: *const *const c_char,
    num_cfs: usize,
    options: *const c_char,
    errptr: *mut *mut c_char,
) -> *mut nil_db {
    guard(errptr, ptr::null_mut(), || {
        let kind = match engine {
            NIL_ENGINE_LSM => EngineKind::Lsm,
            NIL_ENGINE_BTREE => EngineKind::BTree,
            k => return Err(Error::invalid(format!("unknown engine kind {k}"))),
        };
        let dir = unsafe { cstr(dir, "dir") }?;
        if num_cfs == 0 || cf_names.is_null() {
            return Err(Error::invalid("open needs at least one column family name"));
        }
        let names = (0..num_cfs)
            .map(|i| unsafe { cstr(*cf_names.add(i), "column family name") })
            .collect::<Result<Vec<&str>, Error>>()?;
        let opts = if options.is_null() {
            Options::default()
        } else {
            Options::parse(unsafe { cstr(options, "options") }?)?
        };
        let engine = open_engine(kind, Path::new(dir), &names, &opts)?;
        Ok(Box::into_raw(Box::new(nil_db { engine })))
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn nil_close(db: *mut nil_db, errptr: *mut *mut c_char) {
    if db.is_null() {
        return;
    }
    // SAFETY: db came from nil_open and is closed only once.
    let db = unsafe { Box::from_raw(db) };
    guard(errptr, (), || db.engine.close());
    quiet((), move || drop(db));
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn nil_engine_kind(db: *const nil_db) -> c_int {
    quiet(0, || {
        match unsafe { db.as_ref() }.map(|d| d.engine.kind()) {
            Some(EngineKind::Lsm) => NIL_ENGINE_LSM,
            Some(EngineKind::BTree) => NIL_ENGINE_BTREE,
            None => 0,
        }
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn nil_max_key_len(db: *const nil_db) -> usize {
    quiet(0, || {
        unsafe { db.as_ref() }.map_or(0, |d| d.engine.max_key_len())
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn nil_batch_new() -> *mut nil_batch {
    quiet(ptr::null_mut(), || {
        Box::into_raw(Box::new(nil_batch {
            batch: WriteBatch::new(),
            err: None,
        }))
    })
}

fn batch_op(b: *mut nil_batch, f: impl FnOnce(&mut WriteBatch) -> Result<(), Error>) {
    if b.is_null() {
        return;
    }
    // SAFETY: b came from nil_batch_new and is used by one thread at a time.
    let r = catch_unwind(AssertUnwindSafe(|| f(unsafe { &mut (*b).batch })));
    let msg = match r {
        Ok(Ok(())) => return,
        Ok(Err(e)) => e.to_string(),
        Err(p) => format!("panic: {}", panic_message(p)),
    };
    let nb = unsafe { &mut *b };
    if nb.err.is_none() {
        nb.err = Some(msg);
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn nil_batch_put(
    b: *mut nil_batch,
    cf: u32,
    key: *const c_char,
    klen: usize,
    val: *const c_char,
    vlen: usize,
) {
    batch_op(b, |wb| {
        wb.put(cf, unsafe { bytes(key, klen) }?, unsafe {
            bytes(val, vlen)
        }?);
        Ok(())
    });
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn nil_batch_delete(
    b: *mut nil_batch,
    cf: u32,
    key: *const c_char,
    klen: usize,
) {
    batch_op(b, |wb| {
        wb.delete(cf, unsafe { bytes(key, klen) }?);
        Ok(())
    });
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn nil_batch_delete_range(
    b: *mut nil_batch,
    cf: u32,
    start: *const c_char,
    slen: usize,
    end: *const c_char,
    elen: usize,
) {
    batch_op(b, |wb| {
        wb.delete_range(cf, unsafe { bytes(start, slen) }?, unsafe {
            bytes(end, elen)
        }?);
        Ok(())
    });
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn nil_batch_merge(
    b: *mut nil_batch,
    cf: u32,
    key: *const c_char,
    klen: usize,
    operand: *const c_char,
    olen: usize,
) {
    batch_op(b, |wb| {
        wb.merge(cf, unsafe { bytes(key, klen) }?, unsafe {
            bytes(operand, olen)
        }?);
        Ok(())
    });
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn nil_batch_count(b: *const nil_batch) -> usize {
    quiet(0, || unsafe { b.as_ref() }.map_or(0, |b| b.batch.len()))
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn nil_batch_clear(b: *mut nil_batch) {
    quiet((), || {
        if let Some(b) = unsafe { b.as_mut() } {
            b.batch.clear();
            b.err = None;
        }
    });
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn nil_batch_destroy(b: *mut nil_batch) {
    if !b.is_null() {
        // SAFETY: b came from nil_batch_new and is destroyed once.
        let b = unsafe { Box::from_raw(b) };
        quiet((), move || drop(b));
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn nil_write(
    db: *mut nil_db,
    b: *const nil_batch,
    sync: c_int,
    errptr: *mut *mut c_char,
) {
    guard(errptr, (), || {
        let db = unsafe { db_ref(db) }?;
        let b = unsafe { b.as_ref() }.ok_or_else(|| Error::invalid("nil_batch handle is NULL"))?;
        if let Some(e) = &b.err {
            return Err(Error::invalid(format!(
                "batch holds an invalid operation: {e}"
            )));
        }
        db.engine.write(&b.batch, WriteOptions { sync: sync != 0 })
    });
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn nil_get(
    db: *mut nil_db,
    snap: *const nil_snapshot,
    cf: u32,
    key: *const c_char,
    klen: usize,
    vlen: *mut usize,
    errptr: *mut *mut c_char,
) -> *mut c_char {
    if !vlen.is_null() {
        // SAFETY: vlen points to a size_t the caller owns.
        unsafe { *vlen = 0 };
    }
    guard(errptr, ptr::null_mut(), || {
        let db = unsafe { db_ref(db) }?;
        let k = unsafe { bytes(key, klen) }?;
        match db.engine.get(cf, k, unsafe { snap_opt(snap) })? {
            None => Ok(ptr::null_mut()),
            Some(v) => {
                if !vlen.is_null() {
                    unsafe { *vlen = v.len() };
                }
                Ok(malloc_bytes(&v))
            }
        }
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn nil_multi_get(
    db: *mut nil_db,
    snap: *const nil_snapshot,
    cf: u32,
    n: usize,
    keys: *const *const c_char,
    klens: *const usize,
    vals: *mut *mut c_char,
    vlens: *mut usize,
    errptr: *mut *mut c_char,
) {
    guard(errptr, (), || {
        if n == 0 {
            return Ok(());
        }
        if keys.is_null() || klens.is_null() || vals.is_null() || vlens.is_null() {
            return Err(Error::invalid(
                "nil_multi_get needs non-NULL key, length and output arrays",
            ));
        }
        for i in 0..n {
            // SAFETY: the caller provides n-element output arrays.
            unsafe {
                *vals.add(i) = ptr::null_mut();
                *vlens.add(i) = 0;
            }
        }
        let db = unsafe { db_ref(db) }?;
        let ks = (0..n)
            .map(|i| unsafe { bytes(*keys.add(i), *klens.add(i)) })
            .collect::<Result<Vec<&[u8]>, Error>>()?;
        let got = db.engine.multi_get(cf, &ks, unsafe { snap_opt(snap) })?;
        for (i, v) in got.into_iter().enumerate() {
            if let Some(v) = v {
                unsafe {
                    *vlens.add(i) = v.len();
                    *vals.add(i) = malloc_bytes(&v);
                }
            }
        }
        Ok(())
    });
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn nil_snapshot_new(
    db: *mut nil_db,
    errptr: *mut *mut c_char,
) -> *mut nil_snapshot {
    guard(errptr, ptr::null_mut(), || {
        let snap = unsafe { db_ref(db) }?.engine.snapshot()?;
        Ok(Box::into_raw(Box::new(nil_snapshot { snap })))
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn nil_snapshot_seq(s: *const nil_snapshot) -> u64 {
    quiet(0, || unsafe { s.as_ref() }.map_or(0, |s| s.snap.seq()))
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn nil_snapshot_release(s: *mut nil_snapshot) {
    if !s.is_null() {
        // SAFETY: s came from nil_snapshot_new and is released once.
        let s = unsafe { Box::from_raw(s) };
        quiet((), move || drop(s));
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn nil_iter_new(
    db: *mut nil_db,
    snap: *const nil_snapshot,
    cf: u32,
    lower: *const c_char,
    lower_len: usize,
    upper: *const c_char,
    upper_len: usize,
    fill_cache: c_int,
    errptr: *mut *mut c_char,
) -> *mut nil_iter {
    guard(errptr, ptr::null_mut(), || {
        let db = unsafe { db_ref(db) }?;
        let opts = IterOptions {
            snapshot: unsafe { snap_opt(snap) }.cloned(),
            lower_bound: unsafe { opt_bytes(lower, lower_len) }?.map(<[u8]>::to_vec),
            upper_bound: unsafe { opt_bytes(upper, upper_len) }?.map(<[u8]>::to_vec),
            fill_cache: fill_cache != 0,
        };
        let it = db.engine.iter(cf, opts)?;
        Ok(Box::into_raw(Box::new(nil_iter { it, err: None })))
    })
}

/// Runs a positioning call. `reseek` clears an earlier stored error.
fn iter_op(
    it: *mut nil_iter,
    reseek: bool,
    f: impl FnOnce(&mut dyn DbIterator) -> Result<(), Error>,
) {
    if it.is_null() {
        return;
    }
    // SAFETY: it came from nil_iter_new and is used by one thread at a time.
    let ni = unsafe { &mut *it };
    if reseek {
        ni.err = None;
    } else if ni.err.is_some() {
        return;
    }
    let r = catch_unwind(AssertUnwindSafe(|| f(ni.it.as_mut())));
    match r {
        Ok(Ok(())) => {}
        Ok(Err(e)) => ni.err = Some(e),
        Err(p) => ni.err = Some(Error::invalid(format!("panic: {}", panic_message(p)))),
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn nil_iter_seek_to_first(it: *mut nil_iter) {
    iter_op(it, true, |i| {
        i.seek_to_first();
        Ok(())
    });
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn nil_iter_seek_to_last(it: *mut nil_iter) {
    iter_op(it, true, |i| {
        i.seek_to_last();
        Ok(())
    });
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn nil_iter_seek(it: *mut nil_iter, key: *const c_char, klen: usize) {
    iter_op(it, true, |i| {
        i.seek(unsafe { bytes(key, klen) }?);
        Ok(())
    });
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn nil_iter_seek_for_prev(
    it: *mut nil_iter,
    key: *const c_char,
    klen: usize,
) {
    iter_op(it, true, |i| {
        i.seek_for_prev(unsafe { bytes(key, klen) }?);
        Ok(())
    });
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn nil_iter_next(it: *mut nil_iter) {
    iter_op(it, false, |i| {
        i.next();
        Ok(())
    });
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn nil_iter_prev(it: *mut nil_iter) {
    iter_op(it, false, |i| {
        i.prev();
        Ok(())
    });
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn nil_iter_valid(it: *const nil_iter) -> c_int {
    quiet(0, || match unsafe { it.as_ref() } {
        Some(ni) if ni.err.is_none() && ni.it.valid() => 1,
        _ => 0,
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn nil_iter_key(it: *const nil_iter, klen: *mut usize) -> *const c_char {
    let out = quiet(None, || {
        let ni = unsafe { it.as_ref() }?;
        if ni.err.is_some() || !ni.it.valid() {
            return None;
        }
        let k = ni.it.key();
        Some((k.as_ptr(), k.len()))
    });
    if !klen.is_null() {
        // SAFETY: klen points to a size_t the caller owns.
        unsafe { *klen = out.map_or(0, |o| o.1) };
    }
    out.map_or(ptr::null(), |o| o.0 as *const c_char)
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn nil_iter_value(it: *mut nil_iter, vlen: *mut usize) -> *const c_char {
    let out = quiet(None, || {
        // SAFETY: it came from nil_iter_new.
        let ni = unsafe { it.as_mut() }?;
        if ni.err.is_some() || !ni.it.valid() {
            return None;
        }
        let v = ni.it.value();
        let out = (v.as_ptr(), v.len());
        // A lazily read value (a B+ tree overflow chain) reports its
        // read error through status.
        if let Err(e) = ni.it.status() {
            ni.err = Some(e);
            return None;
        }
        Some(out)
    });
    if !vlen.is_null() {
        unsafe { *vlen = out.map_or(0, |o| o.1) };
    }
    out.map_or(ptr::null(), |o| o.0 as *const c_char)
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn nil_iter_status(it: *const nil_iter, errptr: *mut *mut c_char) {
    guard(errptr, (), || {
        let ni = unsafe { it.as_ref() }.ok_or_else(|| Error::invalid("nil_iter handle is NULL"))?;
        if let Some(e) = &ni.err {
            return Err(e.clone());
        }
        ni.it.status()
    });
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn nil_iter_destroy(it: *mut nil_iter) {
    if !it.is_null() {
        // SAFETY: it came from nil_iter_new and is destroyed once.
        let it = unsafe { Box::from_raw(it) };
        quiet((), move || drop(it));
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn nil_flush(db: *mut nil_db, errptr: *mut *mut c_char) {
    guard(errptr, (), || unsafe { db_ref(db) }?.engine.flush());
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn nil_flush_wal(db: *mut nil_db, sync: c_int, errptr: *mut *mut c_char) {
    guard(errptr, (), || {
        unsafe { db_ref(db) }?.engine.flush_wal(sync != 0)
    });
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn nil_compact_range(
    db: *mut nil_db,
    cf: u32,
    start: *const c_char,
    slen: usize,
    end: *const c_char,
    elen: usize,
    errptr: *mut *mut c_char,
) {
    guard(errptr, (), || {
        let db = unsafe { db_ref(db) }?;
        let (s, e) = unsafe { (opt_bytes(start, slen)?, opt_bytes(end, elen)?) };
        db.engine.compact_range(cf, s, e)
    });
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn nil_property(
    db: *mut nil_db,
    cf: u32,
    name: *const c_char,
) -> *mut c_char {
    guard(ptr::null_mut(), ptr::null_mut(), || {
        let db = unsafe { db_ref(db) }?;
        let name = unsafe { cstr(name, "property name") }?;
        Ok(db
            .engine
            .property(cf, name)
            .map_or(ptr::null_mut(), |v| malloc_str(&v)))
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn nil_latest_sequence(db: *mut nil_db) -> u64 {
    quiet(0, || {
        unsafe { db.as_ref() }.map_or(0, |d| d.engine.latest_sequence())
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::ffi::CString;

    struct Err(*mut c_char);

    impl Err {
        fn new() -> Self {
            Err(ptr::null_mut())
        }

        fn take(&mut self) -> Option<String> {
            if self.0.is_null() {
                return None;
            }
            let s = unsafe { CStr::from_ptr(self.0) }
                .to_string_lossy()
                .into_owned();
            unsafe { nil_free(self.0 as *mut c_void) };
            self.0 = ptr::null_mut();
            Some(s)
        }
    }

    fn tmpdir(name: &str) -> CString {
        let base = std::env::var_os("NILENGINE_TEST_DIR")
            .map(std::path::PathBuf::from)
            .unwrap_or_else(std::env::temp_dir);
        let d = base.join(format!("capi-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&d);
        CString::new(d.to_str().unwrap()).unwrap()
    }

    fn get(db: *mut nil_db, snap: *const nil_snapshot, cf: u32, k: &[u8]) -> Option<Vec<u8>> {
        let mut e = Err::new();
        let mut len = 0usize;
        let p = unsafe {
            nil_get(
                db,
                snap,
                cf,
                k.as_ptr() as *const c_char,
                k.len(),
                &mut len,
                &mut e.0,
            )
        };
        assert_eq!(e.take(), None);
        if p.is_null() {
            return None;
        }
        let v = unsafe { std::slice::from_raw_parts(p as *const u8, len) }.to_vec();
        unsafe { nil_free(p as *mut c_void) };
        Some(v)
    }

    fn round_trip(kind: c_int, name: &str) {
        let dir = tmpdir(name);
        let names = [
            CString::new("default").unwrap(),
            CString::new("meta").unwrap(),
        ];
        let ptrs: Vec<*const c_char> = names.iter().map(|n| n.as_ptr()).collect();
        let opts = CString::new("write_buffer_size=64k;page_size=4096").unwrap();
        let mut e = Err::new();
        let db = unsafe {
            nil_open(
                kind,
                dir.as_ptr(),
                ptrs.as_ptr(),
                2,
                opts.as_ptr(),
                &mut e.0,
            )
        };
        assert_eq!(e.take(), None);
        assert!(!db.is_null());
        assert_eq!(unsafe { nil_engine_kind(db) }, kind);

        let b = nil_batch_new();
        unsafe {
            nil_batch_put(b, 0, b"k1".as_ptr() as _, 2, b"v1".as_ptr() as _, 2);
            nil_batch_put(b, 1, b"k1".as_ptr() as _, 2, ptr::null(), 0);
            nil_batch_put(b, 1, b"k2".as_ptr() as _, 2, b"v2".as_ptr() as _, 2);
            nil_batch_merge(
                b,
                0,
                b"n".as_ptr() as _,
                1,
                5i64.to_le_bytes().as_ptr() as _,
                8,
            );
            assert_eq!(nil_batch_count(b), 4);
            nil_write(db, b, 1, &mut e.0);
        }
        assert_eq!(e.take(), None);
        assert_eq!(get(db, ptr::null(), 0, b"k1").as_deref(), Some(&b"v1"[..]));
        assert_eq!(
            get(db, ptr::null(), 1, b"k1").as_deref(),
            Some(&b""[..]),
            "a found empty value"
        );
        assert_eq!(get(db, ptr::null(), 1, b"zz"), None);
        assert_eq!(
            get(db, ptr::null(), 0, b"n"),
            Some(5i64.to_le_bytes().to_vec())
        );

        let snap = unsafe { nil_snapshot_new(db, &mut e.0) };
        assert!(unsafe { nil_snapshot_seq(snap) } > 0);
        unsafe {
            nil_batch_clear(b);
            nil_batch_delete(b, 1, b"k2".as_ptr() as _, 2);
            nil_batch_delete_range(b, 0, b"a".as_ptr() as _, 1, b"z".as_ptr() as _, 1);
            nil_write(db, b, 0, &mut e.0);
        }
        assert_eq!(e.take(), None);
        assert_eq!(get(db, ptr::null(), 1, b"k2"), None);
        assert_eq!(get(db, snap, 1, b"k2").as_deref(), Some(&b"v2"[..]));
        assert_eq!(get(db, ptr::null(), 0, b"k1"), None);

        // multi_get at the snapshot
        let keys: [&[u8]; 3] = [b"k1", b"k2", b"nope"];
        let kp: Vec<*const c_char> = keys.iter().map(|k| k.as_ptr() as *const c_char).collect();
        let kl: Vec<usize> = keys.iter().map(|k| k.len()).collect();
        let mut vals = [ptr::null_mut::<c_char>(); 3];
        let mut vlens = [0usize; 3];
        unsafe {
            nil_multi_get(
                db,
                snap,
                1,
                3,
                kp.as_ptr(),
                kl.as_ptr(),
                vals.as_mut_ptr(),
                vlens.as_mut_ptr(),
                &mut e.0,
            );
        }
        assert_eq!(e.take(), None);
        assert!(!vals[0].is_null() && !vals[1].is_null() && vals[2].is_null());
        assert_eq!((vlens[0], vlens[1]), (0, 2));
        for v in vals {
            unsafe { nil_free(v as *mut c_void) };
        }

        // Reverse scan of the snapshot with bounds [k1, k3).
        let it = unsafe {
            nil_iter_new(
                db,
                snap,
                1,
                b"k1".as_ptr() as _,
                2,
                b"k3".as_ptr() as _,
                2,
                1,
                &mut e.0,
            )
        };
        assert_eq!(e.take(), None);
        unsafe { nil_snapshot_release(snap) };
        let mut seen = Vec::new();
        unsafe {
            nil_iter_seek_to_last(it);
            while nil_iter_valid(it) == 1 {
                let mut kl = 0usize;
                let k = nil_iter_key(it, &mut kl);
                seen.push(std::slice::from_raw_parts(k as *const u8, kl).to_vec());
                nil_iter_prev(it);
            }
            nil_iter_status(it, &mut e.0);
            assert_eq!(e.take(), None);
            nil_iter_seek(it, ptr::null(), 5);
            assert_eq!(nil_iter_valid(it), 0, "a NULL key with a length is refused");
            nil_iter_status(it, &mut e.0);
            assert!(e.take().unwrap().starts_with("invalid-argument"));
            nil_iter_destroy(it);
        }
        assert_eq!(seen, vec![b"k2".to_vec(), b"k1".to_vec()]);

        // Errors come back through errptr.
        unsafe {
            nil_batch_clear(b);
            nil_batch_put(b, 9, b"x".as_ptr() as _, 1, b"y".as_ptr() as _, 1);
            nil_write(db, b, 0, &mut e.0);
        }
        assert!(e.take().unwrap().starts_with("invalid-argument"));
        unsafe {
            nil_batch_clear(b);
            nil_batch_put(b, 0, ptr::null(), 3, b"y".as_ptr() as _, 1);
            nil_write(db, b, 0, &mut e.0);
        }
        assert!(e.take().unwrap().contains("NULL pointer"));

        let name = CString::new("rocksdb.estimate-num-keys").unwrap();
        let p = unsafe { nil_property(db, 1, name.as_ptr()) };
        assert!(!p.is_null());
        unsafe { nil_free(p as *mut c_void) };
        let bad = CString::new("nil.nope").unwrap();
        assert!(unsafe { nil_property(db, 1, bad.as_ptr()) }.is_null());
        unsafe {
            nil_flush_wal(db, 1, &mut e.0);
            nil_flush(db, &mut e.0);
            nil_compact_range(db, 1, ptr::null(), 0, ptr::null(), 0, &mut e.0);
        }
        assert_eq!(e.take(), None);
        assert!(unsafe { nil_latest_sequence(db) } > 0);
        unsafe {
            nil_batch_destroy(b);
            nil_close(db, &mut e.0);
        }
        assert_eq!(e.take(), None);

        let db = unsafe { nil_open(kind, dir.as_ptr(), ptrs.as_ptr(), 2, ptr::null(), &mut e.0) };
        assert_eq!(e.take(), None);
        assert_eq!(get(db, ptr::null(), 1, b"k1").as_deref(), Some(&b""[..]));
        assert_eq!(
            get(db, ptr::null(), 0, b"n"),
            None,
            "deleted by the range [a, z)"
        );
        unsafe { nil_close(db, &mut e.0) };
        let _ = std::fs::remove_dir_all(dir.to_str().unwrap());
    }

    #[test]
    fn lsm_round_trip() {
        round_trip(NIL_ENGINE_LSM, "lsm");
    }

    #[test]
    fn btree_round_trip() {
        round_trip(NIL_ENGINE_BTREE, "btree");
    }

    #[test]
    fn open_errors() {
        let mut e = Err::new();
        let dir = tmpdir("errors");
        let names = [CString::new("default").unwrap()];
        let ptrs: Vec<*const c_char> = names.iter().map(|n| n.as_ptr()).collect();
        let db = unsafe { nil_open(7, dir.as_ptr(), ptrs.as_ptr(), 1, ptr::null(), &mut e.0) };
        assert!(db.is_null());
        assert!(e.take().unwrap().contains("unknown engine kind 7"));
        let opts = CString::new("no_such_option=1").unwrap();
        let db = unsafe {
            nil_open(
                NIL_ENGINE_LSM,
                dir.as_ptr(),
                ptrs.as_ptr(),
                1,
                opts.as_ptr(),
                &mut e.0,
            )
        };
        assert!(db.is_null());
        assert!(e.take().unwrap().starts_with("invalid-argument"));
        let db = unsafe {
            nil_open(
                NIL_ENGINE_LSM,
                dir.as_ptr(),
                ptrs.as_ptr(),
                0,
                ptr::null(),
                &mut e.0,
            )
        };
        assert!(db.is_null());
        assert!(e.take().is_some());
        // A NULL errptr is allowed.
        let db = unsafe {
            nil_open(
                9,
                dir.as_ptr(),
                ptrs.as_ptr(),
                1,
                ptr::null(),
                ptr::null_mut(),
            )
        };
        assert!(db.is_null());
        unsafe { nil_write(ptr::null_mut(), ptr::null(), 0, &mut e.0) };
        assert!(e.take().unwrap().contains("NULL"));
    }

    #[test]
    fn panics_become_errors() {
        let mut e = Err::new();
        let v: i32 = guard(&mut e.0, -1, || panic!("boom"));
        assert_eq!(v, -1);
        assert_eq!(e.take().as_deref(), Some("panic: boom"));
        assert_eq!(quiet(3, || -> i32 { panic!("quiet") }), 3);
    }
}
