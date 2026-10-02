use std::cell::RefCell;
use std::fmt::{self, Write};
use std::str;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

#[cfg(feature = "http2")]
use http::header::HeaderValue;
use httpdate::HttpDate;

// "Sun, 06 Nov 1994 08:49:37 GMT".len()
pub(crate) const DATE_VALUE_LENGTH: usize = 29;

#[cfg(feature = "http1")]
pub(crate) fn extend(dst: &mut Vec<u8>) {
    CACHED.with(|cache| {
        dst.extend_from_slice(cache.borrow().buffer());
    })
}

#[cfg(feature = "http1")]
pub(crate) fn update() {
    CACHED.with(|cache| {
        cache.borrow_mut().check();
    })
}

#[cfg(feature = "http2")]
pub(crate) fn update_and_header_value() -> HeaderValue {
    CACHED.with(|cache| {
        let mut cache = cache.borrow_mut();
        cache.check();
        if cache.header_value.is_none() {
            cache.header_value = Some(
                HeaderValue::from_bytes(cache.buffer())
                    .expect("Date format should be valid HeaderValue"),
            );
        }
        cache.header_value.as_ref().unwrap().clone()
    })
}

/// Copy the cached fixed bytes directly into the original field arena. Keep
/// the existing clock/update policy without constructing the ordinary cached
/// HeaderValue or its independently allocated payload.
#[cfg(feature = "http2")]
pub(crate) fn header_value_with_pool(
    pool: &http::header::HeaderFieldAllocationPool,
) -> Result<HeaderValue, http::header::HeaderFieldFillError<std::convert::Infallible>> {
    let bytes = pool.try_fill(DATE_VALUE_LENGTH, |output| {
        CACHED.with(|cache| {
            let mut cache = cache.borrow_mut();
            cache.check();
            output.copy_from_slice(cache.buffer());
        });
        Ok(())
    })?;
    // HttpDate emits only visible ASCII in the fixed IMF-fixdate representation.
    Ok(HeaderValue::from_maybe_shared(bytes).expect("Date format should be valid HeaderValue"))
}

struct CachedDate {
    bytes: [u8; DATE_VALUE_LENGTH],
    pos: usize,
    #[cfg(feature = "http2")]
    header_value: Option<HeaderValue>,
    next_update: SystemTime,
}

thread_local!(static CACHED: RefCell<CachedDate> = RefCell::new(CachedDate::new()));

impl CachedDate {
    fn new() -> Self {
        let mut cache = CachedDate {
            bytes: [0; DATE_VALUE_LENGTH],
            pos: 0,
            #[cfg(feature = "http2")]
            header_value: None,
            next_update: SystemTime::now(),
        };
        cache.update(cache.next_update);
        cache
    }

    fn buffer(&self) -> &[u8] {
        &self.bytes[..]
    }

    fn check(&mut self) {
        let now = SystemTime::now();
        if now > self.next_update {
            self.update(now);
        }
    }

    fn update(&mut self, now: SystemTime) {
        let nanos = now
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .subsec_nanos();

        self.render(now);
        self.next_update = now + Duration::new(1, 0) - Duration::from_nanos(nanos as u64);
    }

    fn render(&mut self, now: SystemTime) {
        self.pos = 0;
        let _ = write!(self, "{}", HttpDate::from(now));
        debug_assert!(self.pos == DATE_VALUE_LENGTH);
        // Only the ordinary HTTP/2 producer materializes its cached payload.
        // HTTP/1 and bounded HTTP/2 both use these exact inline cached bytes.
        #[cfg(feature = "http2")]
        {
            self.header_value = None;
        }
    }
}

impl fmt::Write for CachedDate {
    fn write_str(&mut self, s: &str) -> fmt::Result {
        let len = s.len();
        self.bytes[self.pos..self.pos + len].copy_from_slice(s.as_bytes());
        self.pos += len;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(feature = "nightly")]
    use test::Bencher;

    #[test]
    fn test_date_len() {
        assert_eq!(DATE_VALUE_LENGTH, "Sun, 06 Nov 1994 08:49:37 GMT".len());
    }

    #[cfg(feature = "nightly")]
    #[bench]
    fn bench_date_check(b: &mut Bencher) {
        let mut date = CachedDate::new();
        // cache the first update
        date.check();

        b.iter(|| {
            date.check();
        });
    }

    #[cfg(feature = "nightly")]
    #[bench]
    fn bench_date_render(b: &mut Bencher) {
        let mut date = CachedDate::new();
        let now = SystemTime::now();
        date.render(now);
        b.bytes = date.buffer().len() as u64;

        b.iter(|| {
            date.render(now);
            test::black_box(&date);
        });
    }
}
