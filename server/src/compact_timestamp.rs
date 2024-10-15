use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// A compact timestamp representation for microseconds using u32 to reduce overhead. Provides ~71 mins of accuracy

pub fn get_current_time_us_u32() -> u32 {
    from_system_time(SystemTime::now())
}

/// Returns the absolute duration since other timestamp
pub fn duration_since_us(timestamp_a_us: u32, timestamp_b_us: u32) -> u32 {
    // use i64 to avoid overflow
    let normal_diff = if timestamp_a_us > timestamp_b_us {
        timestamp_a_us - timestamp_b_us
    } else {
        timestamp_b_us - timestamp_a_us
    };
    let wraparound_diff = u32::MAX.wrapping_sub(normal_diff).wrapping_add(1);
    normal_diff.min(wraparound_diff)
}

pub fn from_system_time(from: SystemTime) -> u32 {
    from.duration_since(UNIX_EPOCH).unwrap().as_micros() as u32
}

pub fn to_system_time(timestamp_us: u32) -> SystemTime {
    let now = SystemTime::now();
    let now_us = now.duration_since(UNIX_EPOCH).unwrap().as_micros() as u32;
    let diff = duration_since_us(now_us, timestamp_us);
    now - Duration::from_micros(diff as u64)
}

#[cfg(test)]
mod test {
    use std::thread::sleep;

    use super::*;

    #[test]
    fn test_duration_since_us() {
        // test overflow behavior
        for i in -100i32..100 {
            for j in -100i32..100 {
                assert_eq!(duration_since_us(i as u32, j as u32), i.abs_diff(j));
            }
        }

        // Test specific cases
        assert_eq!(duration_since_us(100, 50), 50);
        assert_eq!(duration_since_us(50, 100), 50);
        assert_eq!(duration_since_us(0, u32::MAX), 1);
        assert_eq!(duration_since_us(u32::MAX, 0), 1);
    }

    #[test]
    fn test_get_current_time_us_u32() {
        let sleep_time_us = 10_000u32;
        let t1 = get_current_time_us_u32();
        sleep(Duration::from_micros(sleep_time_us as u64));
        let t2 = get_current_time_us_u32();
        assert!(t2 > t1);
        assert!(duration_since_us(t1, t2) > sleep_time_us);
    }

    #[test]
    fn test_to_system_time() {
        let now = SystemTime::now();
        let timestamp = from_system_time(now);
        let reconstructed = to_system_time(timestamp);
        let diff = now
            .duration_since(reconstructed)
            .unwrap_or_else(|e| e.duration());
        assert!(diff < Duration::from_micros(1));
    }
}
