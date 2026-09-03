use std::{
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, SystemTime},
};

use tokio::time;

pub struct Clock {
    secs: AtomicU64,
}

fn unix_now_secs() -> u64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

impl Clock {
    pub fn now(&self) -> u64 {
        self.secs.load(Ordering::Relaxed)
    }

    fn set(&self, secs: u64) {
        self.secs.store(secs, Ordering::Relaxed);
    }

    #[cfg(test)]
    pub fn mock(start_secs: u64) -> SharedClock {
        Arc::new(Clock {
            secs: AtomicU64::new(start_secs),
        })
    }
    #[cfg(test)]
    pub fn advance(&self, secs: u64) {
        self.secs.fetch_add(secs, Ordering::Relaxed);
    }
}

pub type SharedClock = Arc<Clock>;

pub fn spawn_clock() -> SharedClock {
    let clock = Arc::new(Clock {
        secs: AtomicU64::new(unix_now_secs()),
    });

    let bg = clock.clone();
    tokio::spawn(async move {
        let mut interval = time::interval(Duration::from_secs(1));
        loop {
            interval.tick().await;
            bg.set(unix_now_secs());
        }
    });

    clock
}
