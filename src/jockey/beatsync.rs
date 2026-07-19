use std::time::Instant;

#[derive(Debug, Clone)]
pub struct BeatSync {
    pub first: Instant,
    pub last: Instant,
    pub count: u32,
    external_bpm: Option<f32>,
    external_beat: f32,
    external_updated_at: Instant,
}

impl BeatSync {
    pub fn new() -> Self {
        let now = Instant::now();

        Self {
            first: now,
            last: now,
            count: 0,
            external_bpm: None,
            external_beat: 0.0,
            external_updated_at: now,
        }
    }

    pub fn _reset(&mut self) {
        *self = Self::new()
    }

    pub fn trigger(&mut self) {
        self.external_bpm = None;
        let now = Instant::now();
        if now.duration_since(self.last).as_secs_f32() > 2.0 {
            self.first = now;
            self.count = 0;
        }

        self.last = now;
        self.count += 1;
    }

    /// Average number of beats per seconds
    pub fn rate(&self) -> f32 {
        if let Some(bpm) = self.external_bpm {
            return bpm / 60.0;
        }

        let deltas = self.count.saturating_sub(1);
        if deltas > 1 {
            deltas as f32 / self.last.duration_since(self.first).as_secs_f32()
        } else {
            1.0
        }
    }

    /// Average number of beats per minute
    pub fn bpm(&self) -> f32 {
        60.0 * self.rate()
    }

    /// Interpolated number of beats since first trigger
    pub fn beat(&self) -> f32 {
        if self.external_bpm.is_some() {
            return self.external_beat
                + self.rate() * self.external_updated_at.elapsed().as_secs_f32();
        }

        self.rate() * self.first.elapsed().as_secs_f32()
    }

    pub fn set_bpm(&mut self, bpm: f32) {
        if !bpm.is_finite() || bpm <= 0.0 {
            return;
        }

        self.external_beat = self.beat();
        self.external_updated_at = Instant::now();
        self.external_bpm = Some(bpm);
    }
}

#[cfg(test)]
mod test {
    use std::{ops::Sub, time::Duration};

    use super::*;

    #[test]
    fn three_beats() {
        let mut sync = BeatSync::new();
        sync.trigger();

        std::thread::sleep(Duration::from_millis(330));
        sync.trigger();

        std::thread::sleep(Duration::from_millis(330));
        sync.trigger();

        assert!(sync.beat().sub(2.0).abs() < 0.2, "{}", sync.beat());
        assert!(sync.rate().sub(3.0).abs() < 0.2, "{}", sync.rate());
    }
}
