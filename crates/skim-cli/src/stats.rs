use std::time::Duration;

/// Latency samples in milliseconds with simple percentiles.
#[derive(Default, Clone)]
pub struct Samples(Vec<f64>);

impl Samples {
    pub fn push(&mut self, d: Duration) {
        self.0.push(d.as_secs_f64() * 1e3);
    }

    pub fn push_ms(&mut self, ms: f64) {
        self.0.push(ms);
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn pct(&self, p: f64) -> Option<f64> {
        if self.0.is_empty() {
            return None;
        }
        let mut v = self.0.clone();
        v.sort_by(|a, b| a.total_cmp(b));
        let i = ((p / 100.0) * (v.len() - 1) as f64).round() as usize;
        Some(v[i])
    }

    pub fn summary(&self) -> String {
        match (self.pct(50.0), self.pct(95.0)) {
            (Some(a), Some(b)) => format!("n={} p50={a:.0}ms p95={b:.0}ms", self.len()),
            _ => "n=0".into(),
        }
    }

    pub fn json(&self) -> serde_json::Value {
        serde_json::json!({ "n": self.len(), "p50_ms": self.pct(50.0), "p95_ms": self.pct(95.0) })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn percentiles() {
        let mut s = Samples::default();
        for i in 1..=100 {
            s.push_ms(i as f64);
        }
        assert_eq!(s.pct(50.0), Some(51.0));
        assert_eq!(s.pct(95.0), Some(95.0));
        assert_eq!(Samples::default().pct(50.0), None);
    }
}
