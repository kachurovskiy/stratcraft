use std::sync::{Arc, Mutex};

#[derive(Clone, Default)]
pub struct OptimizerStatus {
    inner: Arc<Mutex<OptimizerStatusData>>,
}

#[derive(Default)]
struct OptimizerStatusData {
    phase: String,
    total_variations: usize,
    completed_variations: usize,
    failed_variations: usize,
    best_cagr: Option<f64>,
    debug_notes: Option<String>,
}

#[derive(Clone, Debug)]
pub struct OptimizerStatusSnapshot {
    pub phase: String,
    pub total_variations: usize,
    pub completed_variations: usize,
    pub failed_variations: usize,
    pub best_cagr: Option<f64>,
    pub debug_notes: Option<String>,
}

impl OptimizerStatus {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(OptimizerStatusData {
                phase: "Initializing".to_string(),
                ..Default::default()
            })),
        }
    }

    pub fn set_phase<S: Into<String>>(&self, phase: S) {
        if let Ok(mut data) = self.inner.lock() {
            data.phase = phase.into();
        }
    }

    pub fn set_progress(
        &self,
        total_variations: usize,
        completed_variations: usize,
        failed_variations: usize,
        best_cagr: Option<f64>,
    ) {
        if let Ok(mut data) = self.inner.lock() {
            data.total_variations = total_variations;
            data.completed_variations = completed_variations;
            data.failed_variations = failed_variations;
            data.best_cagr = best_cagr;
        }
    }

    pub fn snapshot(&self) -> OptimizerStatusSnapshot {
        if let Ok(data) = self.inner.lock() {
            OptimizerStatusSnapshot {
                phase: data.phase.clone(),
                total_variations: data.total_variations,
                completed_variations: data.completed_variations,
                failed_variations: data.failed_variations,
                best_cagr: data.best_cagr,
                debug_notes: data.debug_notes.clone(),
            }
        } else {
            OptimizerStatusSnapshot {
                phase: "Status unavailable".to_string(),
                total_variations: 0,
                completed_variations: 0,
                failed_variations: 0,
                best_cagr: None,
                debug_notes: None,
            }
        }
    }

    pub fn set_debug_note<S: Into<String>>(&self, note: S) {
        if let Ok(mut data) = self.inner.lock() {
            data.debug_notes = Some(note.into());
        }
    }

    pub fn clear_debug_note(&self) {
        if let Ok(mut data) = self.inner.lock() {
            data.debug_notes = None;
        }
    }
}
