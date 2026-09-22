use std::fs;

#[derive(Debug, Default, PartialEq, Eq)]
pub struct ProcessMetrics {
    pub memory_bytes: u64,
    pub thread_count: u64,
}

pub fn current_process_metrics() -> ProcessMetrics {
    fs::read_to_string("/proc/self/status")
        .map(|status| parse_proc_status(&status))
        .unwrap_or_default()
}

fn parse_proc_status(status: &str) -> ProcessMetrics {
    let mut metrics = ProcessMetrics::default();
    for line in status.lines() {
        if let Some(value) = line.strip_prefix("VmRSS:") {
            metrics.memory_bytes = first_number(value).saturating_mul(1024);
        } else if let Some(value) = line.strip_prefix("Threads:") {
            metrics.thread_count = first_number(value);
        }
    }
    metrics
}

fn first_number(value: &str) -> u64 {
    value
        .split_whitespace()
        .next()
        .and_then(|value| value.parse().ok())
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_linux_process_status() {
        let metrics = parse_proc_status("Name:\ttile-cache\nVmRSS:\t  1234 kB\nThreads:\t7\n");
        assert_eq!(
            metrics,
            ProcessMetrics {
                memory_bytes: 1_263_616,
                thread_count: 7,
            }
        );
    }
}
