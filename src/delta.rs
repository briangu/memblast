use crate::net::Update;

/// Compute delta updates between a previous and current buffer.
///
/// `prev` is mutated to contain the contents of `curr` after the call.
pub fn compute_deltas(prev: &mut [f64], curr: &[f64], shape: &[usize]) -> Vec<Update> {
    assert_eq!(prev.len(), curr.len());
    let shape_u32: Vec<u32> = shape.iter().map(|&d| d as u32).collect();
    let mut updates = Vec::new();
    let mut start: Option<usize> = None;
    let mut vals: Vec<f64> = Vec::new();

    for i in 0..curr.len() {
        if prev[i] != curr[i] {
            if start.is_none() {
                start = Some(i);
            }
            prev[i] = curr[i];
            vals.push(curr[i]);
        } else if let Some(s) = start.take() {
            updates.push(Update {
                shape: shape_u32.clone(),
                start: s as u32,
                vals: std::mem::take(&mut vals),
            });
        }
    }

    if let Some(s) = start {
        updates.push(Update {
            shape: shape_u32,
            start: s as u32,
            vals,
        });
    }

    updates
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn single_change() {
        let mut prev = vec![0.0, 0.0, 0.0];
        let curr = vec![1.0, 0.0, 0.0];
        let deltas = compute_deltas(&mut prev, &curr, &[3]);
        assert_eq!(deltas.len(), 1);
        assert_eq!(deltas[0].start, 0);
        assert_eq!(deltas[0].vals, vec![1.0]);
    }

    #[test]
    fn multiple_ranges() {
        let mut prev = vec![0.0, 0.0, 0.0, 0.0];
        let curr = vec![1.0, 0.0, 2.0, 0.0];
        let deltas = compute_deltas(&mut prev, &curr, &[4]);
        assert_eq!(deltas.len(), 2);
        assert_eq!(deltas[0].start, 0);
        assert_eq!(deltas[0].vals, vec![1.0]);
        assert_eq!(deltas[1].start, 2);
        assert_eq!(deltas[1].vals, vec![2.0]);
    }

    #[test]
    fn contiguous_range() {
        let mut prev = vec![0.0, 0.0, 0.0];
        let curr = vec![1.0, 2.0, 0.0];
        let deltas = compute_deltas(&mut prev, &curr, &[3]);
        assert_eq!(deltas.len(), 1);
        assert_eq!(deltas[0].start, 0);
        assert_eq!(deltas[0].vals, vec![1.0, 2.0]);
    }
}

