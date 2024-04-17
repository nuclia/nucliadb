use std::collections::BTreeMap;

use nucliadb_core::NodeResult;
use nucliadb_vectors::data_point::{create, DataPointPin, Elem, NoDLog, SearchParams, Similarity};
use rand::{rngs::SmallRng, Rng, SeedableRng};
use tempfile::tempdir;

const DIMENSION: usize = 1536;

fn random_vector(rng: &mut impl Rng) -> Vec<f32> {
    let v: Vec<f32> = (0..DIMENSION).map(|_| rng.gen_range(-1.0..1.0)).collect();
    normalize(v)
}

fn normalize(v: Vec<f32>) -> Vec<f32> {
    let mut modulus = 0.0;
    for w in &v {
        modulus += w * w;
    }
    modulus = modulus.powf(0.5);

    v.into_iter().map(|w| w / modulus).collect()
}

fn random_nearby_vector(rng: &mut impl Rng, close_to: &[f32], distance: f32) -> Vec<f32> {
    // Create a random vector of low modulus
    let fuzz = random_vector(rng);
    let v = close_to.iter().zip(fuzz.iter()).map(|(v, fuzz)| v + fuzz * distance).collect();
    normalize(v)
}

fn random_elem(rng: &mut impl Rng) -> (String, Vec<f32>) {
    let key = random_key(rng);
    let v = random_vector(rng);

    (key, v)
}

fn random_key(rng: &mut impl Rng) -> String {
    format!("{:032x?}", rng.gen::<u128>())
}

fn similarity(x: &[f32], y: &[f32]) -> f32 {
    x.iter().zip(y).map(|(xx, yy)| xx * yy).sum()
}

fn test_recall_random_data() -> NodeResult<()> {
    let mut rng = SmallRng::seed_from_u64(1234567890);
    let elems: BTreeMap<_, _> = (0..500).map(|_| random_elem(&mut rng)).collect();

    // Create a data point
    let temp_dir = tempdir()?;
    let elems_vec: Vec<Elem> =
        elems.iter().map(|(k, v)| Elem::new(k.clone(), v.clone(), Default::default(), None)).collect();

    let pin = DataPointPin::create_pin(temp_dir.path())?;
    let dp = create(&pin, elems_vec, None, Similarity::Dot)?;

    // Search a few times
    let correct = (0..100)
        .map(|_| {
            // Search near-ish an existing datapoint (simulates that the query is related to the data)
            let base_v = elems.values().nth(rng.gen_range(0..elems.len())).unwrap();
            let query = random_nearby_vector(&mut rng, base_v, 0.5);

            let mut similarities: Vec<_> = elems.iter().map(|(k, v)| (k, similarity(v, &query))).collect();
            similarities.sort_unstable_by(|a, b| a.1.total_cmp(&b.1).reverse());

            let results: Vec<_> = dp
                .search(
                    &NoDLog,
                    &query,
                    &Default::default(),
                    false,
                    5,
                    SearchParams {
                        similarity: Similarity::Dot,
                        min_score: -1.0,
                        dimension: DIMENSION,
                    },
                )
                .collect();

            let search: Vec<_> = results.iter().map(|r| String::from_utf8(r.id().to_vec()).unwrap()).collect();
            let brute_force: Vec<_> = similarities.iter().take(5).map(|r| r.0.clone()).collect();
            search == brute_force
        })
        .filter(|x| *x)
        .count();

    let recall = correct as f32 / 100.0;
    // Expected ~0.89
    println!("Assessed recall random data = {recall}");

    Ok(())
}

fn test_recall_clustered_data() -> NodeResult<()> {
    let mut rng = SmallRng::seed_from_u64(1234567890);
    let mut elems = BTreeMap::new();

    // Create some clusters
    let mut center = random_vector(&mut rng);
    for _ in 0..2 {
        // 80 tightly clustered vectors, ideally more than Mmax0
        for _ in 0..80 {
            elems.insert(random_key(&mut rng), random_nearby_vector(&mut rng, &center, 0.01));
        }
        // 80 tightly clustered vectors
        for _ in 0..80 {
            elems.insert(random_key(&mut rng), random_nearby_vector(&mut rng, &center, 0.03));
        }
        // Next cluster is nearby
        center = random_nearby_vector(&mut rng, &center, 0.1);
    }

    // Create a data point
    let temp_dir = tempdir()?;
    let pin = DataPointPin::create_pin(temp_dir.path())?;
    let dp = create(
        &pin,
        elems.iter().map(|(k, v)| Elem::new(k.clone(), v.clone(), Default::default(), None)).collect(),
        None,
        Similarity::Dot,
    )?;

    // Search a few times
    let correct = (0..100)
        .map(|_| {
            // Search near an existing datapoint (simulates that the query is related to the data)
            let base_v = elems.values().nth(rng.gen_range(0..elems.len())).unwrap();
            let query = random_nearby_vector(&mut rng, base_v, 0.05);

            let mut similarities: Vec<_> = elems.iter().map(|(k, v)| (k, similarity(v, &query))).collect();
            similarities.sort_unstable_by(|a, b| a.1.total_cmp(&b.1).reverse());

            let results: Vec<_> = dp
                .search(
                    &NoDLog,
                    &query,
                    &Default::default(),
                    false,
                    5,
                    SearchParams {
                        similarity: Similarity::Dot,
                        min_score: 0.0,
                        dimension: DIMENSION,
                    },
                )
                .collect();

            let search: Vec<_> = results.iter().map(|r| String::from_utf8(r.id().to_vec()).unwrap()).collect();
            let brute_force: Vec<_> = similarities.iter().take(5).map(|r| r.0.clone()).collect();
            search == brute_force
        })
        .filter(|x| *x)
        .count();

    let recall = correct as f32 / 100.0;
    // Expected ~0.86
    println!("Assessed recall clustered data = {recall}");

    Ok(())
}

fn main() -> NodeResult<()> {
    test_recall_random_data()?;
    test_recall_clustered_data()?;
    Ok(())
}
