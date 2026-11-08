use vortex_rs::lob_shm::VORTEX_SHM_VERSION;
use vortex_rs::types::APPROVED_SPECS;

fn main() {
    println!(
        "[VORTEX-L3 STEP 1 GATE] SHM v{} | Pairs: {}",
        VORTEX_SHM_VERSION,
        APPROVED_SPECS.len()
    );
}
