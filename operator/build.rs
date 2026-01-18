use kube::CustomResourceExt;
use std::fs;
use strim_types::*;

fn main() {
    let _ = fs::create_dir("../crds");
    fs::write(
        "../crds/strim.beebs.dev_strim_crd.yaml",
        serde_yaml::to_string(&Strim::crd()).unwrap(),
    )
    .unwrap();
}
