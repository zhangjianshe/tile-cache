use std::process::Command;

fn main() {
    let git = std::env::var("GIT_HASH").unwrap_or_else(|_| {
        Command::new("git")
            .args(["rev-parse", "--short", "HEAD"])
            .output()
            .ok()
            .filter(|o| o.status.success())
            .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_owned())
            .unwrap_or_else(|| "unknown".to_owned())
    });
    let built = std::env::var("BUILD_TIME").unwrap_or_else(|_| "unknown".to_owned());
    println!("cargo:rustc-env=TILE_CACHE_GIT_HASH={git}");
    println!("cargo:rustc-env=TILE_CACHE_BUILD_TIME={built}");
    println!("cargo:rerun-if-env-changed=GIT_HASH");
    println!("cargo:rerun-if-env-changed=BUILD_TIME");
}
