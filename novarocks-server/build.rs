fn main() {
    let compat_enabled = std::env::var_os("CARGO_FEATURE_COMPAT").is_some();
    let target_os = std::env::var("CARGO_CFG_TARGET_OS").unwrap_or_default();

    if compat_enabled && target_os == "macos" {
        // The compat core links brpc without optional gperftools support. These
        // final-link allowances must be emitted by each downstream executable
        // package because rustc link arguments do not cross package boundaries.
        println!("cargo:rustc-link-arg=-Wl,-U,_MallocExtension_ReleaseFreeMemory");
        println!("cargo:rustc-link-arg=-Wl,-U,_ProfilerStart");
        println!("cargo:rustc-link-arg=-Wl,-U,_ProfilerStop");
        println!("cargo:rustc-link-arg=-Wl,-U,__Z13GetStackTracePPvii");
    }
}
