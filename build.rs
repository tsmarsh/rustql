fn main() {
    #[cfg(feature = "sqlite3-ffi")]
    {
        println!("cargo:rerun-if-changed=sqlite3/sqlite3.c");
        println!("cargo:rerun-if-changed=sqlite3/sqlite3.h");
        println!("cargo:rerun-if-changed=tests/vdbe_ref.c");

        cc::Build::new()
            .file("tests/vdbe_ref.c")
            .include("sqlite3")
            .define("SQLITE_THREADSAFE", "0")
            .define("SQLITE_OMIT_LOAD_EXTENSION", "1")
            .opt_level(2)
            .warnings(false)
            .compile("sqlite3_ref");
    }
}
