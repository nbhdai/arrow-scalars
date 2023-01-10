#[cfg(feature = "serde")]
fn main() -> Result<(), Box<dyn std::error::Error>> {
    //std::env::set_var("OUT_DIR", "src");
    tonic_build::configure()
        .build_server(false)
        .out_dir("src/") // you can change the generated code's location
        .type_attribute(".", "#[derive(serde::Serialize, serde::Deserialize)]")
        .compile(
            &["./arrow_scalars.proto"],
            &["./"], // specify the root location to search proto dependencies
        )?;

    Ok(())
}
#[cfg(not(feature = "serde"))]
fn main() -> Result<(), Box<dyn std::error::Error>> {
    //std::env::set_var("OUT_DIR", "src");
    tonic_build::configure()
        .build_server(false)
        .out_dir("src/") // you can change the generated code's location
        .compile(
            &["./arrow_scalars.proto"],
            &["./"], // specify the root location to search proto dependencies
        )?;

    Ok(())
}