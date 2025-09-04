fn main() -> Result<(), Box<dyn std::error::Error>> {
	tonic_build::configure()
		.build_server(true)
		.out_dir("src/api/")
		.compile(&["proto/moderation.proto"], &["proto"]) ?;
	Ok(())
}