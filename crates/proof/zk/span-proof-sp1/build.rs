//! Build script for embedding the SP1 program ELF.

fn main() {
    if std::env::var_os("BASE_ZK_SPAN_SP1_BUILD_WITH_DOCKER").is_some() {
        let args = sp1_build::BuildArgs {
            docker: true,
            warning_level: sp1_build::WarningLevel::Minimal,
            ..sp1_build::BuildArgs::default()
        };
        sp1_build::build_program_with_args("program", args);
    } else {
        sp1_build::build_program("program");
    }
}
