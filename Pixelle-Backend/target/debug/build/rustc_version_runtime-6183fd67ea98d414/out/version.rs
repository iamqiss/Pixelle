
            /// Returns the `rustc` SemVer version and additional metadata
            /// like the git short hash and build date.
            pub fn version_meta() -> VersionMeta {
                VersionMeta {
                    semver: Version {
                        major: 1,
                        minor: 91,
                        patch: 0,
                        pre: vec![semver::Identifier::AlphaNumeric("nightly".to_owned()), ],
                        build: vec![],
                    },
                    host: "x86_64-unknown-linux-gnu".to_owned(),
                    short_version_string: "rustc 1.91.0-nightly (07d246fc6 2025-08-31)".to_owned(),
                    commit_hash: Some("07d246fc6dc227903da2955b38a59e060539a485".to_owned()),
                    commit_date: Some("2025-08-31".to_owned()),
                    build_date: None,
                    channel: Channel::Nightly,
                }
            }
            