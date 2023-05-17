# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]
### Added
- Support for `dbt==1.4` and `dbt==1.5`

## [0.3.3] - 2023-04-20
### Added
- Support for aliases

### Fixed
- Protect division by zero

## [0.3.2] - 2022-11-18
### Added
- Repository metadata

## [0.3.1] - 2022-11-17
### Fixed
- Model selection by path

## [0.3.0] - 2022-11-17
### Added
- Support for custom run artifacts path. [#33] _Thanks to @Fszta!_
- Support for model selection by path. [#45] _Thanks to @followingell!_ 
- Support for `compute doc` to print results in a markdown table format. [#47] _Thanks to @vvvito!_

## [0.2.3] - 2022-10-14
### Added
- Support for `dbt==1.3`

## [0.2.2] - 2022-07-29
### Added
- Support for `dbt==1.2`

### Changed
- Return coverage report object from compute.

## [0.2.1] - 2022-07-25
### Added
- Support for `dbt==1.1`

## [0.2.0] - 2022-04-01
### Added
- Support for `dbt==1.0.*`

### Removed
- Support for `dbt<1`

## [0.1.10] - 2022-03-03
### Changed
- Simplify `CoverageType` enum

## [0.1.9] - 2022-01-20
### Added
- Support for snapshots.

## [0.1.8] - 2021-12-21
### Fixed
- `manifest.json` parsing.
- Take seeds into account.

## [0.1.7] - 2021-12-09
### Changed
- Beef up the README.

## [0.1.6] - 2021-12-03
### Fixed
- Matching of entities between manifest and catalog

## [0.1.5] - 2021-11-28
### Fixed
- Add forgotten dbt run to README.

## [0.1.4] - 2021-11-23
### Added
- Changelog.

### Fixed
- Do not mix doc and test coverage in README.

## [0.1.3] - 2021-11-23
### Changed
- Use jaffle_shop as example project in README.

## [0.1.2] - 2021-11-23
### Fixed
- Fix `compare` with previously nonexistent entity (table or column).

## [0.1.1] - 2021-11-16
### Added
- README in PyPI.

## [0.1.0] - 2021-11-16
### Added
- First release of `dbt-coverage`.
