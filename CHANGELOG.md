# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.4.2] - 2021-03-21

### Added
- Added meaningful error messages when Snowflake account in the connection screen was not given in the correct format

### Fixed
- Fixed an issue when case sensitive database object names or ones with special characters didn't show up in the object browser

## [0.4.1] - 2021-03-15

### Fixed
- Fixed an issue when Browser Based SSO could not connect to Snowflake account in Oregon US West region

## [0.4.0] - 2021-03-13

### Added
- Add Browser Based SSO authentication
- Add Key Pair authentication
- Add OAuth authentication

## [0.3.0] - 2021-02-21

### Added
- Add OCSP Fail-Open and Fail-Close mode

## [0.2.1] - 2021-02-21

### Fixed
- Fixed an issue when could not connect to Snowflake if `QUOTED_IDENTIFIERS_IGNORE_CASE` parameters is True

## [0.2.0] - 2020-11-09

### Changed
- Make warehouse connection parameter mandatory

### Fixed
- Fixed an issue when user without default warehouse could not connect to snowflake

## [0.1.2] - 2020-11-04

### Changed
- Update documentation

## [0.1.1] - 2020-11-03

### Added
- Add extension icon

## [0.1.0] - 2020-11-03

### Added
- Initial release
