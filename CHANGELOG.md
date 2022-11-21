# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).

## [Unreleased]

### Changed

- Top-level xray structure now has a `:tables` key. This refers to what was the
  top-level structure, a map of table names to table metadata
- Top-level xray structure has a `:table-order` key, a vector of table names in
  dependency order. Order is not guaranteed for circular dependencies
- Table-level xray has a `:column-order` key, and generators now use it
- xray structures no longer use ordered maps

### Fixed

- Fixed #5 circular FK support
- Fixed issue with tables being ignored if they were disconnected
