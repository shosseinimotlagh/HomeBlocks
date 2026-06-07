# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [6.0.0]

### Changed

- Migrated the I/O and control paths off Folly futures onto the C++23 stackless-coroutine stack
  (HomeStore v8 / iomgr v13 / sisl v14.6); `async_*` operations are now `co_await`-able `sisl::async::task`s.
- Redesigned the public API down to a single installed header, `<homeblks/home_blocks.hpp>`: `init_homeblocks()`
  takes a `home_blocks_config` value (devices, threads, `on_svc_id` hook) and returns
  `result<std::shared_ptr<home_blocks>>`; volume I/O is the byte-addressed free functions `async_read` /
  `async_write` / `async_unmap(volume_handle, addr, sg_list)` returning `async_result<size_t>`; error handling
  is unified on `std::expected<T, std::error_condition>` (`volume_error` for HomeBlocks-specific failures,
  `std::errc` otherwise).

### Removed

- Folly dependency; the `HomeBlocksApplication` consumer interface (replaced by `home_blocks_config`); and the
  heap-allocated per-I/O request object from the public surface.

### Added

- Created repository

[Unreleased]: https://github.com/eBay/HomeBlocks/compare/...HEAD
