# `merge_raft`

[![Build Status](https://github.com/WhatsApp/merge_raft/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/WhatsApp/merge_raft/actions)

See the [CONTRIBUTING](CONTRIBUTING.md) file for how to help out.

## Installation

TBD

## Usage

```
rebar3 shell
```
Node 1:
```
merge_raft_kv:start(kv).
net_kernel:start('1@localhost', #{name_domain => shortnames}).
merge_raft_kv:async_put(kv, key, value).
```
Node 2:
```
merge_raft_kv:start(kv).
net_kernel:start('2@localhost', #{name_domain => shortnames}).
net_kernel:connect_node('1@localhost').
merge_raft_kv:sync_get(kv, key).
```

## Testing

rebar3 ct

## License

`merge_raft` is Apache License Version 2.0 licensed, as found in the [LICENSE](LICENSE.md) file.
