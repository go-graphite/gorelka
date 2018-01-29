gorelka
-----

This project provides a relay that can accept metrics in various formats (initially Graphite Line protocol) and send them through various ways.

Status
------

Early Proof of Concept. Compiles, but not tested extensively

Features
--------

General:
- [ ] Don't store metrics forever in queues in case destination is unavailable
- [ ] Offload queue overflows to disk
- [ ] Internal stats
- [ ] Extended stats

Config:
- [ ] Override key for Transport distribution

Calculator:
- [ ] Calculate real metric frequency
- [ ] Detect semi-frequent metrics

Input:
- [X] TCP
- [X] UDP
- [X] Unix Socket
- [ ] TLS
- [ ] Configurable encoding

Input Encoders:
- [X] Graphite Line Protocol
- [ ] Graphite Line Protocol with tags
- [ ] Metrics 2.0
- [ ] InfluxDB Line Protocol

Output Encoders:
- [X] Graphite Line Protocol
- [X] JSON
- [X] Protobuf
- [ ] [kafkamdm](https://github.com/raintank/schema)

Output:
- [X] Kafka
- [X] TCP
- [X] UDP
- [ ] Unix Socket

Routing:
- [X] Regexp matching (Re2-based)
- [X] Rewrites
- [X] Prefix Matching
- [X] Blackhole sender
- [X] Log on receive
- [ ] PCRE Regexp Matching
- [ ] Separate tool to show where metric will lend

LoadBalancing:
- [X] fnv1a
- [X] [jump hash fnv1a](https://arxiv.org/abs/1406.2294)
- [ ] round robin with sticking
- [ ] graphite consistent hash

Documentation:
- [ ] At least some docs
- [ ] Design documentation
- [ ] Extended docs

Known issues
------------

- Some internal queues (if you can call it queues) have no limit so malformed or unthrottled input might lead to OOM issues
- If backend go down, first point in queue will be lost
- Performance is far from optimal.
- Config format is far from perfect (readability, easy of modification, easy of generation)
- Unstable config format

Acknowledgement
---------------

This program was originally developed for Booking.com. With approval from Booking.com, the code was generalised and published as Open Source on GitHub, for which the author would like to express his gratitude.

License
-------

This code is licensed under the Apache2 license.
