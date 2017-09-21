Graphite To Multiple Transports
-----

This project provides a relay that can accept metrics in various formats (initially Graphite Line protocol) and send them through various ways.

Status
------

Early Proof of Concept. Compiles, but not tested extensively

Features
--------

Input:
- [X] Graphite Line Protocol
- [ ] Metrics 2.0

Output:
- [ ] [kafkamdm](https://github.com/raintank/schema)
- [X] Kafka (Protobuf Graphite protocol over Kafka)
- [ ] Graphite Line Protocol

LoadBalancing:
- [X] [jump hash](https://arxiv.org/abs/1406.2294)
- [ ] round robin with sticking
- [X] fnv1a
- [ ] graphite consistent hash

Known issues
------------

- Some internal queues (if you can call it queues) have no limit so malformed or unthrottled input might lead to OOM issues
- Performance is untested, at least RegExps matching can be very slow
- Rules readability far from perfect
- Unstable config format
- No Documentation

Acknowledgement
---------------

This program was originally developed for Booking.com. With approval from Booking.com, the code was generalised and published as Open Source on GitHub, for which the author would like to express his gratitude.

License
-------

This code is licensed under the Apache2 license.
