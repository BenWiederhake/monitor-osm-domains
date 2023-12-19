# monitor-osm-domains

This project monitors all web domains stored in OpenStreetMap data in a certain region. The idea is to query these domains every month or so, and collect data over a long period of time. If a server is consistently down for many months, this is strong evidence that a server is dead, and will likely never come back. This evidence can then be used to update the OSM database, possibly even in an automated fashion.

Sure, one could just click a link, see an error, and call it "stale". But that's not good enough, since any error could be temporary. If one can show that an error persists over several months or even years, that feels like sufficient "proof".

## Table of Contents

- [Components](#components)
- [Install](#install)
- [TODOs](#todos)
- [NOTDOs](#notdos)
- [Contribute](#contribute)

## Components

`extract` runs on a `.pbf` extract of OSM data, and produces a `*.monosmdom.json` file containing information about which URLs shall be monitored, and which nodes/ways/relations are affected by that. This component is meant to be run very rarely, and manually.

`monosmdom_server` runs a Django server that automates most of the rest:
- Reading a given `*.monosmdom.json` file, and updating the internal list of URLs-to-be-monitored.
- Crawling all URLs, recording the results, and making sure the generated traffic is very low.
- Providing a web UI with the results.

There are no plans yet for automated edits to OSM. I am aware of the [AECoC](https://wiki.openstreetmap.org/wiki/Automated_Edits_code_of_conduct) and pledge to follow it, when considering automated edits.

## Install

I run this on private servers. I don't expect anyone else to also run this, and if you do, please coordinate with me so that we can avoid overlapping areas (in OSM data) or domains (i.e. unnecessary traffic).

`extract` is a very small C++ program that only depends on osmium, cmake, and either make or ninja. On Debian you need to install `libosmium2-dev`, that pulls in all the other dependencies.

`monosmdom_server` is a complex Django server, and depends on some database (I prefer PostgreSQL). The PyPI dependencies are given in [`requirements.txt`](monosmdom_server/requirements.txt).

The usage instructions of each component can be found in the respective subdirectory.

## TODOs

* Create a nice (leaflet?) frontend to show all results
* Continue running and monitoring
* Find a good handful of real results
* If successful, consider automating removals.

## NOTDOs

Here are some things this project will definitely not support:
* Anything that requires high-resolution sampling. I want to cause minimal impact, on the order of one page request per domain per week at most.
* Anything that has the potential to remove "good" data. I want to avoid false positives.

These are highly unlikely, but I might look into it if it seems like a good idea:
* Try to fix up bad URLs. (E.g. if a value does not start with the string `http` or the domain is something like `localhost`, the tool currently will assume that the value is not a URL. Note that the filtering is distributed over the components.)
* Integration with "trust" databases, to detect "malicious" websites
* Opening the service to a wide audience of laypeople. (Reason: Either there are so few results that I can easily apply them myself, or the results are so many that it is justified to automatically apply them.)

## Contribute

Feel free to dive in! [Open an issue](https://github.com/BenWiederhake/monitor-osm-domains/issues/new) or submit PRs.
