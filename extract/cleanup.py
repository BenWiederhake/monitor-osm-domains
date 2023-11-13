#!/usr/bin/env python3

from collections import Counter, defaultdict
import json
import random
import re
import subprocess
import sys
import urllib.parse


EASY_REPAIRS = [
    (re.compile(r"^http(s?):///(?![\\/])"), r"http\1://"),
    (re.compile(r"^http(s?):/(?![\\/])"), r"http\1://"),
    (re.compile(r"^http(s?):\\\\?(?![\\/])"), r"http\1://"),
    (re.compile(r"^http(s?):(?![\\/])"), r"http\1://"),
    (re.compile(r"^http(s?)://\\\\(?![\\/])"), r"http\1://"),
    (re.compile(r"^http(s?)//:(?![\\/:])"), r"http\1://"),
    (re.compile(r"^http(s?)[.;]?//(?![\\/])"), r"http\1://"),
    # Must be exactly in this order: First strip trailing hash, then strip "trailing" question mark:
    (re.compile(r"^([^#]+)#$"), r"\1"),
    (re.compile(r"^([^#?]+)\?(#[^#?]+)?$"), r"\1\2"),
    (re.compile(r"^http://https://(?![\\/])"), r"https://"),
]
RE_MULTI_VALUE = re.compile("[,;] ?(?=http)")
NORMAL_URL_CHARS = "abcdefghijklmnopqrstuvwxyzäöüßABCDEFGHIJKLMNOPQRSTUVWXYZÄÖÜ0123456789:/.?=&!#%()*+,-;@[]_{|}~$ "
DISASTROUS_CHARACTERS_BYTES = [
    b'\xc2\xa0',
    b'\xc2\xba',
    b'\xcc\x88',
    b'\xe2\x80\x8b',
    b'\xe2\x80\x8e',
    b'\xe2\x80\x90',
]
RE_BARE_IP = re.compile(r"^[0-9.:]+$")
# Note that lookup results can easily be thousands of times larger.
# However, things should get considerably faster by batching queries together.

DISASTROUS_CHARACTERS = [ch.decode() for ch in DISASTROUS_CHARACTERS_BYTES]


class DisasterUrl:
    def __init__(self):
        self.reasons = set()
        self.occs = []

    def extend(self, reason, occs):
        self.reasons.add(reason)
        self.occs.extend(occs)

    def __dict__(self):
        return dict(d="qwe")


class DisasterEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, DisasterUrl):
            return dict(reasons=list(sorted(obj.reasons)), occs=obj.occs)
        return json.JSONEncoder.default(self, obj)


def repair_easy_stuff(url):
    for regex, replacement in EASY_REPAIRS:
        url = regex.sub(replacement, url)
    return url


def selftest():
    fails = 0
    tests = 0
    for given, expected in [
        (r"https://example.com/hello/", None),
        (r"https://example.com/hello", None),
        (r"https://example.com/", None),
        (r"https://example.com", None),
        (r"https:\\example.com/hello/", "https://example.com/hello/"),
        (r"https:\\example.com/hello", "https://example.com/hello"),
        (r"https:\\example.com/", "https://example.com/"),
        (r"https:\\example.com", "https://example.com"),
        (r"https:\example.com/hello/", "https://example.com/hello/"),
        (r"https:\example.com/hello", "https://example.com/hello"),
        (r"https:\example.com/", "https://example.com/"),
        (r"https:\example.com", "https://example.com"),
        (r"https:/example.com/hello/", "https://example.com/hello/"),
        (r"https:/example.com/hello", "https://example.com/hello"),
        (r"https:/example.com/", "https://example.com/"),
        (r"https:/example.com", "https://example.com"),
        (r"http://example.com/hello/", None),
        (r"http://example.com/hello", None),
        (r"http://example.com/", None),
        (r"http://example.com", None),
        (r"http:\\example.com/hello/", "http://example.com/hello/"),
        (r"http:\\example.com/hello", "http://example.com/hello"),
        (r"http:\\example.com/", "http://example.com/"),
        (r"http:\\example.com", "http://example.com"),
        (r"http:\example.com/hello/", "http://example.com/hello/"),
        (r"http:\example.com/hello", "http://example.com/hello"),
        (r"http:\example.com/", "http://example.com/"),
        (r"http:\example.com", "http://example.com"),
        (r"http:/example.com/hello/", "http://example.com/hello/"),
        (r"http:/example.com/hello", "http://example.com/hello"),
        (r"http:/example.com/", "http://example.com/"),
        (r"http:/example.com", "http://example.com"),
        (r"https://example.com/foo?w=?", None),
        (r"https://example.com/foo?", "https://example.com/foo"),
        (r"https://example.com/foo/?", "https://example.com/foo/"),
        (r"https://example.com/foo/?q=1", None),
        (r"https://example.com/foo/?q=1?", None),
        (r"https://example.com/#", "https://example.com/"),
        (r"https://example.com/##", None),
        (r"https://example.com/?#", "https://example.com/"),
        (r"https://example.com/#?", None),
        (r"https://example.com/?#foo", "https://example.com/#foo"),
        (r"https://example.com/#?foo", None),
        (r"https://example.com/?q#foo", None),
        # This is so stupid.
        (r"http://\\foo.com/bar", r"http://foo.com/bar"),
        (r"https://\\foo.com/bar", r"https://foo.com/bar"),
        (r"http://https://example.de", r"https://example.de"),
        (r"http;//example.de", r"http://example.de"),
        (r"https;//example.de", r"https://example.de"),
        (r"http//example.de", r"http://example.de"),
        (r"https//example.de", r"https://example.de"),
        (r"http.//example.de", r"http://example.de"),
        (r"https.//example.de", r"https://example.de"),
        (r"http//:example.de", r"http://example.de"),
        (r"https//:example.de", r"https://example.de"),
    ]:
        actual = repair_easy_stuff(given)
        tests += 1
        if expected is None:
            expected = given
        else:
            assert given != expected, given
        if actual == expected:
            continue
        print(f"FAIL: {given=} {expected=} {actual=}")
        fails += 1
    print(f"Selftest completed: Ran {tests} tests, encountered {fails} failure(s).", file=sys.stderr)
    if fails:
        exit(100)


def simplify_regex(old_findings, disasters):
    by_regexed_url = defaultdict(list)  # Regexed URL string to list of occurrence-objects
    for entry in old_findings:
        orig_url = entry["url"]
        for occ in entry["occ"]:
            occ["orig_url"] = orig_url
        is_disaster = False
        for ch in DISASTROUS_CHARACTERS:
            if ch in orig_url:
                disaster = disasters[orig_url]
                disaster.extend(f"Weird character {ch.encode()}", entry["occ"])
                is_disaster = True
                break
        if is_disaster:
            # Don't break it up into constituent URLs; this tag value is cursed anyway.
            continue
        for partial_url in RE_MULTI_VALUE.split(orig_url):
            parse_url = repair_easy_stuff(partial_url)
            by_regexed_url[parse_url].extend(entry["occ"])
    return by_regexed_url


def simplified_url_or_disaster_reason(parse_url):
    if '\\' in parse_url:
        return None, r"weird character b'\\'"
    if ' ' in parse_url:
        return None, r"weird character b' '"
    parts = urllib.parse.urlsplit(parse_url)
    # scheme, netloc, path, query, fragment
    if parts.scheme not in ["http", "https"]:
        return None, f"unusual scheme {parts.scheme}"
    if parts.username is not None or parts.password is not None:
        return None, f"contains login information, not crawling"
    # Accessing the 'port' property raises an exception in invalid URLs -.-
    try:
        port_number = parts.port
    except ValueError:
        return None, f"port is not a valid integer"
    port_string = ""
    if parts.port is not None:
        # Using ports is highly questionable, and I would love to mark it disastrous.
        # However, a disturbingly high number of servers redirect e.g.
        # from "https://example.com/" to "https://example.com:443/". So we have to permit it.
        if parts.scheme == "http" and parts.port == 80:
            port_string = ":80"
        elif parts.scheme == "https" and parts.port == 443:
            port_string = ":443"
        else:
            return None, f"refusing to use forced port {parts.port}"
    hostname = parts.hostname
    if hostname is not None:
        # Despite the documentation, the hostname is NOT always lowercase. Example:
        # >>> urllib.parse.urlsplit("https://www.geb%C3%A4udereinigung.de/").netloc
        # 'www.geb%C3%A4udereinigung.de'
        hostname = hostname.lower()
    netloc = parts.netloc.lower()
    if netloc and netloc[-1] == ":":
        # Permit a single trailing colon, because some servers redirect like that (ugh).
        netloc = netloc[: -1]
    if hostname is None or netloc != hostname + port_string:
        return None, f"disagreeing {netloc=} and {hostname=} {port_string=}"
    if ".." in hostname:
        return None, r"double-dot in hostname"
    if "http" in hostname:
        return None, r"suspicious 'http' in hostname"
    hostname = hostname.strip(".")
    # FIXME: Shouldn't check for bare IPs here, as these are not necessarily disastrous.
    if RE_BARE_IP.match(hostname):
        return None, f"{hostname=} looks like a bare IP"
    unsplit_url = urllib.parse.urlunsplit(parts)
    if unsplit_url != parse_url:
        # FIXME: Should report this somewhere else.
        print("WARNING: Url splitting probably lost some data!")
        print(f"    Before: >>{parse_url}<<")
        print(f"    After : >>{unsplit_url}<<")
    # Keep query without any checks, and reset fragment.
    if parts.path == "":
        # This simplifies/unifies "https://example.com" to "https://example.com/"
        path = "/"
    else:
        path = parts.path
    simplified_url = urllib.parse.urlunsplit((parts.scheme, hostname, path, parts.query, ""))
    return simplified_url, None


def simplify_semantically(by_regexed_url, disasters):
    by_simplified_url = defaultdict(list)  # Simplified URL string to list of occurrence-objects
    all_seen_chars = Counter()
    usable_hostnames = set()
    for parse_url, occs in by_regexed_url.items():
        simplified_url, disaster_reason = simplified_url_or_disaster_reason(parse_url)
        assert (simplified_url is None) != (disaster_reason is None), parse_url
        if disaster_reason is not None:
            disaster = disasters[parse_url]
            disaster.extend(disaster_reason, occs)
            continue
        all_seen_chars.update(parse_url)
        by_simplified_url[simplified_url].extend(occs)
    return by_simplified_url, all_seen_chars


def find_coordinate(key, candidates_dict):
    # DFS to find anything with a coordinate.
    for candidate in candidates_dict.get(key, []):
        if len(candidate[0]) > 1:
            # Must be a coordinate, we're done.
            return candidate
        result = find_coordinate(candidate, candidates_dict)
        if result is not None:
            return result
    # If this was an indirect call, failing to find a coordinate is okay.
    return None


def resolve_candidates_single_query(query_items, pbf_filename):
    print(f"  resolving {len(query_items)} geolocations …")
    query_string = "".join(f"{item}\n" for item in query_items)
    proc = subprocess.Popen(
        ["osmium", "getid", pbf_filename, "-r", "-fopl", "-i-"],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        text=True,
    )
    # This is an incredibly stupid approach, since:
    # - osmium getid does a linear scan, so ideally we query for all IDs in one go, and retreive
    #   *all* related IDs.
    # - Doing so means that at the end of proc.communicate, we hold the large input and gigantic
    #   output simultaneously in memory.
    # - The pbf fileformat theoretically allows for reasonably easy random access lookups.
    # So in theory, this could be sped up by a large factor.
    outs, _ = proc.communicate(input=query_string)
    del query_string
    # stderr is not captured, and cannot be tested.
    assert proc.returncode in [0, 1], proc.returncode
    candidates_dict = dict()
    lines = outs.split("\n")
    del outs
    assert lines[-1] == ""
    lines.pop()
    for line in lines:
        # Spaces are percent-encoded, so we can safely split by spaces:
        parts = line.split(" ")
        key = parts[0]
        assert key not in candidates_dict
        values_by_letter = {part[0]: part[1:] for part in parts[1:]}
        if key[0] == "r":
            members = values_by_letter["M"].split(",")
            values = [member.split("@")[0] for member in members]
        elif key[0] == "w":
            values = values_by_letter["N"].split(",")
        elif key[0] == "n":
            # Intentionally don't interpret as floating point values, to prevent precision loss/creep.
            values = [(values_by_letter["x"], values_by_letter["y"])]
        else:
            raise AssertionError(f"unknown resulting item type, perhaps line is not valid OPL?! >> {line}")
        candidates_dict[key] = values
    missing_keys = set(query_items) - set(candidates_dict.keys())
    assert not missing_keys, missing_keys
    results = dict()
    for item in query_items:
        coord = find_coordinate(item, candidates_dict)
        # If 'item' cannot be resolved at all, the datasource (pbf file) is incomplete.
        assert coord is not None, item
        results[item] = coord
    return results


def resolve_item_locations(items, pbf_filename):
    item_map = resolve_candidates_single_query(items, pbf_filename)
    assert len(item_map) == len(items), (len(item_map), len(items))
    return item_map


def inject_locations(findings, pbf_filename):
    interesting_osm_items = set()
    for finding in findings:
        for occ in finding["occ"]:
            interesting_osm_items.add(f'{occ["t"]}{occ["id"]}')
    resolved_items = resolve_item_locations(interesting_osm_items, pbf_filename)
    print("  injecting …")
    for finding in findings:
        for occ in finding["occ"]:
            loc = resolved_items[f'{occ["t"]}{occ["id"]}']
            occ["x"] = loc[0]
            occ["y"] = loc[1]


def report_stats(old_findings, by_simplified_url, disasters):
    print(f"{len(old_findings)} unique tag-values resulted in {len(by_simplified_url)} unique simplified URLs.")
    random_disasters = list(disasters.items())
    random.shuffle(random_disasters)
    random_disasters = random_disasters[:10]
    print(f"Random sampling of {len(random_disasters)} of {len(disasters)} disastrous URLs:")
    for disaster in random_disasters:
        print(f" - {disaster[0]} ({disaster[1].reasons})")


def cleanup(data, pbf_filename):
    assert data["v"] == 1
    assert data["type"] == "monitor-osm-domains extraction results"
    data["type"] = "monitor-osm-domains extraction results, filtered"
    old_findings = data["findings"]
    del data["findings"]
    disasters = defaultdict(DisasterUrl)  # Original or simplified URL to list of occurrence-objects and set of reasons
    data["disasters"] = disasters

    print("Resolving and injecting geo locations …")
    inject_locations(old_findings, pbf_filename)

    # Break down all tag values into singular URLs, potentially already rejecting
    # some terrible values, or grouping entries with the same regexed URL.
    print("Simplifying by regex …")
    by_regexed_url = simplify_regex(old_findings, disasters)

    # Parse URLs, simplify semantically. This can cause disasters when we discover
    # login information, have unexplained data loss, or discover a non-standard port.
    print("Simplifying semantically …")
    by_simplified_url, all_seen_chars = simplify_semantically(by_regexed_url, disasters)
    data["simplified_urls"] = by_simplified_url

    report_stats(old_findings, by_simplified_url, disasters)

    # Warn about weird characters:
    for boring_char in NORMAL_URL_CHARS:
        del all_seen_chars[boring_char]
    interesting_chars = list(all_seen_chars.most_common())
    if interesting_chars:
        print(f"Saw some funky characters:")
        for char, count in interesting_chars:
            print(f"    {count} times >>{char}<< → {str(char.encode())}")


def run(input_filename, pbf_filename, output_filename):
    with open(input_filename, "r") as fp:
        data = json.load(fp)
    cleanup(data, pbf_filename)
    print(f"Writing to {output_filename} …")
    with open(output_filename, "w") as fp:
        json.dump(data, fp, cls=DisasterEncoder)
    print(f"All done! Results written to file.")


if __name__ == "__main__":
    print("Warning: This program will easily consumes about 13 GB of RAM.")
    selftest()
    if len(sys.argv) != 4:
        print(f"USAGE: {sys.argv[0]} /path/to/input/raw.monosmdom.json /path/to/input/datasource.pbf /path/to/output/all.monosmdom.json", file=sys.stderr)
        print("Note that the datasource must contain all relations/ways/nodes and their referenced items, but does not necessarily need to be identical to what generated the raw monosmdom json.", file=sys.stderr)
        exit(1)
    run(sys.argv[1], sys.argv[2], sys.argv[3])
