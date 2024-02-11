#!/usr/bin/env python3

from collections import Counter, defaultdict
import json
import os
import random
import re
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

DISASTROUS_CHARACTERS = [ch.decode() for ch in DISASTROUS_CHARACTERS_BYTES]


class DisasterUrl:
    def __init__(self):
        self.reasons = set()
        self.occs = []

    def extend(self, reason, occs):
        self.reasons.add(reason)
        self.occs.extend(occs)


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
        return None, "contains login information, not crawling"
    # Accessing the 'port' property raises an exception in invalid URLs -.-
    try:
        port_number = parts.port
    except ValueError:
        return None, "port is not a valid integer"
    port_string = ""
    if port_number is not None:
        # Using ports is highly questionable, and I would love to mark it disastrous.
        # However, a disturbingly high number of servers redirect e.g.
        # from "https://example.com/" to "https://example.com:443/". So we have to permit it.
        if parts.scheme == "http" and port_number == 80:
            port_string = ":80"
        elif parts.scheme == "https" and port_number == 443:
            port_string = ":443"
        else:
            return None, f"refusing to use forced port {port_number}"
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


def report_stats(old_findings, by_simplified_url, disasters):
    print(f"{len(old_findings)} unique tag-values resulted in {len(by_simplified_url)} unique simplified URLs.")
    random_disasters = list(disasters.items())
    random.shuffle(random_disasters)
    random_disasters = random_disasters[:10]
    print(f"Random sampling of {len(random_disasters)} of {len(disasters)} disastrous URLs:")
    for disaster in random_disasters:
        print(f" - {disaster[0]} ({disaster[1].reasons})")


def cleanup(data):
    assert data["v"] == 2
    assert data["type"] == "monitor-osm-domains extraction results"
    data["type"] = "monitor-osm-domains extraction results, filtered"
    old_findings = data["findings"]
    del data["findings"]
    disasters = defaultdict(DisasterUrl)  # Original or simplified URL to list of occurrence-objects and set of reasons
    data["disasters"] = disasters

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
        print("Saw some funky characters:")
        for char, count in interesting_chars:
            print(f"    {count} times >>{char}<< → {str(char.encode())}")


def run(input_filename, output_filename):
    if os.path.exists(output_filename):
        print(f"Refusing to overwrite {output_filename}")
    with open(input_filename, "r") as fp:
        data = json.load(fp)
    cleanup(data)
    print(f"Writing to {output_filename} …")
    with open(output_filename, "w") as fp:
        json.dump(data, fp, cls=DisasterEncoder)
    print("All done! Results written to file.")


if __name__ == "__main__":
    selftest()
    if len(sys.argv) != 3:
        print(f"USAGE: {sys.argv[0]} /path/to/input/raw.monosmdom.json /path/to/output/all.monosmdom.json", file=sys.stderr)
        exit(1)
    run(sys.argv[1], sys.argv[2])
