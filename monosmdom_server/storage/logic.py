from pathlib import Path
from storage import extract_cleanup, models
import collections
import publicsuffix2
import urllib.parse


PSL_FILENAME = Path(__file__).resolve().parent / "data/public_suffix_list.dat"

# Some hostnames appear way too often in the dataset, and are uninteresting for our purposes.
# These services are likely to work equally well as each other, so checking thousands of URLs is pointless.
# Some of the pages on these servers are actually broken (i.e. 404), but I can't easily fix them.
# I'd be happy to collaborate with you on fixing these. My focus for now is finding dead domains, offline webservers, etc.
IGNORED_HOSTNAMES = {
    "qr.bvg.de",  # Haltestellen Berlin, 7363 URLs
    "fahrinfo.vbb.de",  # Haltestellen Berlin-Brandenburg, 3956 URLs
    "ns.gis-bldam-brandenburg.de",  # Denkmale in Brandenburg, 2228 URLs
    "www.wuppertal.de", "wuppertal.de",  # Denkmale and attractions in Wuppertal, >2128 URLs
    "gisdata.krzn.de",  # Denkmale, GIS data, and attractions in NRW, 1592 URLs
    "www.denkmalpflege.bremen.de", "denkmalpflege.bremen.de",  # Denkmale Bremen, >1462 URLs
    "www.stadtwerke-muenster.de", "stadtwerke-muenster.de",  # Haltestellen Münster, >1314 URLs
    "www.stolpersteine-berlin.de", "stolpersteine-berlin.de",  # Stolpersteine Berlin, >1075 URLs
    "www.suehnekreuz.de", "suehnekreuz.de",  # Mahnkreuze, Germany-wide, >1008 URLs
    "www.dortmund.de", "dortmund.de",  # ??? mostly dead links, Dortmund, >944 URLs
    "kulturdb.de",  # Mahnkreuze, Germany-wide, 884 URLs
    "www.rewe.de", "rewe.de",  # Discounter, Germany-wide, >866 URLs
    "www.edeka.de", "edeka.de",  # Discounter, Germany-wide, >822 URLs
    "denkmaldatenbank.berlin.de",  # Denkmale Berlin, 810 URLs
    "rips-dienste.lubw.baden-wuerttemberg.de",  # Naturschutzgebiete BaWü, 746 URLs
    "www.museenkoeln.de", "museenkoeln.de",  # Stolpersteine Köln, mostly dead links, >663 URLs
    "db-sandsteinklettern.gipfelbuch.de",  # Gipfelbücher, 643 URLs
    "www.facebook.com", "www.facebook.de", "de-de.facebook.com", "m.facebook.com", "facebook.com",  # If you don't know it be glad, >638 URLs
    "www.spessartprojekt.de", "spessartprojekt.de",  # Naturschutzgebiete(?) Spessart, monstly dead links, >621 URLs
    "nsg.naturschutzinformationen.nrw.de",  # Naturschutzgebiete NRW, 618 URLs
    "gdi.essen.de",  # ALL DEAD LINKS, wtf, 601 URLs
    "de.wikipedia.org", "de.m.wikipedia.org",  # You already know, >583 URLs
    "vertretung.allianz.de",  # Store managers(?) of Allianz, 570 URLs
    "www.nlwkn.niedersachsen.de", "nlwkn.niedersachsen.de",  # Naturschutzgebiete Niedersachsen, >556 URLs
    # -- I didn't check the following domains, but it seems reasonable to skip them.
    "www.denkmalprojekt.org", "denkmalprojekt.org",  # >486 URLs
    "denkmalpflege.bremen.de",  # 444 URLs
    "www.aldi-nord.de", "aldi-nord.de", "www.aldi-sued.de", "aldi-sued.de", "aldi.de",  # >438 URLs
    "www.magdeburg.de", "magdeburg.de",  # >415 URLs
    "www.berlin.de", "berlin.de",  # 411 URLs
    "youtu.be", "www.youtube.com", "youtube.com",  # >385 URLs
    "wuerzburgwiki.de",  # 371 URLs
    "www.outdoor-karte.de", "outdoor-karte.de",  # >368 URLs
    "www.lidl.de", "lidl.de",  # 360 URLs
    "polska-org.pl",  # 360 URLs
    "www.netto-online.de", "netto-online.de",  # >357 URLs
    # This eliminates over 37k out of 572k URLs. It may be less than 10%, but it's saved work/traffic/energy nonetheless.
    # Since we avoid recently-queried domains, this only has an effect in the long run.
}


def get_cached_psl(*, _magic=[]):
    if not _magic:
        with open(PSL_FILENAME, "r") as fp:
            _magic.append(publicsuffix2.PublicSuffixList(fp))
    return _magic[0]


def get_strict_sld_of_url_or_none_and_interest(url):
    """
    Sorry for mixing two queries in the same function, but I want to avoid parsing the URL needlessly often.
    Note that we must use hostname for the interest check, and not the second-level-domain.
    Example: We want to ignore the URL 'https://www.wuppertal.de/denkmalliste-online/Detail/Show/13027',
        which has the hostname 'www.wuppertal.de' and the sld 'wuppertal.de'.
    Example: We want to crawl the URL 'http://www.jobcenter.wuppertal.de/geschaeftsstellen/index.php',
        which has the hostname 'www.jobcenter.wuppertal.de' and the sld 'wuppertal.de'.
        This URL currently result results in 404, and should be replaced by:
        https://jobcenter.wuppertal.de/kontakt/content/geschaeftsstellen.php
        (Shame on them for not setting up a proper redirect!)
    Finding this example was non-trivial, so the difference doesn't seem to be too important.
    """
    hostname = urllib.parse.urlsplit(url).hostname
    sld_or_none = get_cached_psl().get_sld(hostname, strict=True)
    have_interest = hostname not in IGNORED_HOSTNAMES
    return sld_or_none, have_interest


def upsert_url(raw_url):
    """
    Creates (or gets an existing equivalent) instance of 'models.Url'.
    Note that this does NOT create any Domain, DisasterUrl, or CrawlableUrl rows.
    """
    url, _created = models.Url.objects.get_or_create(url=raw_url)
    return url


MaybeCrawlableResult = collections.namedtuple("MaybeCrawlableResult", ["url_obj", "crawlable_url_obj_or_none"])


def try_crawlable_url(url_string):
    """
    Given a dirty URL string, this function runs a few sanity checks:
    - Syntactical checks test for illegal schemes, ports, auth info, etc. (see ../../extract/cleanup.py).
    - Semantical checks try to determine a registrable second-level-domain (according to PSL data).
    - Interest check: Does the hostname occur in 'IGNORED_HOSTNAMES'?
    Depending on these checks, it does one of the following:
    (1) If a syntactical or semantical check fails, a corresponding Url and one DisasterUrl is created,
        and the Url instance is returned.
    (2) If the interest check fails, only a corresponding Url is created, and the Url instance is returned.
    (3) If all checks pass, a corresponding Url, CrawlableUrl, and Domain is created, and the Url
        and CrawlableUrl instances are returned.

    If that was too much, maybe this truth table helps:

            -----Passes?-----   →   ---------Creates?--------
             Syn | Sem | Int    |    Url | DisU | CraU | Dom
            --------------------+----------------------------
        (1)   N     -     -     |     Y      Y      n     n
        (1)   Y     N     -     |     Y      Y      n     n
        (2)   Y     Y     N     |     Y      n      n     n
        (3)   Y     Y     Y     |     Y      n      Y     Y

    Guarantees:
    - The return values are always of type MaybeCrawlableResult.
    - Url/Domain/CrawlableUrl are upserted; i.e. if they already exist in the DB, the existing row will be used.
      (Note that duplicate DisasterUrls are somewhat reasonable.)
      FIXME: No, they aren't! Avoid creating duplicate DisasterUrls in case of identical reasons.
      This can happen if for example a server redirects all (distinct) URLs to the same invalid URL.
    - CrawlableUrl" instance is created and linked, and the CrawlableUrl instance is returned.

    FIXME: This obviously needs tests.
    """
    # Note that we don't apply regex-fixups to redirects, since these should absolutely be valid URLs already.
    # === Syntactical check:
    simplified_url, disaster_reason = extract_cleanup.simplified_url_or_disaster_reason(raw_url)
    assert (simplified_url is None) != (disaster_reason is None)
    if simplified_url is not None:
        url_string = simplified_url
    url_object = upsert_url(url_string)
    # === Semantical and interest check:
    second_level_domain, have_interest = None
    if disaster_reason is None:
        second_level_domain = get_strict_sld_of_url_or_none(url_string)
        if second_level_domain is None:
            disaster_reason = "has no public suffix"
    # === Handle syntactical/semantical failure:
    if disaster_reason is not None:
        models.DisasterUrl(url_object, disaster_reason).save()
        return MaybeCrawlableResult(url_object, None)
    # === Handle interest failure:
    return MaybeCrawlableResult(url_object, None)
    # === Handle crawlable URL, the only happy path:
    domain_object, _created = models.Domain.objects.get_or_create(domain_name=second_level_domain)
    crawlable_object, _created = models.CrawlableUrl.objects.get_or_create(url=url_object, domain=domain_object)
    return MaybeCrawlableResult(url_object, crawlable_object)
