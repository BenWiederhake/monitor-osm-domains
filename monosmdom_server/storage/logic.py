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


def get_strict_sld_and_interest(url):
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


class LeafModelBulkCache:
    def __init__(self):
        self.objs_disasterurl = []
        self.objs_crawlableurl = []
        self.objs_occurrenceinosm = []

    def flush(self):
        models.DisasterUrl.objects.bulk_create(
            self.objs_disasterurl,
            update_conflicts=True,
            update_fields=["reason"],
            unique_fields=["url"],
        )
        self.objs_disasterurl = []
        models.CrawlableUrl.objects.bulk_create(self.objs_crawlableurl)
        self.objs_crawlableurl = []
        models.OccurrenceInOsm.objects.bulk_create(self.objs_occurrenceinosm)
        self.objs_occurrenceinosm = []

    # @classmethod
    # FIXME: Somehow, "@classmethod" breaks wk-only arguments. Why?!
    def upsert_durl(cache, *, url, reason):
        if cache is None:
            durl, created = models.DisasterUrl.objects.get_or_create(
                url=url, defaults=dict(reason=reason)
            )
            if not created and durl.reason != reason:
                # Need to overwrite the "wrong" reason:
                durl.save()
        else:
            cache.objs_disasterurl.append(models.DisasterUrl(url=url, reason=reason))

    # @classmethod
    # FIXME: Somehow, "@classmethod" breaks "**kwargs". Why?!
    def upsert_crurl_via(cache, **kwargs):
        if cache is None:
            crurl, _created = models.CrawlableUrl.objects.get_or_create(**kwargs)
            return crurl
        else:
            # Note: If we use a cache, then we can assume this is a bulk import that already has
            # been deduplicated.
            crurl = models.CrawlableUrl(**kwargs)
            cache.objs_crawlableurl.append(crurl)
            # We shouldn't return crurl because it does not have an ID yet, and never will have.
            # We shouldn't return None, because that might be misconstrued as the indication for
            # disaster. Instead, return a poison value, since in the casae of bulk inserting, the
            # CrawlableUrl should not be used anyway.
            return models.CrawlableUrl.DoesNotExist()

    def cache_occ(self, **kwargs):
        occ = models.OccurrenceInOsm(**kwargs)
        self.objs_occurrenceinosm.append(occ)


MaybeCrawlableResult = collections.namedtuple("MaybeCrawlableResult", ["url_obj", "domain", "want_to_crawl"])


def discover_url(url_string, *, mark_crawlable=False, cache=None):
    """
    Given a dirty URL string, this function runs a few sanity checks:
    - Syntactical checks test for illegal schemes, ports, auth info, etc. (see ../../extract/cleanup.py).
    - Semantical checks try to determine a registrable second-level-domain (according to PSL data).
    - Interest check: Does the hostname occur in 'IGNORED_HOSTNAMES'?
    Depending on these checks, it does one of the following:
    (1) If a syntactical or semantical check fails, a corresponding Url and DisasterUrl
        is created, and the Url instance is returned, with want_to_crawl=False.
    (2) If the interest check fails, only a corresponding Url is created, and the Url instance is
        returned, with want_to_crawl=False.
    (3) If all checks pass, then a corresponding Url and Domain is created and returned,
        with want_to_crawl=True. If mark_crawlable is True, then a CrawlableUrl is also created and
        linked with the freshly-created Url and Domain.

    If that was too much, maybe this truth table helps:

            -----Passes?----- -arg-   →   ---------Creates?--------    ---Returns?--
             Syn | Sem | Int | mark   |    Url | DisU | CraU | Dom     want_to_crawl
            --------------------------+-----------------------------------------------
        (1)   N     -     -      -    |     Y      Y      n     n          False
        (1)   Y     N     -      -    |     Y      Y      n     n          False
        (2)   Y     Y     N      -    |     Y      n      n     n          False
        (3)   Y     Y     Y      N    |     Y      n      n     Y          True
        (3)   Y     Y     Y      Y    |     Y      n      Y     Y          True

    Guarantees:
    - The return values are always of type MaybeCrawlableResult.
    - Url/Domain/CrawlableUrl are upserted; i.e. if they already exist in the DB, the existing row will be used.
      (Note that duplicate DisasterUrls are somewhat reasonable during import.)
    - CrawlableUrl" instance is created and linked, and the CrawlableUrl instance is returned.

    FIXME: This obviously needs tests.
    """
    # Note that we don't apply regex-fixups to redirects, since these should absolutely be valid URLs already.
    # === Syntactical check:
    simplified_url, disaster_reason = extract_cleanup.simplified_url_or_disaster_reason(url_string)
    assert (simplified_url is None) != (disaster_reason is None)
    if cache is not None:
        assert simplified_url == url_string, (simplified_url, url_string)
    if simplified_url is not None:
        url_string = simplified_url
    # Note that only now we know what the URL in the database will actually be, since we want to deduplicate in case of "weird" redirects.
    url_object = upsert_url(url_string)
    # === Semantical and interest check:
    second_level_domain, have_interest = None, False
    if disaster_reason is None:
        second_level_domain, have_interest = get_strict_sld_and_interest(url_string)
        if second_level_domain is None:
            disaster_reason = "has no public suffix"
    # === Handle syntactical/semantical failure:
    if disaster_reason is not None:
        LeafModelBulkCache.upsert_durl(cache, url=url_object, reason=disaster_reason)
        return MaybeCrawlableResult(url_object, None, False)
    # === Handle interest failure:
    if not have_interest:
        return MaybeCrawlableResult(url_object, None, False)
    # === Handle crawlable URL, the only happy path:
    domain_object, _created = models.Domain.objects.get_or_create(domain_name=second_level_domain)
    if mark_crawlable:
        crawlable_object = LeafModelBulkCache.upsert_crurl_via(cache, url=url_object, domain=domain_object)
    else:
        crawlable_object = None
    return MaybeCrawlableResult(url_object, domain_object, True)
