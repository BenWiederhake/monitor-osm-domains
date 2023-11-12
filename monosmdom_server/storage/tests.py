from django.test import TestCase, TransactionTestCase
from storage import extract_cleanup, logic, models


# Note that the simplification itself is already tested as self-tests during boot of extract_cleanup.py.
# This unittest checks whether certain things are detected or not.
class UrlClassificationTests(TestCase):
    def assertReason(self, url_string, expected_simplification, expected_reason):
        simplified_url, actual_reason = extract_cleanup.simplified_url_or_disaster_reason(url_string)
        self.assertEqual((expected_simplification, expected_reason), (simplified_url, actual_reason), url_string)

    def assertReasonBulk(self, batch):
        for url_string, simplified_url, expected_reason in batch:
            with self.subTest(url_string=url_string, simplified_url=simplified_url, expected_reason=expected_reason):
                self.assertReason(url_string, simplified_url, expected_reason)

    def testManuallyWritten(self):
        self.assertReasonBulk([
            ("ftp://asdf/qwer", None, "unusual scheme ftp"),
            ("https://asdf/qwer", "https://asdf/qwer", None),
            ("https://foo.com:8475/qwer", None, "refusing to use forced port 8475"),
            ("https://user@pass:foo.com/qwer", None, "contains login information, not crawling"),
            ("https://user:foo.com/qwer", None, "port is not a valid integer"),
            ("https://user@foo.com/qwer", None, "contains login information, not crawling"),
            ("https://foo.com:0/qwer", None, "refusing to use forced port 0"),
            ("https://foo.com:-1/qwer", None, "port is not a valid integer"),
            ("https://foo.com:65535/qwer", None, "refusing to use forced port 65535"),
            ("https://foo.com:65536/qwer", None, "port is not a valid integer"),
            ("https://foo.com:0x1bb/qwer", None, "port is not a valid integer"),
            ("https://foo.com:443.1/qwer", None, "port is not a valid integer"),
            ("https://foo.com:443e0/qwer", None, "port is not a valid integer"),
            ("https://foo.com/", "https://foo.com/", None),
            ("https://foo.com/example?q=1#quux", "https://foo.com/example?q=1", None),
            ("https://foo.com?q=1#quux", "https://foo.com/?q=1", None),
            ("https://weird....dots", None, "double-dot in hostname"),
            ("https:///slashes/bro/", None, "disagreeing netloc='' and hostname=None"),
            ("https://12.34.56.78/frobnicate", None, "hostname='12.34.56.78' looks like a bare IP"),
            ("https://lol.invalid/frobnicate.html=3", "https://lol.invalid/frobnicate.html=3", None),
        ])

    def testRealLifeDistinct(self):
        self.assertReasonBulk([
            ("https://", None, "disagreeing netloc='' and hostname=None"),
            ("https:///", None, "disagreeing netloc='' and hostname=None"),
            ("https://://www.golfclub-rheinblick.ck", None, "disagreeing netloc=':' and hostname=None"),
            ("https://https://baeckerei-kuenkel.de/", None, "disagreeing netloc='https:' and hostname='https'"),
            ("https://whttps://www.autohaus-wehner.de/Standorte/Buchholzww.hyundai.de/", None, "disagreeing netloc='whttps:' and hostname='whttps'"),
            ("https://www.fliesen-ermert.de:", None, "disagreeing netloc='www.fliesen-ermert.de:' and hostname='www.fliesen-ermert.de'"),
            ("http://www.https://www.columbus-evk.de/", None, "disagreeing netloc='www.https:' and hostname='www.https'"),
            ("https://wwwhttps://stadtteilpraxis.de/#block-2", None, "disagreeing netloc='wwwhttps:' and hostname='wwwhttps'"),
            ("https://www.hyundai.https://www.autohaus-wehner.de/Standorte/Buchholzde/", None, "disagreeing netloc='www.hyundai.https:' and hostname='www.hyundai.https'"),
            ("https://www.mc-oberspree.de:", None, "disagreeing netloc='www.mc-oberspree.de:' and hostname='www.mc-oberspree.de'"),
            ("http://www.johanniter...rvwuerttemberg-mitte/", None, "double-dot in hostname"),
            ("http://176.28.22.140", None, "hostname='176.28.22.140' looks like a bare IP"),
            ("https.www.metzgerei-woelfel.de", None, "unusual scheme "),
            ("https.com://www.drk.de/", None, "unusual scheme https.com"),
        ])


class SecondLevelDomainTests(TestCase):
    def assertSld(self, url_string, expected_sld, expected_interest):
        actual = logic.get_strict_sld_and_interest(url_string)
        self.assertEqual((expected_sld, expected_interest), actual, url_string)

    def assertSldBulk(self, batch):
        for url_string, expected_sld, expected_interest in batch:
            with self.subTest(url_string=url_string, expected_sld=expected_sld, expected_interest=expected_interest):
                self.assertSld(url_string, expected_sld, expected_interest)

    def testManuallyWritten(self):
        self.assertSldBulk([
            ("https://foo.com/qwer", "foo.com", True),
            ("https://foo.com/", "foo.com", True),
            ("https://foo.com", "foo.com", True),
            ("https://bar.example.foo.com", "foo.com", True),
            ("https://com", "com", True),  # Meh
            ("https://something.local", None, True),
            ("https://localhost", None, True),
            ("https://weird.aaaaaaaaa", None, True),
            ("https://in.the.biz", "the.biz", True),
            ("https://qr.bvg.de", "bvg.de", False),
            ("https://www.stadtwerke-muenster.de", "stadtwerke-muenster.de", False),
            ("https://stadtwerke-muenster.de", "stadtwerke-muenster.de", False),
            ("https://anders.stadtwerke-muenster.de", "stadtwerke-muenster.de", True),
            ("https://de.wikipedia.org", "wikipedia.org", False),
            ("https://de.m.wikipedia.org", "wikipedia.org", False),
            ("https://xy.m.wikipedia.org", "wikipedia.org", True),
            ("https://www.netto-online.de", "netto-online.de", False),
            ("https://www.netto-online.com", "netto-online.com", True),
            ("https://house.cat", "house.cat", True),  # How is this a real tld?!
            ("https://foo.invalid", None, True),
            ("https://foo.example", None, True),
            ("https://whateverest", None, True),
        ])

    def testRealLifeNegative(self):
        self.assertSldBulk([
            ("https://interstil.d/", None, True),
            ("http://denkmalliste/denkmalliste/index.php", None, True),
            ("https://1996-03-04/", None, True),
            ("http://silvia-steinfort.da/", None, True),
            ("https://1988-11-11/", None, True),
            ("https://www.geers.depaign=mybusiness/", None, True),
            ("https://wheelparkverein.jimdo.html/", None, True),
            ("https://h/", None, True),
            ("http://bestell-bei-zero,de/", None, True),
            ("https://www.stadtkirche-heidelberg.dehtml/content/kindergarten_st_marien749.html", None, True),
        ])

    def testRealLifePositive(self):
        self.assertSldBulk([
            ("https://sub.kinderladen-strueverweg.de", "kinderladen-strueverweg.de", True),
            ("https://kinderladen-strueverweg.de", "kinderladen-strueverweg.de", True),
            ("https://sub.bikeoholix.de", "bikeoholix.de", True),
            ("https://bikeoholix.de", "bikeoholix.de", True),
            ("https://sub.fotofriedrich.eu", "fotofriedrich.eu", True),
            ("https://fotofriedrich.eu", "fotofriedrich.eu", True),
            ("https://sub.dr-knefel.com", "dr-knefel.com", True),
            ("https://dr-knefel.com", "dr-knefel.com", True),
            ("https://sub.afs.aero", "afs.aero", True),
            ("https://afs.aero", "afs.aero", True),
            ("https://sub.campingplatz-prettin.de", "campingplatz-prettin.de", True),
            ("https://campingplatz-prettin.de", "campingplatz-prettin.de", True),
            ("https://sub.behrens-gartengestaltung.app", "behrens-gartengestaltung.app", True),
            ("https://behrens-gartengestaltung.app", "behrens-gartengestaltung.app", True),
            ("https://sub.reiterhof-schmidt.info", "reiterhof-schmidt.info", True),
            ("https://reiterhof-schmidt.info", "reiterhof-schmidt.info", True),
            ("https://sub.hangbird.net", "hangbird.net", True),
            ("https://hangbird.net", "hangbird.net", True),
            ("https://sub.implantat.cc", "implantat.cc", True),
            ("https://implantat.cc", "implantat.cc", True),
            ("https://sub.immobilienmakler.koeln", "immobilienmakler.koeln", True),
            ("https://immobilienmakler.koeln", "immobilienmakler.koeln", True),
            ("https://sub.vossen.biz", "vossen.biz", True),
            ("https://vossen.biz", "vossen.biz", True),
            ("https://sub.buchen.travel", "buchen.travel", True),
            ("https://buchen.travel", "buchen.travel", True),
            ("https://sub.kraftwerk24.fitness", "kraftwerk24.fitness", True),
            ("https://kraftwerk24.fitness", "kraftwerk24.fitness", True),
            ("https://sub.mr-crumble.shop", "mr-crumble.shop", True),
            ("https://mr-crumble.shop", "mr-crumble.shop", True),
            ("https://sub.meissen.online", "meissen.online", True),
            ("https://meissen.online", "meissen.online", True),
            ("https://sub.suffa.ac", "suffa.ac", True),
            ("https://suffa.ac", "suffa.ac", True),
            ("https://sub.vet.sh", "vet.sh", True),
            ("https://vet.sh", "vet.sh", True),
            ("https://sub.physiovita.org", "physiovita.org", True),
            ("https://physiovita.org", "physiovita.org", True),
            ("https://sub.simplybook.it", "simplybook.it", True),
            ("https://simplybook.it", "simplybook.it", True),
            ("https://sub.grobi.tv", "grobi.tv", True),
            ("https://grobi.tv", "grobi.tv", True),
            ("https://sub.wannseeterrassen.berlin", "wannseeterrassen.berlin", True),
            ("https://wannseeterrassen.berlin", "wannseeterrassen.berlin", True),
            ("https://sub.planit.legal", "planit.legal", True),
            ("https://planit.legal", "planit.legal", True),
            ("https://sub.klostermaier.bayern", "klostermaier.bayern", True),
            ("https://klostermaier.bayern", "klostermaier.bayern", True),
            ("https://sub.retina.to", "retina.to", True),
            ("https://retina.to", "retina.to", True),
            ("https://sub.traum-ferienwohnungen.at", "traum-ferienwohnungen.at", True),
            ("https://traum-ferienwohnungen.at", "traum-ferienwohnungen.at", True),
            ("https://sub.drk-ambulanzdienst.hamburg", "drk-ambulanzdienst.hamburg", True),
            ("https://drk-ambulanzdienst.hamburg", "drk-ambulanzdienst.hamburg", True),
            ("https://sub.landauer.ch", "landauer.ch", True),
            ("https://landauer.ch", "landauer.ch", True),
            ("https://sub.hd.digital", "hd.digital", True),
            ("https://hd.digital", "hd.digital", True),
            ("https://sub.manoah.haus", "manoah.haus", True),
            ("https://manoah.haus", "manoah.haus", True),
            ("https://sub.eurotrucks.nl", "eurotrucks.nl", True),
            ("https://eurotrucks.nl", "eurotrucks.nl", True),
            ("https://sub.watzup.bike", "watzup.bike", True),
            ("https://watzup.bike", "watzup.bike", True),
            ("https://sub.weiterstadt.taxi", "weiterstadt.taxi", True),
            ("https://weiterstadt.taxi", "weiterstadt.taxi", True),
            ("https://sub.zeitfuerdich.center", "zeitfuerdich.center", True),
            ("https://zeitfuerdich.center", "zeitfuerdich.center", True),
            ("https://sub.likehome.immo", "likehome.immo", True),
            ("https://likehome.immo", "likehome.immo", True),
            ("https://sub.volgelsheim.fr", "volgelsheim.fr", True),
            ("https://volgelsheim.fr", "volgelsheim.fr", True),
            ("https://sub.saitow.ag", "saitow.ag", True),
            ("https://saitow.ag", "saitow.ag", True),
            ("https://sub.seven.one", "seven.one", True),
            ("https://seven.one", "seven.one", True),
            ("https://sub.horskasluzba.cz", "horskasluzba.cz", True),
            ("https://horskasluzba.cz", "horskasluzba.cz", True),
            ("https://sub.melter.xyz", "melter.xyz", True),
            ("https://melter.xyz", "melter.xyz", True),
            ("https://sub.nasa.gov", "nasa.gov", True),
            ("https://nasa.gov", "nasa.gov", True),
            ("https://sub.spinlab.co", "spinlab.co", True),
            ("https://spinlab.co", "spinlab.co", True),
            ("https://sub.fairway.name", "fairway.name", True),
            ("https://fairway.name", "fairway.name", True),
            ("https://sub.wurzelwerk.events", "wurzelwerk.events", True),
            ("https://wurzelwerk.events", "wurzelwerk.events", True),
            ("https://sub.theatercafe.es", "theatercafe.es", True),
            ("https://theatercafe.es", "theatercafe.es", True),
            ("https://sub.vorreiter.doctor", "vorreiter.doctor", True),
            ("https://vorreiter.doctor", "vorreiter.doctor", True),
            ("https://sub.spies.hn", "spies.hn", True),
            ("https://spies.hn", "spies.hn", True),
            ("https://sub.konsolosluk.gov.tr", "konsolosluk.gov.tr", True),
            ("https://konsolosluk.gov.tr", "konsolosluk.gov.tr", True),
            ("https://sub.kronprinz.beer", "kronprinz.beer", True),
            ("https://kronprinz.beer", "kronprinz.beer", True),
            ("https://sub.freudich.design", "freudich.design", True),
            ("https://freudich.design", "freudich.design", True),
            ("https://sub.pfinztal.versicherung", "pfinztal.versicherung", True),
            ("https://pfinztal.versicherung", "pfinztal.versicherung", True),
            ("https://sub.radiomkw.fm", "radiomkw.fm", True),
            ("https://radiomkw.fm", "radiomkw.fm", True),
            ("https://sub.galerie-seidel.cologne", "galerie-seidel.cologne", True),
            ("https://galerie-seidel.cologne", "galerie-seidel.cologne", True),
            ("https://sub.braeutigam.gmbh", "braeutigam.gmbh", True),
            ("https://braeutigam.gmbh", "braeutigam.gmbh", True),
            ("https://sub.marclanz.reisen", "marclanz.reisen", True),
            ("https://marclanz.reisen", "marclanz.reisen", True),
            ("https://sub.camping-port.lu", "camping-port.lu", True),
            ("https://camping-port.lu", "camping-port.lu", True),
            ("https://sub.landkreis.gr", "landkreis.gr", True),
            ("https://landkreis.gr", "landkreis.gr", True),
            ("https://sub.malafemmena.restaurant", "malafemmena.restaurant", True),
            ("https://malafemmena.restaurant", "malafemmena.restaurant", True),
            ("https://sub.quincyschultz.club", "quincyschultz.club", True),
            ("https://quincyschultz.club", "quincyschultz.club", True),
            ("https://sub.biovox.systems", "biovox.systems", True),
            ("https://biovox.systems", "biovox.systems", True),
            ("https://sub.mfa.bg", "mfa.bg", True),
            ("https://mfa.bg", "mfa.bg", True),
            ("https://sub.fsspx.today", "fsspx.today", True),
            ("https://fsspx.today", "fsspx.today", True),
            ("https://sub.konrad.media", "konrad.media", True),
            ("https://konrad.media", "konrad.media", True),
            ("https://sub.epma.care", "epma.care", True),
            ("https://epma.care", "epma.care", True),
        ])
