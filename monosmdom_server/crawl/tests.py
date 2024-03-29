from collections import Counter
from crawl import logic, models
from django.conf import settings
from django.test import TestCase, TransactionTestCase
from monosmdom_server import common
import brotli
import datetime
import storage


# If any of the following tests takes longer than this, then something has *seriously* gone wrong anyway.
TEST_DATETIME_EPSILON = datetime.timedelta(seconds=1)


def old_date(extra_days=1):
    return common.now_tzaware() - datetime.timedelta(days=logic.CRAWL_DOMAIN_DELAY_DAYS + extra_days)


class PickCrawlableUrlTests(TransactionTestCase):
    def assert_recent_domain(self, domain):
        self.assertIsNotNone(domain.last_contacted, domain)
        upper_bound = common.now_tzaware()
        lower_bound = upper_bound - TEST_DATETIME_EPSILON
        self.assertTrue(lower_bound <= domain.last_contacted <= upper_bound, (domain, domain.last_contacted, upper_bound))

    def assert_old_domain(self, domain, extra_days):
        self.assertIsNotNone(domain.last_contacted, domain)
        upper_bound = old_date(extra_days)
        lower_bound = upper_bound - TEST_DATETIME_EPSILON
        self.assertTrue(lower_bound <= domain.last_contacted <= upper_bound, (domain, domain.last_contacted, upper_bound))

    def test_empty(self):
        picked_crurl = logic.pick_and_bump_random_crawlable_url()
        self.assertIsNone(picked_crurl)

    def test_just_untouched_domain(self):
        storage.models.Domain.objects.create(domain_name="foo.com")
        picked_crurl = logic.pick_and_bump_random_crawlable_url()
        self.assertIsNone(picked_crurl)

    def test_just_old_domain(self):
        storage.models.Domain.objects.create(domain_name="foo.com", last_contacted=old_date())
        picked_crurl = logic.pick_and_bump_random_crawlable_url()
        self.assertIsNone(picked_crurl)

    def test_just_fresh_domain(self):
        storage.models.Domain.objects.create(domain_name="foo.com", last_contacted=common.now_tzaware())
        picked_crurl = logic.pick_and_bump_random_crawlable_url()
        self.assertIsNone(picked_crurl)

    def test_just_passive_url(self):
        _some_domain = storage.models.Domain.objects.create(domain_name="foo.com")
        storage.models.Url.objects.create(url="httpss./invalid::////:::")
        picked_crurl = logic.pick_and_bump_random_crawlable_url()
        self.assertIsNone(picked_crurl)

    def test_too_recent_url(self):
        some_domain = storage.models.Domain.objects.create(domain_name="foo.com", last_contacted=common.now_tzaware())
        some_url = storage.models.Url.objects.create(url="https://foo.com/bar/baz")
        _some_crurl = storage.models.CrawlableUrl.objects.create(url=some_url, domain=some_domain)
        picked_crurl = logic.pick_and_bump_random_crawlable_url()
        self.assertIsNone(picked_crurl)

    def test_happy_single(self):
        some_domain = storage.models.Domain.objects.create(domain_name="foo.com")
        some_url = storage.models.Url.objects.create(url="https://foo.com/bar/baz")
        some_crurl = storage.models.CrawlableUrl.objects.create(url=some_url, domain=some_domain)
        picked_crurl = logic.pick_and_bump_random_crawlable_url()
        self.assertEqual(picked_crurl, some_crurl)
        some_domain.refresh_from_db()
        self.assert_recent_domain(some_domain)

    def test_happy_null_domain1(self):
        some_domain = storage.models.Domain.objects.create(domain_name="foo.com")
        some_url = storage.models.Url.objects.create(url="https://foo.com/bar/baz")
        some_crurl = storage.models.CrawlableUrl.objects.create(url=some_url, domain=some_domain)
        other_domain = storage.models.Domain.objects.create(domain_name="quux.com", last_contacted=old_date(1))
        other_url = storage.models.Url.objects.create(url="https://quux.com/bar/baz")
        other_crurl = storage.models.CrawlableUrl.objects.create(url=other_url, domain=other_domain)
        picked_crurl = logic.pick_and_bump_random_crawlable_url()
        self.assertEqual(picked_crurl, some_crurl)
        self.assertNotEqual(picked_crurl, other_crurl)
        some_domain.refresh_from_db()
        other_domain.refresh_from_db()
        self.assert_recent_domain(some_domain)
        self.assert_old_domain(other_domain, 1)

    def test_happy_null_domain2(self):
        # Insertion order might matter.
        # To make super duper sure that the domain is chosen for the right reason, try both possible orderings.
        some_domain = storage.models.Domain.objects.create(domain_name="foo.com", last_contacted=old_date(1))
        some_url = storage.models.Url.objects.create(url="https://foo.com/bar/baz")
        some_crurl = storage.models.CrawlableUrl.objects.create(url=some_url, domain=some_domain)
        other_domain = storage.models.Domain.objects.create(domain_name="quux.com")
        other_url = storage.models.Url.objects.create(url="https://quux.com/bar/baz")
        other_crurl = storage.models.CrawlableUrl.objects.create(url=other_url, domain=other_domain)
        picked_crurl = logic.pick_and_bump_random_crawlable_url()
        self.assertEqual(picked_crurl, other_crurl)
        self.assertNotEqual(picked_crurl, some_crurl)
        some_domain.refresh_from_db()
        other_domain.refresh_from_db()
        self.assert_recent_domain(other_domain)
        self.assert_old_domain(some_domain, 1)

    def test_happy_old_domain1(self):
        some_domain = storage.models.Domain.objects.create(domain_name="foo.com", last_contacted=old_date(2))
        some_url = storage.models.Url.objects.create(url="https://foo.com/bar/baz")
        some_crurl = storage.models.CrawlableUrl.objects.create(url=some_url, domain=some_domain)
        other_domain = storage.models.Domain.objects.create(domain_name="quux.com", last_contacted=old_date(1))
        other_url = storage.models.Url.objects.create(url="https://quux.com/bar/baz")
        other_crurl = storage.models.CrawlableUrl.objects.create(url=other_url, domain=other_domain)
        picked_crurl = logic.pick_and_bump_random_crawlable_url()
        self.assertEqual(picked_crurl, some_crurl)
        self.assertNotEqual(picked_crurl, other_crurl)
        some_domain.refresh_from_db()
        other_domain.refresh_from_db()
        self.assert_recent_domain(some_domain)
        self.assert_old_domain(other_domain, 1)

    def test_happy_old_domain2(self):
        # Insertion order might matter.
        # To make super duper sure that the domain is chosen for the right reason, try both possible orderings.
        some_domain = storage.models.Domain.objects.create(domain_name="foo.com", last_contacted=old_date(1))
        some_url = storage.models.Url.objects.create(url="https://foo.com/bar/baz")
        some_crurl = storage.models.CrawlableUrl.objects.create(url=some_url, domain=some_domain)
        other_domain = storage.models.Domain.objects.create(domain_name="quux.com", last_contacted=old_date(2))
        other_url = storage.models.Url.objects.create(url="https://quux.com/bar/baz")
        other_crurl = storage.models.CrawlableUrl.objects.create(url=other_url, domain=other_domain)
        picked_crurl = logic.pick_and_bump_random_crawlable_url()
        self.assertEqual(picked_crurl, other_crurl)
        self.assertNotEqual(picked_crurl, some_crurl)
        some_domain.refresh_from_db()
        other_domain.refresh_from_db()
        self.assert_recent_domain(other_domain)
        self.assert_old_domain(some_domain, 1)

    def test_happy_null_result1(self):
        some_domain = storage.models.Domain.objects.create(domain_name="foo.com")
        some_url = storage.models.Url.objects.create(url="https://foo.com/1")
        some_crurl = storage.models.CrawlableUrl.objects.create(url=some_url, domain=some_domain)
        other_url = storage.models.Url.objects.create(url="https://foo.com/2")
        other_crurl = storage.models.CrawlableUrl.objects.create(url=other_url, domain=some_domain)
        models.Result.objects.create(url=other_url, crawl_begin=old_date())
        picked_crurl = logic.pick_and_bump_random_crawlable_url()
        self.assertEqual(picked_crurl, some_crurl)
        self.assertNotEqual(picked_crurl, other_crurl)

    def test_happy_null_result2(self):
        some_domain = storage.models.Domain.objects.create(domain_name="foo.com")
        some_url = storage.models.Url.objects.create(url="https://foo.com/1")
        some_crurl = storage.models.CrawlableUrl.objects.create(url=some_url, domain=some_domain)
        models.Result.objects.create(url=some_url, crawl_begin=old_date())
        other_url = storage.models.Url.objects.create(url="https://foo.com/2")
        other_crurl = storage.models.CrawlableUrl.objects.create(url=other_url, domain=some_domain)
        picked_crurl = logic.pick_and_bump_random_crawlable_url()
        self.assertEqual(picked_crurl, other_crurl)
        self.assertNotEqual(picked_crurl, some_crurl)

    def test_happy_old_result1(self):
        some_domain = storage.models.Domain.objects.create(domain_name="foo.com")
        some_url = storage.models.Url.objects.create(url="https://foo.com/1")
        some_crurl = storage.models.CrawlableUrl.objects.create(url=some_url, domain=some_domain)
        models.Result.objects.create(url=some_url, crawl_begin=old_date(2))
        other_url = storage.models.Url.objects.create(url="https://foo.com/2")
        other_crurl = storage.models.CrawlableUrl.objects.create(url=other_url, domain=some_domain)
        models.Result.objects.create(url=other_url, crawl_begin=old_date(1))
        picked_crurl = logic.pick_and_bump_random_crawlable_url()
        self.assertEqual(picked_crurl, some_crurl)
        self.assertNotEqual(picked_crurl, other_crurl)

    def test_happy_old_result2(self):
        some_domain = storage.models.Domain.objects.create(domain_name="foo.com")
        some_url = storage.models.Url.objects.create(url="https://foo.com/1")
        some_crurl = storage.models.CrawlableUrl.objects.create(url=some_url, domain=some_domain)
        models.Result.objects.create(url=some_url, crawl_begin=old_date(1))
        other_url = storage.models.Url.objects.create(url="https://foo.com/2")
        other_crurl = storage.models.CrawlableUrl.objects.create(url=other_url, domain=some_domain)
        models.Result.objects.create(url=other_url, crawl_begin=old_date(2))
        picked_crurl = logic.pick_and_bump_random_crawlable_url()
        self.assertEqual(picked_crurl, other_crurl)
        self.assertNotEqual(picked_crurl, some_crurl)


class LossyCompressionTests(TestCase):
    def assert_compresses_at_least(self, content_bytes, max_length, saved_bytes_min, saved_bytes_max):
        with self.subTest(content_bytes=content_bytes):
            compressed_bytes = logic.compress_lossy(content_bytes, max_length)
            self.assertLessEqual(len(compressed_bytes), max_length)
            uncompressed_bytes = brotli.decompress(compressed_bytes)
            self.assertTrue(content_bytes.startswith(uncompressed_bytes), f"Decompression result {uncompressed_bytes} is not a prefix of the original input {content_bytes}?!")
            self.assertGreaterEqual(len(uncompressed_bytes), saved_bytes_min)
            self.assertLessEqual(len(uncompressed_bytes), saved_bytes_max)

    def test_simple(self):
        for length in range(30):
            with self.subTest(length=length):
                self.assert_compresses_at_least(b"a" * length, 10, length, length)

    def test_bytearray(self):
        self.assert_compresses_at_least(bytearray(b"asdfasdfasdfasdf"), 14, 16, 16)

    def test_random(self):
        random_bytes = b"l\xf6\x8f\xa7\xb9\x7f\xb9yi\x81\x0c%(\x0f4.\x0b\xfcL\xc6\xc9\xb6\xa1\xe9\xd8\xf5\xc2\xe9\x15-0v"
        max_savable = 26  # Depends on random_bytes.
        for length in range(len(random_bytes)):
            expect_bytes = min(length, max_savable)
            with self.subTest(length=length):
                self.assert_compresses_at_least(random_bytes[:length], 30, expect_bytes, expect_bytes)

    def test_real_tiny_caddy(self):
        header = b'Accept-Ranges: bytes\nContent-Length: 2840\nContent-Type: text/html; charset=utf-8\nEtag: "rycylg26w"\nLast-Modified: Tue, 25 Jul 2023 15:20:04 GMT\nServer: Caddy\nDate: Fri, 27 Oct 2023 02:08:02 GMT'
        assert 193 == len(header)
        # …
        # header[:171] compresses to 130 bytes
        # header[:172] compresses to 130 bytes
        # header[:173] compresses to 132 bytes
        # header[:174] compresses to 134 bytes
        # header[:175] compresses to 133 bytes
        # header[:176] compresses to 134 bytes
        # header[:177] compresses to 135 bytes
        # header[:178] compresses to 137 bytes
        # header[:179] compresses to 134 bytes
        # header[:180] compresses to 134 bytes
        # header[:181] compresses to 134 bytes
        # header[:182] compresses to 135 bytes
        # header[:183] compresses to 136 bytes
        # header[:184] compresses to 137 bytes
        # header[:185] compresses to 138 bytes
        # header[:186] compresses to 140 bytes
        # header[:187] compresses to 140 bytes
        # header[:188] compresses to 141 bytes
        # header[:189] compresses to 143 bytes
        # header[:190] compresses to 153 bytes
        # header[:191] compresses to 144 bytes
        # header[:192] compresses to 144 bytes
        # header[:193] compresses to 141 bytes
        target_to_saved = {
            133: 173,  # or 175
            134: 181,  # or 174
            135: 182,  # or 177
            136: 183,  # or 177
            # Up until here, the determined value actually depends on which part the bisecting
            # process "randomly" (deterministic but hard to predict) hits.
            137: 184,
            138: 185,
            139: 185,
            140: 187,
            141: 193,
        }
        for target_length, saved_length in target_to_saved.items():
            with self.subTest(target_length=target_length, saved_length=saved_length):
                self.assert_compresses_at_least(header, target_length, saved_length, saved_length)

    def test_real_large_sedo_header(self):
        with open(settings.BASE_DIR / "crawl/testfiles/sedoparking.header.raw", "rb") as fp:
            header = fp.read()
        assert 619 == len(header)
        target_to_saved = {
            133: 202,  # or 198, or 195
            134: 202,  # or 199
            135: 203,  # or 199
            136: 204,  # deterministic!
            137: 204,  # deterministic!
            138: 206,  # 204
            139: 211,  # 207, 204
            140: 212,  # or 207, 204
            141: 216,  # or 213, or 208
        }
        for target_length, saved_length in target_to_saved.items():
            with self.subTest(target_length=target_length, saved_length=saved_length):
                self.assert_compresses_at_least(header, target_length, saved_length, saved_length)

    def test_real_large_sedo_body(self):
        with open(settings.BASE_DIR / "crawl/testfiles/sedoparking.html.br", "rb") as fp:
            body_compressed = fp.read()
        body = brotli.decompress(body_compressed)
        assert 23223 == len(body)
        # Take a look at how non-linear the compression length is:
        # len(compress(data[15319])) = 4073
        # len(compress(data[15320])) = 4098
        # len(compress(data[15321])) = 4089
        # … (low)
        # len(compress(data[15337])) = 4071
        # len(compress(data[15338])) = 4101
        # len(compress(data[15339])) = 4093
        # … (low)
        # len(compress(data[15352])) = 4094
        # len(compress(data[15353])) = 4098
        # len(compress(data[15354])) = 4076
        # len(compress(data[15355])) = 4107
        # len(compress(data[15356])) = 4108
        # len(compress(data[15357])) = 4087
        # len(compress(data[15358])) = 4112
        # … (high)
        # len(compress(data[15375])) = 4110
        # len(compress(data[15376])) = 4094
        # len(compress(data[15377])) = 4089
        # len(compress(data[15378])) = 4117
        # … (high)
        # len(compress(data[15384])) = 4125
        # len(compress(data[15385])) = 4090
        # … (low)
        # len(compress(data[15410])) = 4090
        # len(compress(data[15411])) = 4128
        # … (high)
        # len(compress(data[15414])) = 4128
        # len(compress(data[15415])) = 4094
        # len(compress(data[15416])) = 4093
        # len(compress(data[15417])) = 4121
        # … (rest is too high)
        # So we might store either 15319 bytes, 15416, or something "random" in between.
        self.assert_compresses_at_least(body, 4096, 15319, 15416)
        # That's quite a range! Thankfully, our naive bisecting seems to "hit in the middle":
        self.assert_compresses_at_least(body, 4096, 15352, 15352)
        self.assert_compresses_at_least(body, 1024, 2357, 2367)
        # Again, pretty much the middle:
        self.assert_compresses_at_least(body, 1024, 2364, 2364)


class UploadRegexAndFunctionAgree(TestCase):
    def test_simple(self):
        # Since this will randomly fail if it ever fails, it has to be called repeatedly.
        # Let's say it fails with probability p.
        # If p is less than 1e-3, the additional work done by failed crawl attempts is non-catastrophic.
        # If p is 1e-3 or larger, then the probability to be detected in 10_000 trials is 99.995%:
        #    (1 - 1/1000) ** 1000 = 1/e, "approximately" (accurate up to <1ULP)
        #    1 - (1 - 1/1000) ** 10000 = 1 - (1/e)**10 = 0.9999546…
        lengths = Counter()
        for i in range(10_000):
            # Doing this in subtests allows us to gauge how bad the situation is, should this ever fail:
            path = models.user_directory_path(None, None)
            lengths[len(path)] += 1
            with self.subTest(i=i, path=path):
                self.assertRegex(path, models.USER_DIRECTORY_PATH_REGEX)
        # Check that all paths have identical length:
        self.assertEqual(len(lengths), 1, lengths)


class CrawlProcessTests(TransactionTestCase):
    def test_golden(self):
        some_domain = storage.models.Domain.objects.create(domain_name="foo.com")
        some_url = storage.models.Url.objects.create(url="https://foo.com/")
        _some_crurl = storage.models.CrawlableUrl.objects.create(url=some_url, domain=some_domain)
        grabbed_result = None
        crawl_content = b"Welcome to my wonderful website!"
        with logic.CrawlProcess(some_url) as process:
            grabbed_result = process.result
            self.assertQuerySetEqual(models.Result.objects.all(), [grabbed_result])
            self.assertQuerySetEqual(models.ResultSuccess.objects.all(), [])
            self.assertQuerySetEqual(models.ResultError.objects.all(), [])
            self.assertIsNone(grabbed_result.crawl_end)
            # This creates a real file in the real MEDIA_ROOT :(
            result_success = process.submit_success(234, b"vary: None", 10, False, crawl_content, 32, False, None)
            self.assertQuerySetEqual(models.Result.objects.all(), [grabbed_result])
            self.assertQuerySetEqual(models.ResultSuccess.objects.all(), [result_success])
            self.assertQuerySetEqual(models.ResultError.objects.all(), [])
            self.assertIsNotNone(grabbed_result.crawl_end)
        self.assertQuerySetEqual(models.Result.objects.all(), [grabbed_result])
        self.assertQuerySetEqual(models.ResultSuccess.objects.all(), [result_success])
        self.assertQuerySetEqual(models.ResultError.objects.all(), [])
        self.assertIsNotNone(grabbed_result.crawl_end)
        result_success_from_db = models.ResultSuccess.objects.all().get()
        crawl_content_compressed = result_success_from_db.content_file.read()
        self.assertEqual(brotli.decompress(crawl_content_compressed), crawl_content)
        self.assertEqual(result_success_from_db.headers_orig_size, 10)
        self.assertEqual(result_success_from_db.content_orig_size, 32)
        self.assertLess(len(crawl_content_compressed), len(crawl_content))
        # Try to clean up the file from MEDIA_ROOT:
        result_success_from_db.content_file.delete(save=False)
        # Note that this leaves empty dirs behind. Sigh.

    def test_golden_truncated(self):
        some_domain = storage.models.Domain.objects.create(domain_name="foo.com")
        some_url = storage.models.Url.objects.create(url="https://foo.com/")
        _some_crurl = storage.models.CrawlableUrl.objects.create(url=some_url, domain=some_domain)
        grabbed_result = None
        crawl_content = b"Welcome to my wonderful website!" * 2
        with logic.CrawlProcess(some_url) as process:
            grabbed_result = process.result
            self.assertQuerySetEqual(models.Result.objects.all(), [grabbed_result])
            self.assertQuerySetEqual(models.ResultSuccess.objects.all(), [])
            self.assertQuerySetEqual(models.ResultError.objects.all(), [])
            self.assertIsNone(grabbed_result.crawl_end)
            # This creates a real file in the real MEDIA_ROOT :(
            result_success = process.submit_success(234, b"vary: None", 10, False, crawl_content, 999, True, None)
            self.assertQuerySetEqual(models.Result.objects.all(), [grabbed_result])
            self.assertQuerySetEqual(models.ResultSuccess.objects.all(), [result_success])
            self.assertQuerySetEqual(models.ResultError.objects.all(), [])
            self.assertIsNotNone(grabbed_result.crawl_end)
        self.assertQuerySetEqual(models.Result.objects.all(), [grabbed_result])
        self.assertQuerySetEqual(models.ResultSuccess.objects.all(), [result_success])
        self.assertQuerySetEqual(models.ResultError.objects.all(), [])
        self.assertIsNotNone(grabbed_result.crawl_end)
        result_success_from_db = models.ResultSuccess.objects.all().get()
        crawl_content_compressed = result_success_from_db.content_file.read()
        self.assertEqual(brotli.decompress(crawl_content_compressed), crawl_content)
        self.assertEqual(result_success_from_db.headers_orig_size, 10)
        self.assertEqual(result_success_from_db.content_orig_size, -999)
        self.assertLess(len(crawl_content_compressed), len(crawl_content))
        # Try to clean up the file from MEDIA_ROOT:
        result_success_from_db.content_file.delete(save=False)
        # Note that this leaves empty dirs behind. Sigh.

    def test_curl_error(self):
        some_domain = storage.models.Domain.objects.create(domain_name="foo.com")
        some_url = storage.models.Url.objects.create(url="https://foo.com/")
        _some_crurl = storage.models.CrawlableUrl.objects.create(url=some_url, domain=some_domain)
        grabbed_result = None
        with logic.CrawlProcess(some_url) as process:
            grabbed_result = process.result
            self.assertQuerySetEqual(models.Result.objects.all(), [grabbed_result])
            self.assertQuerySetEqual(models.ResultSuccess.objects.all(), [])
            self.assertQuerySetEqual(models.ResultError.objects.all(), [])
            self.assertIsNone(grabbed_result.crawl_end)
            process.submit_error({
                "errcode": 1234,
                "errstr": "could not avoid connecting to the wrong non-domain",
                "response_code": 567,
                "header_size_recv": 0,
                "body_size_recv": 1,
            })
            self.assertQuerySetEqual(models.Result.objects.all(), [grabbed_result])
            self.assertQuerySetEqual(models.ResultSuccess.objects.all(), [])
            self.assertEqual(models.ResultError.objects.count(), 1)
            self.assertIsNotNone(grabbed_result.crawl_end)
        self.assertQuerySetEqual(models.Result.objects.all(), [grabbed_result])
        self.assertQuerySetEqual(models.ResultSuccess.objects.all(), [])
        self.assertEqual(models.ResultError.objects.count(), 1)
        self.assertIsNotNone(grabbed_result.crawl_end)
        result_error_from_db = models.ResultError.objects.all().get()
        self.assertRegex(result_error_from_db.description_json, '"could not avoid connecting to the wrong non-domain"')
        self.assertRegex(result_error_from_db.description_json, '"curl_error"')
        self.assertFalse(result_error_from_db.is_internal_error)

    def test_missing(self):
        some_domain = storage.models.Domain.objects.create(domain_name="foo.com")
        some_url = storage.models.Url.objects.create(url="https://foo.com/")
        _some_crurl = storage.models.CrawlableUrl.objects.create(url=some_url, domain=some_domain)
        grabbed_result = None
        with logic.CrawlProcess(some_url) as process:
            grabbed_result = process.result
            self.assertQuerySetEqual(models.Result.objects.all(), [grabbed_result])
            self.assertQuerySetEqual(models.ResultSuccess.objects.all(), [])
            self.assertQuerySetEqual(models.ResultError.objects.all(), [])
            self.assertIsNone(grabbed_result.crawl_end)
            # And then … we just "forget" to call submit! D:
        self.assertQuerySetEqual(models.Result.objects.all(), [grabbed_result])
        self.assertQuerySetEqual(models.ResultSuccess.objects.all(), [])
        self.assertEqual(len(models.ResultError.objects.all()), 1)
        self.assertIsNotNone(grabbed_result.crawl_end)
        result_error_from_db = models.ResultError.objects.all().get()
        self.assertRegex(result_error_from_db.description_json, '"None"')
        self.assertRegex(result_error_from_db.description_json, '"exception_or_missing_submit"')
        self.assertTrue(result_error_from_db.is_internal_error)

    def test_exception(self):
        some_domain = storage.models.Domain.objects.create(domain_name="foo.com")
        some_url = storage.models.Url.objects.create(url="https://foo.com/")
        _some_crurl = storage.models.CrawlableUrl.objects.create(url=some_url, domain=some_domain)
        grabbed_result = None
        got_rethrown = False
        try:
            with logic.CrawlProcess(some_url) as process:
                grabbed_result = process.result
                self.assertQuerySetEqual(models.Result.objects.all(), [grabbed_result])
                self.assertQuerySetEqual(models.ResultSuccess.objects.all(), [])
                self.assertQuerySetEqual(models.ResultError.objects.all(), [])
                self.assertIsNone(grabbed_result.crawl_end)
                # Oh noes, an exception! D:
                raise ValueError("Let's pretend something went wrong")
        except ValueError as e:
            assert e.args == ("Let's pretend something went wrong",)
            got_rethrown = True
        self.assertQuerySetEqual(models.Result.objects.all(), [grabbed_result])
        self.assertQuerySetEqual(models.ResultSuccess.objects.all(), [])
        self.assertTrue(got_rethrown)
        self.assertEqual(len(models.ResultError.objects.all()), 1)
        self.assertIsNotNone(grabbed_result.crawl_end)
        result_error_from_db = models.ResultError.objects.all().get()
        self.assertRegex(result_error_from_db.description_json, r'ValueError\(\\"Let\'s pretend something went wrong\\"\)')
        self.assertRegex(result_error_from_db.description_json, '"exception_or_missing_submit"')
        self.assertTrue(result_error_from_db.is_internal_error)
