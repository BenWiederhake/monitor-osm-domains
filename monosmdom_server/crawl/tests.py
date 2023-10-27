from crawl import logic, models
from django.test import TestCase, TransactionTestCase
from monosmdom_server import common
import datetime
import storage


# If any of the following tests takes longer than this, then something has *seriously* gone wrong anyway.
TEST_DATETIME_EPSILON = datetime.timedelta(seconds=1)


def old_date(extra_days=1):
    return common.now_tzaware() - datetime.timedelta(days=logic.CRAWL_DOMAIN_DELAY_DAYS + extra_days)


class PickCrawlableUrlTests(TransactionTestCase):
    def assertRecentDomain(self, domain):
        self.assertIsNotNone(domain.last_contacted, domain)
        upper_bound = common.now_tzaware()
        lower_bound = upper_bound - TEST_DATETIME_EPSILON
        self.assertTrue(lower_bound <= domain.last_contacted <= upper_bound, (domain, domain.last_contacted, upper_bound))

    def assertOldDomain(self, domain, extra_days):
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
        some_domain = storage.models.Domain.objects.create(domain_name="foo.com")
        storage.models.Url.objects.create(url="httpss./invalid::////:::")
        picked_crurl = logic.pick_and_bump_random_crawlable_url()
        self.assertIsNone(picked_crurl)

    def test_too_recent_url(self):
        some_domain = storage.models.Domain.objects.create(domain_name="foo.com", last_contacted=common.now_tzaware())
        some_url = storage.models.Url.objects.create(url="https://foo.com/bar/baz")
        some_crurl = storage.models.CrawlableUrl.objects.create(url=some_url, domain=some_domain)
        picked_crurl = logic.pick_and_bump_random_crawlable_url()
        self.assertIsNone(picked_crurl)

    def test_happy_single(self):
        some_domain = storage.models.Domain.objects.create(domain_name="foo.com")
        some_url = storage.models.Url.objects.create(url="https://foo.com/bar/baz")
        some_crurl = storage.models.CrawlableUrl.objects.create(url=some_url, domain=some_domain)
        picked_crurl = logic.pick_and_bump_random_crawlable_url()
        self.assertEqual(picked_crurl, some_crurl)
        some_domain.refresh_from_db()
        self.assertRecentDomain(some_domain)

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
        self.assertRecentDomain(some_domain)
        self.assertOldDomain(other_domain, 1)

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
        self.assertRecentDomain(other_domain)
        self.assertOldDomain(some_domain, 1)

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
        self.assertRecentDomain(some_domain)
        self.assertOldDomain(other_domain, 1)

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
        self.assertRecentDomain(other_domain)
        self.assertOldDomain(some_domain, 1)

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

