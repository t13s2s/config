import unittest

import dynamicfilter


class FunctionTestCase(unittest.TestCase):

    maxDiff = None

    def test_given_empty_result_throw_exception(self):
        # given
        stub = []

        # when-then
        with self.assertRaises(Exception) as e:
            dynamicfilter.map_query_results(stub)

        self.assertTrue("Failed to map results: empty query result list" in str(e.exception))

    def test_given_country_default_and_host_non_default_result_throw_exception(self):
        # given
        stub = [
            DynamicFilterQueryResult("adform", "AF", "default", "www.testing.com", 1.0),
        ]

        # when-then
        with self.assertRaises(Exception) as e:
            print(dynamicfilter.map_query_results(stub))

        self.assertTrue("Invalid row format: default country and non default host isn't allowed: "
                        "www.testing.com" in str(e.exception))

    def test_given_none_filter_value_result_throw_exception(self):
        # given
        stub = [
            DynamicFilterQueryResult("adform", "AF", "default", "www.testing.com", None)
        ]

        # when-then
        with self.assertRaises(Exception) as e:
            print(dynamicfilter.map_query_results(stub))

        self.assertTrue("Invalid row format: empty column value is now allowed")

    def test_given_empty_host_value_result_throw_exception(self):
        # given
        stub = [
            DynamicFilterQueryResult("adform", "AF", "default", "", 1.0)
        ]

        # when-then
        with self.assertRaises(Exception) as e:
            print(dynamicfilter.map_query_results(stub))

        self.assertTrue("Invalid row format: empty column value is now allowed")

    def test_given_default_continent_result_map_filters_correctly(self):
        # given
        stub = [
            DynamicFilterQueryResult("adform", "AF", "ZA", "www.aljazeera.com", 1.0),
            DynamicFilterQueryResult("adform", "AF", "default", "default", 0.2)
        ]

        expected_default_continent = {
            "adform": {
                "AF": {
                    "default": 0.2,
                    "ZA": {
                        "www.aljazeera.com": 1.0
                    }
                }
            }
        }

        # when
        results = dynamicfilter.map_query_results(stub)

        # then
        self.assertDictEqual(expected_default_continent, results)

    def test_given_default_continent_only_result_map_filters_correctly(self):
        # given
        stub = [
            DynamicFilterQueryResult("adform", "AF", "default", "default", 0.2)
        ]

        expected_default_continent = {
            "adform": {
                "AF": {
                    "default": 0.2
                }
            }
        }

        # when
        results = dynamicfilter.map_query_results(stub)

        # then
        self.assertDictEqual(expected_default_continent, results)

    def test_given_default_country_only_result_map_filters_correctly(self):
        # given
        stub = [
            DynamicFilterQueryResult("adform", "AF", "SA", "default", 0.1)
        ]

        expected_default_continent = {
            "adform": {
                "AF": {
                    "SA": {
                        "default": 0.1
                    }
                }
            }
        }

        # when
        results = dynamicfilter.map_query_results(stub)

        # then
        self.assertDictEqual(expected_default_continent, results)

    def test_given_default_country_result_map_filters_correctly(self):
        # given
        stub = [
            DynamicFilterQueryResult("adform", "AF", "ZA", "www.aljazeera.com", 1.0),
            DynamicFilterQueryResult("adform", "AF", "ZA", "default", 0.2)
        ]

        expected_default_continent = {
            "adform": {
                "AF": {
                    "ZA": {
                        "default": 0.2,
                        "www.aljazeera.com": 1.0
                    }
                }
            }
        }

        # when
        results = dynamicfilter.map_query_results(stub)

        # then
        self.assertDictEqual(expected_default_continent, results)

    def test_given_multiple_countries_result_map_filters_correctly(self):
        # given
        stub = [
            DynamicFilterQueryResult("adform", "AF", "LA", "default", 1.0),
            DynamicFilterQueryResult("adform", "AF", "ZA", "www.aljazeera.com", 1.0),
            DynamicFilterQueryResult("adform", "AF", "default", "default", 1.0),
            DynamicFilterQueryResult("adform", "AF", "ZA", "www.another-site.com", 0.5),
            DynamicFilterQueryResult("adform", "AF", "LA", "www.another-site.com", 0.5),
            DynamicFilterQueryResult("adform", "AF", "ZA", "default", 0.8)
        ]

        expected_results = {'adform': {
            'AF': {'LA': {'default': 1.0, 'www.another-site.com': 0.5},
                   'ZA': {'www.aljazeera.com': 1.0, 'www.another-site.com': 0.5, 'default': 0.8}, 'default': 1.0}
        }}

        # when
        results = dynamicfilter.map_query_results(stub)

        # then
        self.assertEqual(expected_results, results)

    def test_given_multiple_hosts_result_map_filters_correctly(self):
        # given
        stub = [
            DynamicFilterQueryResult("adform", "AF", "ZA", "default", 1.0),
            DynamicFilterQueryResult("adform", "AF", "ZA", "www.testing.com", 0.8),
            DynamicFilterQueryResult("adform", "AF", "SA", "www.another-test.com", 1),
            DynamicFilterQueryResult("adform", "AF", "SA", "www.aljazeera.com", 0.2)
        ]

        expected_default_continent = {
            "adform": {
                "AF": {
                    "ZA": {
                        "default": 1.0,
                        "www.testing.com": 0.8
                    },
                    "SA": {
                        "www.another-test.com": 1.0,
                        "www.aljazeera.com": 0.2
                    }
                }
            }
        }

        # when
        results = dynamicfilter.map_query_results(stub)

        # then
        self.assertDictEqual(expected_default_continent, results)

    def test_given_multiple_bidders_result_map_filters_correctly(self):
        # given
        stub = [
            DynamicFilterQueryResult("adform", "AF", "ZA", "default", 1.0),
            DynamicFilterQueryResult("adform", "AF", "default", "default", 0.2),
            DynamicFilterQueryResult("adform", "AF", "ZA", "www.testing.com", 0.8),
            DynamicFilterQueryResult("adform", "AF", "SA", "www.another-test.com", 1),
            DynamicFilterQueryResult("adform", "SA", "BR", "www.another-test.com", 1),
            DynamicFilterQueryResult("adform", "SA", "BR", "default", 0.5),
            DynamicFilterQueryResult("another-bidder", "SA", "BR", "www.testing123.com", 0.2),
            DynamicFilterQueryResult("another-bidder", "AF", "ZA", "default", 1.0),
            DynamicFilterQueryResult("another-bidder", "AF", "default", "default", 0.2),
            DynamicFilterQueryResult("another-bidder", "AF", "ZA", "www.testing.com", 0.8),
            DynamicFilterQueryResult("another-bidder", "AF", "SA", "www.another-test.com", 1),
            DynamicFilterQueryResult("another-bidder", "SA", "default", "default", 0.9),
            DynamicFilterQueryResult("another-bidder", "SA", "BR", "www.another-test.com", 1),
            DynamicFilterQueryResult("another-bidder", "SA", "BR", "default", 0.5),
        ]

        expected_default_continent = {
            "adform": {
                "AF": {
                    "default": 0.2,
                    "ZA": {
                        "default": 1.0,
                        "www.testing.com": 0.8
                    },
                    "SA": {
                        "www.another-test.com": 1.0
                    }
                },
                "SA": {
                    "BR": {
                        "default": 0.5,
                        "www.another-test.com": 1.0
                    }
                }
            },
            "another-bidder": {
                "AF": {
                    "default": 0.2,
                    "ZA": {
                        "default": 1.0,
                        "www.testing.com": 0.8
                    },
                    "SA": {
                        "www.another-test.com": 1.0
                    }
                },
                "SA": {
                    "default": 0.9,
                    "BR": {
                        "default": 0.5,
                        "www.another-test.com": 1.0,
                        "www.testing123.com": 0.2
                    }
                }
            }
        }

        # when
        results = dynamicfilter.map_query_results(stub)

        # then
        self.assertDictEqual(expected_default_continent, results)


class DynamicFilterQueryResult:
    def __init__(self, bidder, continent, country, host, filter):
        self.bidder = bidder
        self.continent = continent
        self.country = country
        self.host = host
        self.filter = filter


if __name__ == '__main__':
    unittest.main()
