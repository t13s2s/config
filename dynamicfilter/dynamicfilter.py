__all__ = ['map_query_results']

DEFAULT = "default"


def map_query_results(query_results):
    final_json = {}

    for row in query_results:

        _check_row_format(row)

        if _is_bidder_empty(final_json, row):
            if _is_continent_default_row(row):
                final_json = _build_bidder_continent_default(row)
            else:
                final_json |= _build_bidder_continent(row)
        elif _is_continent_empty(final_json, row):
            if _is_continent_default_row(row):
                final_json[row.bidder] |= _build_continent_2(row)
            else:
                final_json[row.bidder] |= _build_continent(row)
        elif _is_continent_default_row(row):
            final_json[row.bidder][row.continent] |= _build_continent_default(row)
        elif _is_country_empty(final_json, row):
            final_json[row.bidder][row.continent] |= _build_country(row)
        else:
            final_json[row.bidder][row.continent][row.country] |= _update_country_host(row)

    if len(final_json) == 0:
        raise Exception("Failed to map results: empty query result list")

    return final_json


def _is_continent_empty(final_json, row):
    return final_json.get(row.bidder).get(row.continent) is None


def _is_bidder_empty(final_json, row):
    return final_json.get(row.bidder) is None


def _check_row_format(row):
    if not row.bidder or not row.continent or not row.country or row.filter is None or row.filter < 0:
        raise Exception("Invalid row format: empty or negative column value is now allowed")
    if _is_continent_default_row(row) and row.host != DEFAULT:
        raise Exception("Invalid row format: default country and non default host isn't allowed: {0}".format(row.host))


def _is_continent_default_row(row):
    return row.country == DEFAULT


def _is_country_empty(final_json, row):
    return final_json.get(row.bidder).get(row.continent).get(row.country) is None


def _update_country_host(row):
    return {
        row.host: row.filter
    }


def _build_country(row):
    return {
        row.country: {
            row.host: row.filter
        }
    }


def _build_continent_default(row):
    return {
        DEFAULT: row.filter
    }


def _build_continent(row):
    return {
        row.continent: {
            row.country: {
                row.host: row.filter
            }
        }
    }


def _build_continent_2(row):
    return {
        row.continent: {
            DEFAULT: row.filter
        }
    }


def _build_bidder_continent_default(row):
    return {
        row.bidder: {
            row.continent: {
                DEFAULT: row.filter
            }
        }
    }


def _build_bidder_continent(row):
    return {
        row.bidder: {
            row.continent: {
                row.country: {
                    row.host: row.filter
                }
            }
        }
    }
