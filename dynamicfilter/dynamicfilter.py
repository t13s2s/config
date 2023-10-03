__all__ = ['map_query_results']

DEFAULT = "default"


def map_query_results(query_results):
    final_json = {}

    if len(query_results) == 0:
        raise Exception("Failed to map results: empty query result list")

    for row in query_results:

        _check_row_format(row)

        if _is_bidder_empty(final_json, row):
            if _is_continent_default_row(row):
                final_json = _build_bidder_continent_default(row)
            else:
                final_json |= _build_bidder_continent(row)
        elif _is_continent_default_row(row):
            final_json[row.Bidder][row.Continent] |= _build_continent_default(row)
        elif _is_continent_empty(final_json, row):
            final_json[row.Bidder] |= _build_continent(row)
        elif _is_country_empty(final_json, row):
            final_json[row.Bidder][row.Continent] |= _build_country(row)
        else:
            final_json[row.Bidder][row.Continent][row.Country] |= _update_country_host(row)

    return final_json


def _is_continent_empty(final_json, row):
    return final_json.get(row.Bidder).get(row.Continent) is None


def _is_bidder_empty(final_json, row):
    return final_json.get(row.Bidder) is None


def _check_row_format(row):
    if not row.Bidder or not row.Continent or not row.Country or not row.Filter:
        raise Exception("Invalid row format: empty column value is now allowed")
    if _is_continent_default_row(row) and row.Host != DEFAULT:
        raise Exception("Invalid row format: default country and non default host isn't allowed: {0}".format(row.Host))


def _is_continent_default_row(row):
    return row.Country == DEFAULT


def _is_country_empty(final_json, row):
    return final_json.get(row.Bidder).get(row.Continent).get(row.Country) is None


def _update_country_host(row):
    return {
        row.Host: row.Filter
    }


def _build_country(row):
    return {
        row.Country: {
            row.Host: row.Filter
        }
    }


def _build_continent_default(row):
    return {
        DEFAULT: row.Filter
    }


def _build_continent(row):
    return {
        row.Continent: {
            row.Country: {
                row.Host: row.Filter
            }
        }
    }


def _build_bidder_continent_default(row):
    return {
        row.Bidder: {
            row.Continent: {
                DEFAULT: row.Filter
            }
        }

    }


def _build_bidder_continent(row):
    return {
        row.Bidder: {
            row.Continent: {
                row.Country: {
                    row.Host: row.Filter
                }
            }
        }
    }
