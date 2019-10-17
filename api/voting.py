from . import explorer
from .utils import needs_es
from services.cache import cache

from flask import abort
from elasticsearch_dsl import Search, Q
from services.bitshares_elasticsearch_client import es
from datetime import datetime, timedelta


def make_equal_time_intervals(from_date, to_date, datapoints):
    """
    Creates equal time intervals for partial es querries
    :param from_date:
    :type from_date:
    :param to_date:
    :type to_date:
    :param datapoints:
    :type datapoints:
    """

    datetime_format = "%Y-%m-%d"

    datetime_from = datetime.strptime(from_date, datetime_format)
    datetime_to = datetime.strptime(to_date, datetime_format)

    time_window_days = (datetime_to - datetime_from).days / datapoints
    if time_window_days == 0:
        time_window_days = 1
    delta_between_points = timedelta(days=time_window_days)

    datetime_intervals = []
    # extend the points by one to include a point around 'from_date' (if present)
    datetime_val = datetime_from - 2 * delta_between_points
    for i in range(datapoints + 1):  # @UnusedVariable
        datetime_val += delta_between_points
        datetime_val_str = datetime_val.strftime(datetime_format)
        datetime_intervals.append(datetime_val_str)

    return datetime_intervals


def calc_total_power(field):
    total_power = 0
    for id_to_power in field:
        total_power += int(int(id_to_power[1]) / 100000)
    return total_power


def fill_sub_power(field, storage, block_counter, datapoints):
    for id_to_power in field:
        _id = id_to_power[0]
        power = int(int(id_to_power[1]) / 100000)
        if _id not in storage:
            # precreate storage with max size
            storage[_id] = [0] * datapoints

        # add the power to the corresponding proxy/voter (id) at the block_counter
        storage[_id][block_counter] = power

    return storage


def get_self_power(hit):
    if hit["proxy"] == "1.2.5":
        return int(int(hit["stake"]) / 100000)
    return 0


def merge_fields_below_percentage(
        merge_below_percentage,
        size_successful_queries,
        datapoints,
        storage,
        optional_storage
):

    if size_successful_queries == 0:
        return []

    size_not_found_elements = datapoints - size_successful_queries
    index_to_delete_begin = datapoints - size_not_found_elements
    index_to_delete_end = datapoints
    last_block = size_successful_queries - 1  # last available element

    # we preallocated a bigger size, now be delete the not needed part
    for power in storage.values():
        del power[index_to_delete_begin:index_to_delete_end]

    total_power_last_block = optional_storage[last_block] if optional_storage is not None else 0
    for power in storage.values():
        total_power_last_block += power[last_block]

    merge_below_abs = int(merge_below_percentage * total_power_last_block / 100)

    merged_elements = [0] * size_successful_queries
    to_delete = []
    for _id, power in storage.items():
        if power[last_block] < merge_below_abs:
            for i in range(size_successful_queries):
                merged_elements[i] += power[i]
            to_delete.append(_id)

    storage["< " + str(merge_below_percentage) + "%"] = merged_elements

    for _id in to_delete:
        del storage[_id]

    # sort all proxies by last item size
    def sort_by_last_item_asc(powers):
        return powers[1][-1]

    # convert to list to be sortable
    storage_list = [[k, v] for k, v in storage.items()]
    storage_list.sort(key=sort_by_last_item_asc)

    return storage_list


@needs_es()
@cache.memoize()
def _get_account_power(from_date, to_date, account, datapoints):
    hits = []
    time_intervals = make_equal_time_intervals(from_date, to_date, datapoints)
    for i in range(1, len(time_intervals)):

        from_date_to_search = time_intervals[i - 1]
        to_date_to_search = time_intervals[i]

        print("Searching from: ", from_date_to_search, " to: ", to_date_to_search)

        req = Search(using=es, index="objects-voting-statistics", extra={"size": 1})  # return size 1k
        req = req.source(["account", "stake", "proxy", "proxy_for", "block_time", "block_number"])  # retrive only this attributes
        req = req.sort("-block_number")  # sort by blocknumber => last_block is first hit
        qaccount = Q("match", account=account)  # match account
        qrange = Q("range", block_time={"gte": from_date_to_search, "lte": to_date_to_search})  # match date range
        req.query = qaccount & qrange  # combine queries

        response = req.execute()

        for hit in response:
            hit = hit.to_dict()
            hits.append(hit)
    return hits


def get_account_power(
        from_date="2019-01-01",
        to_date="2020-01-01",
        account="1.2.285",
        datapoints=50,
        type="total",  # @ReservedAssignment
        grouplessthan=5
):

    if grouplessthan < 2:
        grouplessthan = 2

    if datapoints > 700:
        datapoints = 700

    account_obj = None
    try:
        print("acc:", account)
        account_obj = explorer.get_account(account)
        account = account_obj["id"]
    except Exception as e:  # RPCError: probably due to not existent account
        # TODO add error msg
        print("EXPCEPTION: probably due to wrong id: " + str(e))
        return {
            "account": account,
            "name:": "ERROR: probably due to wrong account id"
        }

    hits = None
    try:
        hits = _get_account_power(from_date, to_date, account, datapoints)
    except ValueError:
        return {
            "account": account,
            "name": "ERROR: wrong dateformat (format: YYYY-MM-DD)",
        }

    blocks = []
    block_time = []
    self_powers = []

    if type == "total":
        total_powers = []
        for hit in hits:
            blocks.append(hit["block_number"])
            block_time.append(hit["block_time"])
            self_powers.append(get_self_power(hit))
            total_powers.append(calc_total_power(hit["proxy_for"]))

        ret = {
            "account": account,
            "name": account_obj["name"],
            "blocks": blocks,
            "block_time": block_time,
            "self_powers": self_powers,
            "total_powers": total_powers
        }

    elif type == "proxy":
        proxy_powers = {}
        block_counter = 0

        for hit in hits:
            blocks.append(hit["block_number"])
            block_time.append(hit["block_time"])
            self_powers.append(get_self_power(hit))
            proxy_powers = fill_sub_power(hit["proxy_for"], proxy_powers, block_counter, datapoints)
            block_counter += 1

        proxy_powers = merge_fields_below_percentage(
            grouplessthan, len(blocks), datapoints, proxy_powers, self_powers)

        for pp in proxy_powers:
            try:
                proxy_name = explorer.get_account_name(pp[0])
                pp[0] = proxy_name
            except Exception:
                pass

        ret = {
            "account": account,
            "name": account_obj["name"],
            "blocks": blocks,
            "block_time": block_time,
            "self_powers": self_powers,
            "proxy_powers": proxy_powers
        }
    else:
        abort(400)

    return ret


@needs_es()
@cache.memoize()
def _get_voteable_votes(from_date, to_date, vote_id, datapoints):
    hits = []
    time_intervals = make_equal_time_intervals(from_date, to_date, datapoints)

    for i in range(1, len(time_intervals)):

        from_date_to_search = time_intervals[i - 1]
        to_date_to_search = time_intervals[i]

        print("Searching from: ", from_date_to_search, " to: ", to_date_to_search)

        req = Search(using=es, index="objects-voteable-statistics", extra={"size": 1})  # size: max return size
        req = req.source(["vote_id", "block_time", "block_number", "voted_by"])  # retrive only this attributes
        req = req.sort("-block_number")  # sort by blocknumber => newest block is first hit
        qvote_id = Q("match_phrase", vote_id=vote_id)
        qrange = Q(
            "range",
            block_time={
                "gte": from_date_to_search,
                "lte": to_date_to_search
            }
        )
        req.query = qvote_id & qrange
        response = req.execute()

        for hit in response:
            hit = hit.to_dict()
            hits.append(hit)

    return hits


def get_voteable_votes(
        from_date="2019-01-01",
        to_date="2020-01-01",
        id="1.14.206",  # @ReservedAssignment
        datapoints=50,
        type="total",  # @ReservedAssignment
        grouplessthan=5
):
    """
    Returns the voting power of a worker over time with each account voted for him
    :param from_date:
    :type from_date:
    :param to_date:
    :type to_date:
    :param id:
    :type id:
    :param datapoints:
    :type datapoints:
    :param type:
    :type type:
    """

    if grouplessthan < 2:
        grouplessthan = 2

    # acquiring all worker and resolving the object_id to vote_id and name
    workers = explorer.get_workers()
    name = ""
    vote_id = ""
    for worker in workers:
        worker = worker[0]
        if worker["id"] == id:
            name = worker["name"]
            vote_id = worker["vote_for"]
            break

    if vote_id == "":
        return {
            "id": id,
            "vote_id": "ERROR ID DOES NOT EXIST",
            "name": "ERROR ID DOES NOT EXIST",
        }

    print("vote_id", vote_id)
    if datapoints > 700:
        datapoints = 700

    hits = None
    try:
        hits = _get_voteable_votes(from_date, to_date, vote_id, datapoints)
    except ValueError:
        return {
            "vote_id": "ERROR: wrong dateformat (format: YYYY-MM-DD)",
            "name": "ERROR: wrong dateformat (format: YYYY-MM-DD)",
        }

    blocks = []
    block_time = []

    if type == "total":
        total_votes = []

        for hit in hits:
            blocks.append(hit["block_number"])
            block_time.append(hit["block_time"])
            total_votes.append(calc_total_power(hit["voted_by"]))

        ret = {
            "id": id,
            "vote_id": vote_id,
            "name": name,
            "blocks": blocks,
            "block_time": block_time,
            "total_votes": total_votes,
        }

    else:  # if type == "voters"
        voted_by = {}
        block_counter = 0

        for hit in hits:
            blocks.append(hit["block_number"])
            block_time.append(hit["block_time"])
            voted_by = fill_sub_power(hit["voted_by"], voted_by, block_counter, datapoints)
            block_counter += 1

        voted_by = merge_fields_below_percentage(grouplessthan, len(blocks), datapoints, voted_by, None)

        for vb in voted_by:
            try:
                vb[0] = explorer.get_account_name(vb[0])
            except Exception:
                pass

        ret = {
            "id": id,
            "vote_id": vote_id,
            "name": name,
            "blocks": blocks,
            "block_time": block_time,
            "voted_by": voted_by
        }

    return ret
