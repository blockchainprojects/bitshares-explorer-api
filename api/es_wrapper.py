from elasticsearch_dsl import Search, Q
from services.elasticsearch_client import es
from datetime import datetime, timedelta
import explorer
from pprint import pprint


def get_account_history(account_id=None, operation_type=None, from_=0, size=10,
                        from_date='2015-10-10', to_date='now', sort_by='-block_data.block_time',
                        type='data', agg_field='operation_type'):
    if type != "data":
        s = Search(using=es, index="bitshares-*")
    else:
        s = Search(using=es, index="bitshares-*", extra={"size": size, "from": from_})

    q = Q()

    if account_id:
        q = q & Q("match", account_history__account=account_id)
    if operation_type:
        q = q & Q("match", operation_type=operation_type)

    range_query = Q("range", block_data__block_time={'gte': from_date, 'lte': to_date})
    s.query = q & range_query

    if type != "data":
        s.aggs.bucket('per_field', 'terms', field=agg_field, size=size)

    s = s.sort(sort_by)
    response = s.execute()

    if type == "data":
        return [ hit.to_dict() for hit in response ]
    else:
        return [ field.to_dict() for field in response.aggregations.per_field.buckets ]


def get_single_operation(operation_id):
    s = Search(using=es, index="bitshares-*", extra={"size": 1})
    s.query = Q("match", account_history__operation_id=operation_id)

    response = s.execute()

    return [ hit.to_dict() for hit in response ]

def get_trx(trx, from_=0, size=10):
    s = Search(using=es, index="bitshares-*", extra={"size": size, "from": from_})
    s.query = Q("match", block_data__trx_id=trx)

    response = s.execute()

    return [ hit.to_dict() for hit in response ]


# TODO maybe write to files not to RAM
# Response cache for es queries
class ResponseCache:
    def __init__(self, store_max):
        self.store_max = store_max
        self.storage = {}

    def add( self, account, datapoints, from_date, to_date, data ):
        self.storage[account] = {}
        stored = self.storage[account]
        stored["datapoints"] = datapoints
        stored["from_date"] = from_date
        stored["to_date"] = to_date
        stored["data"] = data

    def get( self, account, datapoints, from_date, to_date ):
        stored = {}
        try:
            stored = self.storage[account]
        except:
            return None

        if stored["from_date"] != from_date \
            or stored["to_date"] != to_date \
            or stored["datapoints"] != datapoints:
                return None

        return stored["data"]

account_power_cache  = ResponseCache(100)
voteable_votes_cache = ResponseCache(100)


# creates equal time intervals for partial es querries
def make_equal_time_intervals( from_date, to_date, datapoints ):

    datetime_format = "%Y-%m-%d"
    datetime_from   = None
    datetime_to     = None

    try:
        datetime_from = datetime.strptime( from_date, datetime_format )
        datetime_to   = datetime.strptime( to_date, datetime_format )
    except ValueError:
        datetime_from = datetime.strptime( "2019-01-01", datetime_format )
        datetime_to   = datetime.strptime( "2020-01-01", datetime_format )

    time_window_days = ( datetime_to - datetime_from ).days / datapoints
    delta_between_points = timedelta( days=time_window_days )

    datetime_intervals = []
    # extend the points by one to include a point around 'from_date' (if present)
    datetime_val = datetime_from - 2 * delta_between_points
    for i in range( datapoints + 1 ):
        datetime_val += delta_between_points
        datetime_val_str = datetime_val.strftime( datetime_format )
        datetime_intervals.append( datetime_val_str )

    return datetime_intervals

def calc_total_power( field ):
    total_power = 0
    for id_to_power in field:
        total_power += int( int( id_to_power[1] ) / 100000 )
    return total_power

def fill_sub_power( field, storage, block_counter, datapoints ):
    for id_to_power in field:
        id = id_to_power[0]
        power = int( int( id_to_power[1] ) / 100000 )
        if id not in storage:
            # precreate storage with max size
            storage[id] = [0] * datapoints

        # add the power to the corresponding proxy/voter (id) at the block_counter
        storage[id][block_counter] = power

    return storage

def get_self_power( hit ):
    if hit["proxy"] != "1.2.5":
        return int( int( hit["stake"] )  / 100000 )
    return 0

def merge_fields_below_percentage( merge_below_percentage, size_successful_queries, datapoints, storage,
    optional_storage ):

    if size_successful_queries == 0:
        return []

    size_not_found_elements = datapoints - size_successful_queries
    index_to_delete_begin   = datapoints - size_not_found_elements
    index_to_delete_end     = datapoints
    last_block              = size_successful_queries - 1 # last available element

    # we preallocated a bigger size, now be delete the not needed part
    for power in storage.values():
        del power[index_to_delete_begin:index_to_delete_end]


    total_power_last_block = optional_storage[last_block] if optional_storage != None else 0
    for power in storage.values():
        total_power_last_block += power[last_block]

    merge_below_abs = int( merge_below_percentage * total_power_last_block / 100 )

    merged_elements = [0] * size_successful_queries
    to_delete = []
    for _id, power in storage.items():
        if power[last_block] < merge_below_abs:
            for i in range( size_successful_queries ):
                merged_elements[i] += power[i]
            to_delete.append( _id )

    storage["< " + str(merge_below_percentage) + "%"] = merged_elements

    for _id in to_delete:
        del storage[_id]

    # sort all proxies by last item size
    def sort_by_last_item_asc( powers ):
        return powers[1][-1]

    # convert to list to be sortable
    storage_list = [ [k,v] for k, v in storage.items() ]
    storage_list.sort( key = sort_by_last_item_asc )

    return storage_list

def get_account_power( from_date="2019-01-01", to_date="2020-01-01", account="1.2.285",
                       datapoints=50, type="total" ):
    global account_power_cache

    if datapoints > 700:
        datapoints = 700

    account_obj = None
    try:
        account_obj = explorer.get_account( account )[0]
    except: # RPCError: probably due to not existent account
        # TODO add error msg
        return
        {
            "account":      "ERROR",
            "blocks":       [],
            "block_time":   [],
            "self_powers":  [],
            "total_powers": []
        }



    hits = account_power_cache.get( account, datapoints, from_date, to_date )
    if hits == None: # nothing cached query the data
        hits = []

        time_intervals = make_equal_time_intervals( from_date, to_date, datapoints )
        for i in range( 1, len(time_intervals) ):

            from_date_to_search = time_intervals[i-1]
            to_date_to_search = time_intervals[i]

            print( "Searching from: ", from_date_to_search, " to: ", to_date_to_search )

            req = Search( using=es, index="objects-voting-statistics", extra={"size": 1} ) # return size 1k
            req = req.source( ["account", "stake", "proxy", "proxy_for", "block_time", "block_number"] ) # retrive only this attributes
            req = req.sort("-block_number") # sort by blocknumber => last_block is first hit
            qaccount = Q( "match", account=account ) # match account
            qrange = Q( "range", block_time={ "gte": from_date_to_search, "lte": to_date_to_search} ) # match date range
            req.query = qaccount & qrange # combine queries

            response = req.execute()

            for hit in response:
                hit = hit.to_dict()
                hits.append( hit )

        account_power_cache.add( account, datapoints, from_date, to_date, hits )

    blocks      = []
    block_time  = []
    self_powers = []

    if type == "total":
        total_powers = []
        for hit in hits:
            blocks.append( hit["block_number"] )
            block_time.append( hit["block_time"] )
            self_powers.append( get_self_power( hit ) )
            total_powers.append( calc_total_power( hit["proxy_for"] ) )

        ret = {
            "account":      account,
            "name":         account_obj["name"],
            "blocks":       blocks,
            "block_time":   block_time,
            "self_powers":  self_powers,
            "total_powers": total_powers
        }

    else: # type == proxy
        proxy_powers = {}
        block_counter = 0
        for hit in hits:
            blocks.append( hit["block_number"] )
            block_time.append( hit["block_time"] )
            self_powers.append( get_self_power( hit ) )
            proxy_powers = fill_sub_power( hit["proxy_for"], proxy_powers, block_counter, datapoints )
            block_counter += 1

        proxy_powers = merge_fields_below_percentage(
            5, len(blocks), datapoints, proxy_powers, self_powers )

        ret = {
            "account":      account,
            "name":         account_obj["name"],
            "blocks":       blocks,
            "block_time":   block_time,
            "self_powers":  self_powers,
            "proxy_powers": proxy_powers
        }

    #ret = jsonify( ret )
    return ret

# returns the voting power of a worker over time with each account voted for him
def get_voteable_votes( from_date="2019-01-01", to_date="2020-01-01", id="1.14.206",
                        datapoints=50, type="total" ):
    print( "REQUEST RECEIVED" )
    global voteable_votes_cache


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

    print( "vote_id", vote_id )
    # TODO maybe remove
    if datapoints > 700:
        datapoints = 700

    hits = voteable_votes_cache.get( vote_id, datapoints, from_date, to_date )
    if hits == None:
        hits = []

        time_intervals = make_equal_time_intervals( from_date, to_date, datapoints )
        for i in range( 1, len(time_intervals) ):

            from_date_to_search = time_intervals[i-1]
            to_date_to_search   = time_intervals[i]

            print( "Searching from: ", from_date_to_search, " to: ", to_date_to_search )

            req = Search( using=es, index="objects-voteable-statistics", extra={ "size": 1 } ) # size: max return size
            req = req.source( ["vote_id", "block_time", "block_number", "voted_by"] ) # retrive only this attributes
            req = req.sort("-block_number") # sort by blocknumber => newest block is first hit
            qvote_id = Q( "match_phrase", vote_id=vote_id )
            qrange = Q( "range", block_time={ "gte": from_date_to_search, "lte": to_date_to_search} )
            req.query = qvote_id & qrange
            response = req.execute()

            for hit in response:
                hit = hit.to_dict()
                hits.append( hit )

        voteable_votes_cache.add( vote_id, datapoints, from_date, to_date, hits )


    blocks      = []
    block_time  = []

    if type == "total":
        total_votes = []

        for hit in hits:
            blocks.append( hit["block_number"] )
            block_time.append( hit["block_time"] )
            total_votes.append( calc_total_power( hit["voted_by"] ) )

        ret = {
            "id":          id,
            "vote_id":     vote_id,
            "name":        name,
            "blocks":      blocks,
            "block_time":  block_time,
            "total_votes": total_votes,
        }

    else: # if type == "voters"
        voted_by    = {}
        block_counter = 0

        for hit in hits:
            blocks.append( hit["block_number"] )
            block_time.append( hit["block_time"] )
            voted_by = fill_sub_power( hit["voted_by"], voted_by, block_counter, datapoints )
            block_counter += 1

        voted_by = merge_fields_below_percentage( 5, len(blocks), datapoints, voted_by, None )

        ret = {
            "id":         id,
            "vote_id":    vote_id,
            "name":       name,
            "blocks":     blocks,
            "block_time": block_time,
            "voted_by":   voted_by
        }

    #ret = jsonify(ret)
    return ret
