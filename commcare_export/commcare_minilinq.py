"""
CommCare/Export-specific extensions to MiniLinq.

To date, this is simply built-ins for querying the
API directly.
"""
import json

from commcare_export.env import DictEnv, CannotBind, CannotReplace
from datetime import datetime

from commcare_export.misc import unwrap

try:
    from urllib.parse import urlparse, parse_qs
except ImportError:
    from urlparse import urlparse, parse_qs


class SimpleSinceParams(object):
    def __init__(self, start, end):
        self.start_param = start
        self.end_param = end

    def __call__(self, since, until):
        params = {
            self.start_param: [s.isoformat() for s in since]
        }
        if until:
            params[self.end_param] = [u.isoformat() for u in until]
        return params


class FormFilterSinceParams(object):
    def __call__(self, since, until):
        print(since, until)
        assert since is None or isinstance(since, list)
        assert until is None or isinstance(until, list)

        print('since', since)
        print('until', until)

        # since and until should be lists of two times [mod_recv_time, indexed_on]
        # where mod_recv_time may correspond either to server_modified_on or received_on.
        mod_recv_range_expression = {}
        indexed_range_expression = {}
        if since:
            mod_recv_range_expression['gte'] = since[0].isoformat()
            if len(since) > 1:
                indexed_range_expression['gte'] = since[1].isoformat()

        if until:
            mod_recv_range_expression['lte'] = until[0].isoformat()
            if len(until) > 1:
                indexed_range_expression['lte'] = until[1].isoformat()

        server_modified_missing = {"missing": {
            "field": "server_modified_on", "null_value": True, "existence": True}
        }
        query = json.dumps({
            'filter': {
                "and": [
                    {
                        "or": [
                            {
                                "and": [
                                    {
                                        "not": server_modified_missing
                                    },
                                    {
                                        "range": {
                                            "server_modified_on": mod_recv_range_expression
                                        }
                                    }
                                ]
                            },
                            {
                                "and": [
                                    server_modified_missing,
                                    {
                                        "range": {
                                            "received_on": mod_recv_range_expression
                                        }
                                    }
                                ]
                            }
                        ]
                    },
                    {
                        "range": {
                            "indexed_on": indexed_range_expression
                        }
                    }
                ]
            }})

        return {'_search': query}


resource_since_params = {
    'form': FormFilterSinceParams(),
    'case': SimpleSinceParams('server_date_modified_start', 'server_date_modified_end'),
    'user': None,
    'location': None,
    'application': None,
    'web-user': None,
}


def get_paginator(resource, page_size=1000):
    return {
        # TODO(Charlie): Confirm that 'indexed_on' should not be used as the since_field
        # because one of the previous fields should always be present. It will only
        # be used as the final ordering field.
        'form': DatePaginator('form', [('server_modified_on','received_on'), 'indexed_on'], page_size),
        'case': DatePaginator('case', ['server_date_modified', 'indexed_on'], page_size),
        'user': SimplePaginator('user', page_size),
        'location': SimplePaginator('location', page_size),
        'application': SimplePaginator('application', page_size),
        'web-user': SimplePaginator('web-user', page_size),
    }[resource]


class CommCareHqEnv(DictEnv):
    """
    An environment providing primitives for pulling from the
    CommCareHq API.
    """
    
    def __init__(self, commcare_hq_client, until=None, page_size=1000):
        self.commcare_hq_client = commcare_hq_client
        self.until = until
        self.page_size = page_size
        super(CommCareHqEnv, self).__init__({
            'api_data' : self.api_data
        })

    @unwrap('checkpoint_manager')
    def api_data(self, resource, checkpoint_manager, payload=None, include_referenced_items=None):
        if resource not in resource_since_params:
            raise ValueError('I do not know how to access the API resource "%s"' % resource)

        paginator = get_paginator(resource, self.page_size)
        paginator.init(payload, include_referenced_items, [self.until] if self.until is not None else None)
        since_param = [checkpoint_manager.since_param] if checkpoint_manager.since_param is not None else None
        initial_params = paginator.next_page_params_since(since_param)
        return self.commcare_hq_client.iterate(
            resource, paginator,
            params=initial_params, checkpoint_manager=checkpoint_manager
        )

    def bind(self, name, value):
        raise CannotBind()

    def replace(self, data):
        raise CannotReplace()


class SimplePaginator(object):
    """
    Paginate based on the 'next' URL provided in the API response.
    """
    def __init__(self, resource, page_size=1000):
        self.resource = resource
        self.page_size = page_size

    def init(self, payload=None, include_referenced_items=None, until=None):
        self.payload = dict(payload or {})  # Do not mutate passed-in dicts
        self.include_referenced_items = include_referenced_items
        self.until = until

    # TODO(Charlie): When using 'indexed_on' the order key and since/until keys become
    # tuples. The command line arguments may only be the first part of the tuple.
    # What is the since/until param pulled from the batch? See get_since_date for
    # DatePaginator, it is the first field found in the object. We would change that
    # to the tuple of all fields found in the object, in the order given in since_field.
    def next_page_params_since(self, since=None):
        params = self.payload
        params['limit'] = self.page_size

        resource_date_params = resource_since_params[self.resource]
        if (since or self.until) and resource_date_params:
            params.update(
                resource_date_params(since, self.until)
            )

        if self.include_referenced_items:
            params.update([('%s__full' % referenced_item, 'true') for referenced_item in self.include_referenced_items])

        return params

    def next_page_params_from_batch(self, batch):
        if batch['meta']['next']:
            return parse_qs(urlparse(batch['meta']['next']).query)


class DatePaginator(SimplePaginator):
    """
    This paginator is designed to get around the issue of deep paging where the deeper the page the longer
    the query takes.

    Paginate records according to a date in the record. The params for the next batch will include a filter
    for the date of the last record in the previous batch.

    This also adds an ordering parameter to ensure that the records are ordered by the date field in ascending order.

    :param resource: The name of the resource being fetched: ``form`` or ``case``.
    :param since_field: The name of the date field to use for pagination.
    """
    def __init__(self, resource, since_field, page_size=1000):
        super(DatePaginator, self).__init__(resource, page_size)
        self.since_field = since_field

    def next_page_params_since(self, since=None):
        params = super(DatePaginator, self).next_page_params_since(since)
        params['order_by'] = []
        for sf in self.since_field:
            if isinstance(sf, tuple):
                for ssf in sf:
                    params['order_by'].append(ssf)
            else:
                params['order_by'].append(sf)
        return params

    def next_page_params_from_batch(self, batch):
        since_date = self.get_since_date(batch)
        if since_date:
            return self.next_page_params_since(since_date)

    def get_since_date(self, batch):
        try:
            last_obj = batch['objects'][-1]
        except IndexError:
            return

        def parse_timestamp(ts):
            if ts:
                for fmt in ('%Y-%m-%dT%H:%M:%SZ', '%Y-%m-%dT%H:%M:%S.%fZ'):
                    try:
                        return datetime.strptime(ts, fmt)
                    except ValueError:
                        pass

        # desc can be a single field, which is either found or None, or a tuple,
        # which means take the first available sub-field.
        def get_field(desc):
            if isinstance(desc, tuple):
                for sub_desc in desc:
                    maybe_ts = get_field(sub_desc)
                    if maybe_ts:
                        return maybe_ts
            else:
                return parse_timestamp(last_obj.get(desc))
                    
        if last_obj:
            since = []
            for field in self.since_field:
                maybe_ts = get_field(field)
                if maybe_ts:
                    since.append(maybe_ts)
            return since if len(since) > 0 else None
