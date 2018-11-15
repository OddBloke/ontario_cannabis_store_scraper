#!/usr/bin/env python
import json

import jinja2
from sqlalchemy import func

from scraper import (
    HistoricalListing, HistoricalProductAvailability, _get_db_session)


def _get_total_datapoints(session):
    query = session.query(
        HistoricalProductAvailability.timestamp.label('timestamp'),
        func.sum(HistoricalProductAvailability.size *
                 HistoricalProductAvailability.availability).label('amount')
    ).group_by(HistoricalProductAvailability.timestamp)
    print(str(query))
    data_points = []
    for instance in query:
        instance_dict = instance._asdict()
        instance_dict['timestamp'] *= 1000
        instance_dict['label'] = 'Total'
        data_points.append(instance_dict)
    return data_points


def _get_per_brand_datapoints(session):
    query = session.query(
        HistoricalProductAvailability.timestamp.label('timestamp'),
        HistoricalListing.brand.label('label'),
        func.sum(HistoricalProductAvailability.size *
                 HistoricalProductAvailability.availability).label('amount')
    ).join(HistoricalListing).group_by(HistoricalListing.brand,
                                       HistoricalListing.timestamp)
    print(str(query))
    data_points = []
    for instance in query:
        instance_dict = instance._asdict()
        instance_dict['timestamp'] *= 1000
        data_points.append(instance_dict)
    return data_points


def main():
    session = _get_db_session()
    data_points = _get_total_datapoints(session)
    data_points.extend(_get_per_brand_datapoints(session))
    with open('graph.html.j2') as template_file:
        template = jinja2.Template(template_file.read())
    with open('output.html', 'w') as output_file:
        output_file.write(
            template.render(
                {'data_points': json.dumps(data_points, indent=1)}))


if __name__ == '__main__':
    main()
