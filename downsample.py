import argparse
import json
import re
import sys
import time

from influxdb import InfluxDBClient
from influxdb.exceptions import InfluxDBClientError

TEMP_TABLE_SUFFIX = 'downsample_temp'
DOWNSAMPLED_FIELD_TYPES = ['float', 'integer']


def main(argv=None):
    if argv is None:
        argv = sys.argv[1:]

    parser = argparse.ArgumentParser()
    parser.add_argument('-s', '--schema', help='schema file to use', action='store', default='storage-schema.json')
    parser.add_argument('-H', '--host', help='influxdb host', action='store')
    parser.add_argument('-P', '--port', help='influxdb port', action='store')
    parser.add_argument('-u', '--username', help='influxdb username', action='store')
    parser.add_argument('-p', '--password', help='influxdb password', action='store')
    args = parser.parse_args(argv)

    schema = parse_schema(args.schema)

    client = InfluxDBClient(args.host or schema['host'], args.port or schema['port'],
                            args.username or schema['username'], args.password or schema['password'])
    for database in schema['databases']:
        client.switch_database(database)
        measurements = find_matching_measurements(client, schema, database)

        for measurement, fields in measurements.items():
            print("Processing: {}".format(measurement))
            temp_measurement = '{}_{}'.format(measurement, TEMP_TABLE_SUFFIX)
            query_with_sleep(client, 'DROP MEASUREMENT "{}"'.format(temp_measurement))

            for field in fields:
                if field['fieldType'] != 'string':
                    field_key = field['fieldKey']
                    if 'retentions' in field:
                        for retention in field['retentions']:
                            time_query = ''
                            if retention['min_timestamp'] is not None or retention['max_timestamp'] is not None:
                                time_query += 'WHERE '
                            if retention['min_timestamp'] is not None:
                                time_query += "time >= '{}' ".format(retention['min_timestamp'])
                            if retention['max_timestamp'] is not None:
                                if retention['min_timestamp'] is not None:
                                    time_query += 'AND '
                                time_query += "time <= '{}' ".format(retention['max_timestamp'])

                            interval_query = ''
                            select_query = '"{}"'.format(field_key)
                            if retention['interval'] is not None:
                                interval_query = 'time({}), '.format(retention['interval'])
                                select_query = field['retention_function'].format(field_key)

                            query = 'SELECT {} AS "{}" INTO "{}" FROM "{}" {} GROUP BY {} *'\
                                .format(select_query, field_key, temp_measurement,
                                        measurement, time_query, interval_query)
                            result = query_with_sleep(client, query)

                            print('Field: {:>20}: {:>6}, Written: {}'.format(field_key, retention['interval'] or '*',
                                                                             [x for x in result][0][0]['written']))
                    else:
                        pass
                        result = query_with_sleep(client, 'SELECT "{}" AS "{}" INTO "{}" FROM "{}" GROUP BY *'.
                                                  format(field_key, field_key, temp_measurement, measurement))
                        print('Field: {:>20}: {:>6}, Written: {}'.format(field_key, '*',
                                                                         [x for x in result][0][0]['written']))

            query_with_sleep(client, 'DROP MEASUREMENT "{}"'.format(measurement))
            result = query_with_sleep(client, 'SELECT * INTO "{}" FROM "{}" GROUP BY *'.format(
                measurement, temp_measurement))
            query_with_sleep(client, 'DROP MEASUREMENT "{}"'.format(temp_measurement))
            print('Measurement Finished: {}: Written: {}'.format(measurement, [x for x in result][0][0]['written']))


def query_with_sleep(client, query):
    successful = False
    result = None
    while not successful:
        try:
            result = client.query(query)
            successful = True
        except InfluxDBClientError as e:
            if e.content == 'timeout':
                print('Warning: influxdb timeout, sleeping 10s')
                time.sleep(10)
            else:
                raise

    return result


def find_matching_measurements(client, schema, database):
    measurements = {}

    for measurement in client.get_list_measurements():
        if TEMP_TABLE_SUFFIX not in measurement['name']:
            has_match = False
            policies = []
            fields = None
            for policy in schema['policies']:
                if policy['pattern']['database'].match(database) and \
                        policy['pattern']['measurement'].match(measurement['name']):
                    policies.append(policy)

            if len(policies) > 0:
                result = client.query('SHOW FIELD KEYS FROM "{}"'.format(measurement['name']))
                fields = [fields for fields in result[measurement['name']]]

                for field in fields:
                    if field['fieldType'] in DOWNSAMPLED_FIELD_TYPES:
                        for policy in policies:
                            if policy['pattern']['field'].match(field['fieldKey']):
                                field['retentions'] = policy['retentions']
                                if 'function' in policy:
                                    field['retention_function'] = policy['function']
                                else:
                                    if field['fieldType'] == 'float':
                                        field['retention_function'] = 'MEAN("{}")'
                                    else:
                                        field['retention_function'] = 'LAST("{}")'
                                has_match = True
                                break

            if has_match:
                measurements[measurement['name']] = fields

    return measurements


def parse_schema(schema_file):
    schema = json.loads(open(schema_file).read())

    start_time = time.time()
    for policy in schema['policies']:
        # noinspection PyUnresolvedReferences,PyTypeChecker
        policy['retentions'] = generate_retention_dates(start_time, policy['retentions'])

        policy['pattern']['database'] = re.compile(policy['pattern'].get('database', '.*'))
        policy['pattern']['measurement'] = re.compile(policy['pattern'].get('measurement', '.*'))
        policy['pattern']['field'] = re.compile(policy['pattern'].get('field', '.*'))

    return schema


def generate_retention_dates(start_time, retentions):
    calculated_retentions = []
    for retention in retentions.split(','):
        interval, min_time = retention.split(':')
        if len(calculated_retentions) > 0:
            max_timestamp = calculated_retentions[-1]['min_timestamp']
        else:
            max_timestamp = None

        if min_time != '*':
            min_timestamp = convert_time_offset(start_time, min_time)
        else:
            min_timestamp = None

        if interval == '*':
            interval = None
        calculated_retentions.append({'interval': interval,
                                      'max_timestamp': max_timestamp,
                                      'min_timestamp': min_timestamp})
    return calculated_retentions


def convert_time_offset(start_time, offset_time):
    offset = int(offset_time[:-1])
    offset_type = offset_time[-1:]

    if offset_type == 's':
        offset = offset * 1
    elif offset_type == 'm':
        offset = offset * 60
    elif offset_type == 'h':
        offset = offset * 60 * 60
    elif offset_type == 'd':
        offset = offset * 60 * 60 * 24
    elif offset_type == 'w':
        offset = offset * 60 * 60 * 24 * 7
    elif offset_type == 'y':
        offset = offset * 60 * 60 * 24 * 365
    else:
        raise Exception('Invalid Offset Type')

    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(start_time - offset))


if __name__ == "__main__":
    sys.exit(main())
