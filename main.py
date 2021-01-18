import requests
import json
import psycopg2

user = "postgres"
database = "HL7STD"
host = "127.0.0.1"
port = "5432"
password = "xxxxx"


def psql_connect():
    try:
        connection = psycopg2.connect(
            user=user,
            password=password,
            host=host,
            port=port,
            database=database)

        cursor = connection.cursor()

        # Print PostgreSQL version
        cursor.execute("SELECT version();")
        record = cursor.fetchone()
        print("You are connected to - ", record, "\n")

    except (Exception, psycopg2.Error) as error:
        raise Exception("Error while connecting to database", error)

    return connection


# get a trigger event: https://hl7-definition.caristix.com/v2-api/1/HL7v2.3/TriggerEvents/ADT_A40

# get all events: https://hl7-definition.caristix.com/v2-api/1/HL7v2.1/TriggerEvents

# get all segments: https://hl7-definition.caristix.com/v2-api/1/HL7v2.2/Segments

# segment type: https://hl7-definition.caristix.com/v2-api/1/HL7v2.2/Segments/ACC


#
# print(json.loads(r.content))


headers = {'Accept': 'application/json'}


def sanitize_string(s: str):
    if s is None:
        return s

    return s.replace("'", "''")


def truncate_all(tables, conn):
    cursor = conn.cursor()

    for table in tables:
        cursor.execute(f"truncate {table}")

    conn.commit()


def insert_versions(versions, conn):
    cursor = conn.cursor()

    for v in versions:
        cursor.execute(f"insert into hl7.version values ('{v}')")

    conn.commit()


def insert_events(events, version, conn):
    cursor = conn.cursor()

    for ev in events:
        cursor.execute(
            f"insert into hl7.events (event_idx, version, label, description) values ('{ev['id']}', '{version}', '{sanitize_string(ev['label'])}', '{sanitize_string(ev['description'])}')")

    conn.commit()


def insert_segments(segments, version, conn):
    cursor = conn.cursor()

    for seg in segments:
        cursor.execute(
            f"insert into hl7.segments (segm_idx, version, description, label) values ('{seg['id']}', '{version}', '{sanitize_string(seg['description'])}', '{sanitize_string(seg['label'])}')")

    conn.commit()


def insert_seq(seg, curr, version, event_id):
    if seg['isGroup']:

        for sub_seg in seg['segments']:
            insert_seq(sub_seg, curr, version, event_id)

    else:
        query = f"insert into hl7.event_segment (event_idx, version, segm_idx, usage, rpt, seq) values ('{event_details['id']}', '{version}'," \
                f" '{seg['id']}', '{seg['usage']}', '{seg['rpt']}','{seg['sequence']}')"

        curr.execute(query)


def insert_event_details(event_details, version, conn):
    curr = conn.cursor()

    if event_details['sample']:
        curr.execute(
            f"update hl7.events set sample = '{sanitize_string(event_details['sample'])}' where version = '{version}' and event_idx = '{event_details['id']}'")

    for seg in event_details['segments']:
        insert_seq(seg, curr, version, event_details['id'])

    conn.commit()


def insert_segment_details(segm_details, version, connection):
    curr = connection.cursor()

    if segm_details['sample'] is not None and len(segm_details['sample']) > 0:
        curr.execute(
            f"update hl7.segments set sample = '{sanitize_string(segm_details['sample'])}' where version = '{version}' and segm_idx = '{segm_details['id']}'")

    for field in segm_details['fields']:
        query = f"insert into hl7.fields (version, segm_idx, dataType, dataTypeName, description, field_idx, length, name, rpt, tableId, tableName, usage, position)" \
                f" values ('{version}', '{segm_details['id']}', '{sanitize_string(field['dataType'])}', '{sanitize_string(field['dataTypeName'])}', '{sanitize_string(field['description'])}', '{field['id']}', '{field['length']}', '{sanitize_string(field['name'])}', " \
                f"'{field['rpt']}', '{sanitize_string(field['tableId'])}', '{sanitize_string(field['tableName'])}', '{field['usage']}', '{field['position']}')"

        curr.execute(query)

    connection.commit()


def get_all_events(version):
    url = f"https://hl7-definition.caristix.com/v2-api/1/HL7{version}/TriggerEvents"
    r = requests.get(url, headers=headers)

    # if not r.status_code == 200:
    #     raise Exception(f"Get all events for version {version} failed")

    return json.loads(r.content)


def get_all_segments(version):
    url = f"https://hl7-definition.caristix.com/v2-api/1/HL7{version}/Segments"
    r = requests.get(url, headers=headers)

    # if not r.status_code == 200:
    #     raise Exception(f"Get all events for version {version} failed")

    return json.loads(r.content)


def get_event_details(version, event_idx):
    url = f"https://hl7-definition.caristix.com/v2-api/1/HL7{version}/TriggerEvents/{event_idx}"
    r = requests.get(url,
                     headers=headers)

    # if not r.status_code == 200:
    #     raise Exception(f"Get event details {event_idx} for version {version} failed")

    return json.loads(r.content)


def get_segment_details(version, segment_id):
    url = f"https://hl7-definition.caristix.com/v2-api/1/HL7{version}/Segments/{segment_id}"

    r = requests.get(url,
                     headers=headers)

    return json.loads(r.content)


if __name__ == '__main__':
    connection = psql_connect()

    versions = ["v2.1", "v2.2", "v2.3", "v2.3.1", "v2.4", "v2.5", "v2.5.1", "v2.6", "v2.7", "v2.7.1", "v2.8"]
    tables = ["hl7.version", "hl7.event_segment", "hl7.events", "hl7.segments", "hl7.fields"]

    truncate_all(tables, connection)

    insert_versions(versions, connection)

    for version in versions:
        print("Starting for version ", version)

        print("Get all events for version ", version)
        events = get_all_events(version)

        print("Inserting events into db for version ", version)
        insert_events(events, version, connection)

        for ev in events:
            print(f"Getting event details for event {ev['id']} for version {version}")
            event_details = get_event_details(version, ev['id'])

            print(f"Inserting event details into db {ev['id']} for version {version}")
            insert_event_details(event_details, version, connection)

        print("Getting all segments for version ", version)
        segments = get_all_segments(version)

        print("Inserting segments into db for version ", version)
        insert_segments(segments, version, connection)

        for seg in segments:
            print(f"Getting segment details for seg {seg['id']} for version {version}")
            seg_details = get_segment_details(version, seg['id'])

            print(f"Inserting segment details into db {seg['id']} for version {version}")
            insert_segment_details(seg_details, version, connection)

        print("End version ", version)
