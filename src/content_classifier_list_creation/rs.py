"""Remote Settings API interactions using kinto_http."""

import uuid

from kinto_http import BearerTokenAuth, Client


def create_client(server_url, auth_token, bucket, collection):
    """Create a kinto_http Client for the given Remote Settings collection."""
    if auth_token.startswith("Bearer "):
        auth = auth_token
    else:
        auth = BearerTokenAuth(auth_token)
    return Client(
        server_url=server_url,
        auth=auth,
        bucket=bucket,
        collection=collection,
    )


def get_records(client):
    """Fetch all records from the collection.

    Returns a list of record dicts.
    """
    return client.get_records()


def find_record_by_name(records, name):
    """Find a record with a matching Name field. Returns the record or None."""
    for record in records:
        if record.get("Name") == name:
            return record
    return None


def upload_attachment(client, filepath, record_data, record_id=None):
    """Upload a file as an attachment to a Remote Settings record.

    If record_id is provided, updates the existing record.
    Otherwise generates a new UUID to create a new record.
    """
    if record_id is None:
        record_id = str(uuid.uuid4())
    print(f"  Uploading {filepath} (record id: {record_id}) ...")
    result = client.add_attachment(
        id=record_id,
        filepath=filepath,
        data=record_data,
    )
    print(f"  Upload successful (record id: {record_id})")
    return result


def request_review(client, message="Automated upload"):
    """Request a review for the collection changes."""
    print("Requesting review ...")
    result = client.request_review(message=message)
    print("Review requested")
    return result


def approve_changes(client):
    """Approve and sign the collection changes (skips review)."""
    print("Approving changes ...")
    result = client.approve_changes()
    print("Changes approved")
    return result


def delete_all_records(client, limit=None):
    """Delete records from the collection.

    Args:
        client: kinto_http Client instance.
        limit: max number of records to delete. None means all.

    Returns:
        Number of records deleted.
    """
    records = client.get_records()
    if not records:
        print("No records to delete")
        return 0

    if limit is None:
        print(f"Deleting all {len(records)} records ...")
        client.delete_records()
        print(f"Deleted {len(records)} records")
        return len(records)

    to_delete = records[:limit]
    print(f"Deleting {len(to_delete)} of {len(records)} records ...")
    with client.batch() as batch_client:
        for record in to_delete:
            batch_client.delete_record(id=record["id"], safe=False)
    print(f"Deleted {len(to_delete)} records")
    return len(to_delete)


def batch_create_records(client, records_to_create, list_name):
    """Create records for rules using the kinto batch API.

    Args:
        client: kinto_http Client instance.
        records_to_create: list of (id, rule_text) tuples.
        list_name: the Name field value for all created records.

    Returns:
        Number of records created.
    """
    if not records_to_create:
        return 0

    print(f"  Creating {len(records_to_create)} records for '{list_name}' ...")
    with client.batch() as batch_client:
        for record_id, rule_text in records_to_create:
            batch_client.create_record(
                id=record_id,
                data={"Rule": rule_text},
                safe=False,
            )
    print(f"  Created {len(records_to_create)} records")
    return len(records_to_create)


def batch_delete_records(client, record_ids):
    """Delete records by ID using the kinto batch API.

    Args:
        client: kinto_http Client instance.
        record_ids: list of record ID strings.

    Returns:
        Number of records deleted.
    """
    if not record_ids:
        return 0

    print(f"  Deleting {len(record_ids)} records ...")
    with client.batch() as batch_client:
        for record_id in record_ids:
            batch_client.delete_record(id=record_id, safe=False)
    print(f"  Deleted {len(record_ids)} records")
    return len(record_ids)
