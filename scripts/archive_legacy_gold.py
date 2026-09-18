"""Optional post-migration cleanup. Default is DRY RUN. Never deletes an unbacked-up object.

Only handles old gold/chicago_taxi/start=... keyspace; leaves all business Gold,
Silver, Bronze, metadata and quality prefixes untouched.
"""
import argparse
from taxi_pipeline.config import Settings
from taxi_pipeline.storage import s3_client, load_manifest


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--apply', action='store_true', help='Copy all old keys to archive then remove originals')
    args = parser.parse_args()
    cfg = Settings.from_env()
    client = s3_client(cfg)
    publication = load_manifest(client, cfg.bucket, 'metadata/chicago_taxi/current_publication.json')
    if publication.get('quality_gate') != 'PASS':
        raise SystemExit('Refusing cleanup: no confirmed publication marker')
    prefix = 'gold/chicago_taxi/start='
    keys = []
    for page in client.get_paginator('list_objects_v2').paginate(Bucket=cfg.bucket, Prefix=prefix):
        keys.extend(item['Key'] for item in page.get('Contents', []))
    print(f'Found {len(keys)} legacy objects under {prefix}.')
    if not args.apply:
        print('DRY RUN: old files were NOT changed. Use --apply after checking Dremio and backups.')
        return
    for key in keys:
        dest = 'archive/legacy_gold/' + key[len('gold/'):]
        client.copy_object(Bucket=cfg.bucket, CopySource={'Bucket': cfg.bucket, 'Key': key}, Key=dest)
        # Verify size before deleting; archive target is unique to legacy layout.
        original_size = client.head_object(Bucket=cfg.bucket, Key=key)['ContentLength']
        archive_size = client.head_object(Bucket=cfg.bucket, Key=dest)['ContentLength']
        if original_size != archive_size:
            raise RuntimeError(f'Archive size mismatch for {key}; original preserved')
        client.delete_object(Bucket=cfg.bucket, Key=key)
    print(f'Archived {len(keys)} legacy objects. Remove old Dremio dataset promotions manually.')


if __name__ == '__main__':
    main()
