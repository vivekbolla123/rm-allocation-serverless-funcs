import logging
import json
from urllib.parse import quote_plus
from redis import Redis
from boto3 import client

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class LastSoldFareMigration:

    def __init__(self):
        self.cache_client = None
        self.logger = logger
        self.generate_configs()
        self.migrate()

    def get_credentials(self, parameter_name):
        ssm_client = client('ssm', region_name='ap-south-1')
        try:
            response = ssm_client.get_parameter(Name=parameter_name, WithDecryption=True)
        except ssm_client.exceptions.ParameterNotFound:
            self.logger.error(f"Parameter not found: {parameter_name}")
            raise
        return response['Parameter']['Value']

    def generate_configs(self):
        ISPROD = False
        if ISPROD:
            CACHE_ENDPOINT = "ip-10-16-6-51.ap-south-1.compute.internal"
            CACHE_PORT = 6379
            CACHE_USERNAME = "default"
            CACHE_PASSWORD = self.get_credentials('/rm/prod/rm-allocation/RM_CACHE_PASSWORD')
        else:
            CACHE_ENDPOINT = "172.24.72.8"
            CACHE_PORT = 6379
            CACHE_USERNAME = "default"
            CACHE_PASSWORD = self.get_credentials('/rm/uat/rm-allocation/RM_CACHE_PASSWORD')

        try:
            self.cache_client = Redis(
                host=CACHE_ENDPOINT,
                port=CACHE_PORT,
                username=CACHE_USERNAME,
                password=CACHE_PASSWORD
            )
            if self.cache_client.ping():
                self.logger.info("Connected to Redis")
        except Exception as e:
            self.logger.error(f"Redis connection error: {e}")
            raise

    def migrate(self):
        pattern = 'LastSoldFare:*'
        keys = self.cache_client.keys(pattern)

        self.logger.info(f"Found {len(keys)} LastSoldFare keys to migrate")

        success_count = 0
        skip_count = 0
        error_count = 0

        for key in keys:
            key_str = key.decode('utf-8')

            # Skip executionTime key — not a fare manifest
            if 'executionTime' in key_str:
                self.logger.info(f"Skipping non-fare key: {key_str}")
                skip_count += 1
                continue

            try:
                key_type = self.cache_client.type(key).decode('utf-8')

                if key_type == 'hash':
                    self.logger.info(f"Already hash type, skipping: {key_str}")
                    skip_count += 1
                    continue

                if key_type != 'string':
                    self.logger.warning(f"Unexpected type '{key_type}' for key {key_str}, skipping")
                    skip_count += 1
                    continue

                # Read existing string value
                existing_str = self.cache_client.get(key).decode('utf-8')
                existing_dict = json.loads(existing_str)

                allfares = existing_dict.get('allfares', [])
                b2bfares = existing_dict.get('b2bfares', [])
                b2cfares = existing_dict.get('b2cfares', [])

                # Get existing TTL to preserve it
                ttl = self.cache_client.ttl(key)

                # Delete old string key
                self.cache_client.delete(key)

                # Write as hash — prevLastSoldB2B/B2C intentionally absent
                # allocation will write them on first run
                self.cache_client.hset(key_str, mapping={
                    'allfares': json.dumps(allfares),
                    'b2bfares': json.dumps(b2bfares),
                    'b2cfares': json.dumps(b2cfares),
                })

                # Restore TTL if it existed
                if ttl > 0:
                    self.cache_client.expire(key_str, ttl)
                    self.logger.info(f"Restored TTL of {ttl}s for {key_str}")

                self.logger.info(
                    f"Migrated {key_str} | "
                    f"allfares: {len(allfares)} | "
                    f"b2bfares: {len(b2bfares)} | "
                    f"b2cfares: {len(b2cfares)}"
                )
                success_count += 1

            except json.JSONDecodeError as e:
                self.logger.error(f"JSON parse error for key {key_str}: {e}")
                error_count += 1
            except Exception as e:
                self.logger.error(f"Error migrating key {key_str}: {e}")
                error_count += 1

        self.logger.info(
            f"Migration complete — "
            f"Success: {success_count} | "
            f"Skipped: {skip_count} | "
            f"Errors: {error_count} | "
            f"Total: {len(keys)}"
        )

    def verify(self):
        """
        Run after migrate() to confirm all LastSoldFare keys
        are now Hash type and none remain as String.
        """
        pattern = 'LastSoldFare:*'
        keys = self.cache_client.keys(pattern)

        string_keys = []
        hash_keys = []
        other_keys = []

        for key in keys:
            key_str = key.decode('utf-8')
            if 'executionTime' in key_str:
                continue
            key_type = self.cache_client.type(key).decode('utf-8')
            if key_type == 'string':
                string_keys.append(key_str)
            elif key_type == 'hash':
                hash_keys.append(key_str)
            else:
                other_keys.append((key_str, key_type))

        self.logger.info(f"Verification — Hash: {len(hash_keys)} | String: {len(string_keys)} | Other: {len(other_keys)}")

        if string_keys:
            self.logger.warning(f"Keys still as String type — re-run migration: {string_keys}")
        else:
            self.logger.info("All LastSoldFare keys successfully migrated to Hash type ✅")

        return {
            'hash': hash_keys,
            'string': string_keys,
            'other': other_keys
        }


if __name__ == "__main__":
    migration = LastSoldFareMigration()
    migration.verify()