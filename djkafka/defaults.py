from django.conf import settings

config = getattr(settings, 'DJKAFKA', {
    'SERVERS': {},
    'SKIP_MIGRATION_DBS': []
})

SKIP_MIGRATION_DBS = config.get('SKIP_MIGRATION_DBS', [])
