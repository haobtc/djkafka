from django.conf import settings

config = getattr(settings, 'DJKAFKA', {
    #'BOOTSTRAP_SERVERS': 'localhost:9092'
})

SKIP_MIGRATION_DBS = config.get('SKIP_MIGRATION_DBS', [])
SERVERS = config.get('BOOTSTRAP_SERVERS', 'localhost:9092')
