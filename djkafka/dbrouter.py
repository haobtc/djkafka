from .defaults import SKIP_MIGRATION_DBS

class KafkaDBRouter(object):
    def allow_migrate(self, db, app_label, model=None, **hints):
        if app_label == 'djkafka':
            return db not in SKIP_MIGRATION_DBS
        return None

