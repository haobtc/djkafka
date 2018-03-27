# djkafka
Use kafka in django

## install
```shell
pip install git+https://github.com/haobtc/djkafka.git
```

## include into django projects

add to installed apps, in the settings.py

```python

INSTALLED_APPS = [
    ...
    'djkafka'
]

# make djkafka dbrouter migratable to all databases
DATABASE_ROUTERS = [
    'djkafka.dbrouter.KafkaDBRouter',
    ....
]

DJKAFKA = {
    # 'BOOTSTRAP_SERVERS': 'localhost:9092',
    # 'SKIP_MIGRATION_DBS': [],
    # 'SSL_CONFIG': {...}
}

```

## examples

please refer to djkafka/tests.py
