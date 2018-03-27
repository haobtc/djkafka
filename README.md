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
    ....
    'djkafka.dbrouter.KafkaDBRouter'
]

```
