"""zerodb-celery: Celery broker + result backend powered by ZeroDB.

Replace Redis or RabbitMQ with ZeroDB in one line:

    app = Celery('tasks')
    app.config_from_object({
        'broker_url': 'zerodb://auto',
        'result_backend': 'zerodb://auto',
    })
"""

from zerodb_celery.backend import ZeroDBBackend
from zerodb_celery.broker import ZeroDBBroker

__version__ = "0.1.0"
__all__ = ["ZeroDBBackend", "ZeroDBBroker"]
