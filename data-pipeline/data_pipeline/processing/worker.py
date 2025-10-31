from celery import Celery
import os

# Import and register custom JSON serialization types
from .serialization import register_custom_types

# Register custom types with Kombu's JSON serializer
register_custom_types()

# Allow broker/backend to be overridden via environment variables for containerized
# deployments. Defaults keep previous behaviour for backward compatibility.
broker_url = os.environ.get('CELERY_BROKER_URL', 'amqp://')
result_backend = os.environ.get('CELERY_RESULT_BACKEND', 'rpc://')

app = Celery('processor', broker=broker_url, backend=result_backend, include=['processing.tasks'])

# Optional configuration, see the application user guide.
app.conf.update(
    result_expires=3600,
    task_serializer='json',
    result_serializer='json',
    accept_content=['json'],
    task_always_eager=False,
    task_eager_propagates=True,
)

if __name__ == '__main__':
    app.start()
