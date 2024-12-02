import time
from fastapi_utils.tasks import repeat_every

@repeat_every(seconds=1, max_repetitions=3)
def example_task():
    current_time = time.time()
    print(f'task ran successfully at {current_time}')