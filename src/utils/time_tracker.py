import time

def timer(active=True, msg=None):
    def decorator(func):
        def wrapper(*args, **kwargs):
            if active:
                start_time = time.time()
                result = func(*args, **kwargs)
                end_time = time.time()
                elapsed_time = end_time - start_time
                if msg:
                    print(f"{msg}")
                print(f"Function '{func.__name__}' executed in {elapsed_time:.4f} seconds.")
                return result
            else:
                return func(*args, **kwargs)
        return wrapper
    return decorator
