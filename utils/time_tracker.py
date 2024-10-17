import time

def track_time(func):
    def wrapper(*args, **kwargs):
        start_time = time.time()  # Start the timer
        result = func(*args, **kwargs)  # Execute the function
        end_time = time.time()  # End the timer
        elapsed_time = end_time - start_time  # Time difference
        
        # Convert to minutes and seconds
        minutes = int(elapsed_time // 60)
        seconds = elapsed_time % 60
        print(f"Function '{func.__name__}' took {minutes} minute(s) and {seconds:.2f} second(s).")
        
        return result
    
    return wrapper
