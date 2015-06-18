import time

def div_plus_mod(a, b):
    return a/b + bool(a%b)

def sleep(start_time):
    sleep_time = min_sleep - time.clock() + start_time
    if sleep_time > 0: time.sleep(sleep_time)

