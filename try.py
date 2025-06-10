import time
from Quartz.CoreGraphics import (
    CGEventCreateMouseEvent,
    CGEventPost,
    kCGHIDEventTap,
    kCGEventMouseMoved
)

def move_mouse(dx=1, dy=0):
    # get current position (not shown here—harder in Quartz)
    # instead just move by an offset repeatedly:
    evt = CGEventCreateMouseEvent(
        None,
        kCGEventMouseMoved,
        (dx, dy),
        0
    )
    CGEventPost(kCGHIDEventTap, evt)

if __name__ == "__main__":
    while True:
        move_mouse(1, 0)
        time.sleep(0.1)
        move_mouse(-1, 0)
        time.sleep(60)  # jiggle every 60s
