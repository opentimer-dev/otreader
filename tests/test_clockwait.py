from app.clockwait import main, wait_for_sync


class FakeTime:
    def __init__(self):
        self.t = 0.0

    def sleep(self, s):
        self.t += s

    def monotonic(self):
        return self.t


def test_returns_as_soon_as_synchronised():
    ft = FakeTime()
    states = iter([False, False, True])
    st = wait_for_sync(60, check=lambda: {"synchronized": next(states)}, sleep=ft.sleep, monotonic=ft.monotonic)
    assert st["synchronized"] is True and ft.t == 4.0


def test_gives_up_after_timeout_without_time_source():
    ft = FakeTime()
    st = wait_for_sync(10, check=lambda: {"synchronized": False}, sleep=ft.sleep, monotonic=ft.monotonic)
    assert st["synchronized"] is False and 10 <= ft.t <= 12  # bounded: never waits forever


def test_unknown_status_does_not_wait():
    ft = FakeTime()
    st = wait_for_sync(60, check=lambda: {"synchronized": None}, sleep=ft.sleep, monotonic=ft.monotonic)
    assert st["synchronized"] is None and ft.t == 0


def test_cli_always_exits_zero():
    assert main(["--timeout", "0"]) == 0
