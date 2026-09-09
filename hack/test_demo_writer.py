import demo_writer


class FakeInput:
    def fileno(self) -> int:
        return 7

    def read(self, size: int) -> str:
        assert size == 1
        return "P"


def test_read_key_returns_one_lowercase_keystroke_and_restores_terminal(monkeypatch):
    original_settings = ["original"]
    restored = []

    monkeypatch.setattr(demo_writer.sys, "stdin", FakeInput())
    monkeypatch.setattr(demo_writer.termios, "tcgetattr", lambda _fd: original_settings)
    monkeypatch.setattr(demo_writer.tty, "setcbreak", lambda _fd: None)
    monkeypatch.setattr(
        demo_writer.termios,
        "tcsetattr",
        lambda fd, when, settings: restored.append((fd, when, settings)),
    )

    assert demo_writer.read_key() == "p"
    assert restored == [(7, demo_writer.termios.TCSADRAIN, original_settings)]


def test_every_stage_action_has_one_key():
    assert demo_writer.COMMANDS == {
        "p": "pause",
        "t": "repoint",
        "r": "resume",
        "s": "status",
        "q": "quit",
    }
