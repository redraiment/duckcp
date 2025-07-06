from rich.prompt import Prompt


def ensure(condition: bool, message: str = '参数异常'):
    """
    Assert的运行时版本。
    """
    if not condition:
        raise AssertionError(message)


def confirm(prompt: str, default: str = 'n') -> bool:
    return Prompt.ask(prompt, choices=['y', 'n'], default=default).lower() == 'y'
