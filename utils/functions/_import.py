from importlib import import_module


def import_from_string(string: str):
    p, m = string.rsplit('.', 1)

    mod = import_module(p)
    met = getattr(mod, m)
    return met
