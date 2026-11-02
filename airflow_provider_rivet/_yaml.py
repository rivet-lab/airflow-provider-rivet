"""Read a rivet config the way rivet does: `${name}` spliced into the raw text first (src/config/resolve.rs), then the
YAML 1.2 scalars of serde_yaml_ng, so `on` and `12:30` stay strings and `017` is the string `017`."""

from __future__ import annotations

import math
import re
from collections.abc import Callable, Mapping, Sequence
from typing import Any

import yaml

MASK = ("", "")


class ScalarInt(int):
    """An int read from a plain scalar, with the text it was written as: rivet reads a string field as that text."""

    text: str


class ScalarFloat(float):
    """A float read from a plain scalar, with the text it was written as."""

    text: str


_INT = re.compile(r"^(?:[-+]?(?:0|[1-9][0-9]*)|\+?0x[0-9a-fA-F]+|\+?0o[0-7]+|\+?0b[01]+|-0x[0-9a-fA-F]+|-0o[0-7]+|-0b[01]+)$")
_FLOAT = re.compile(r"^(?:[-+]?(?:\.[0-9]+|[0-9]+(?:\.[0-9]*)?)(?:[eE][-+]?[0-9]+)?|[-+]?\.(?:inf|Inf|INF)|\.(?:nan|NaN|NAN))$")
_LEADING_ZERO = re.compile(r"^[-+]?0[0-9]+$")
_PLACEHOLDER = re.compile(r"\$\{([^}]*)\}")
_CORE = (
    ("tag:yaml.org,2002:bool", re.compile(r"^(?:true|True|TRUE|false|False|FALSE)$"), list("tTfF")),
    ("tag:yaml.org,2002:int", _INT, list("-+0123456789")),
    ("tag:yaml.org,2002:float", _FLOAT, list("-+0123456789.")),
)
_REPLACED = {tag for tag, _, _ in _CORE} | {"tag:yaml.org,2002:timestamp"}


class ConfigLoader(yaml.SafeLoader):
    """SafeLoader with the YAML 1.1 bool / int / float / timestamp guesses replaced by serde_yaml_ng's 1.2 ones."""


ConfigLoader.yaml_implicit_resolvers = {
    first: [(tag, rx) for tag, rx in pairs if tag not in _REPLACED]
    for first, pairs in yaml.SafeLoader.yaml_implicit_resolvers.items()
}
for _tag, _rx, _first in _CORE:
    ConfigLoader.add_implicit_resolver(_tag, _rx, _first)


def _int(loader: yaml.SafeLoader, node: yaml.ScalarNode) -> Any:
    """serde_yaml_ng's `parse_*_int`: base 10, or 0x / 0o / 0b; a leading zero before digits is a string."""
    text = loader.construct_scalar(node)
    if _LEADING_ZERO.match(text) or not _INT.match(text):
        return text
    sign, body = (-1, text[1:]) if text.startswith("-") else (1, text.lstrip("+"))
    base = {"0x": 16, "0o": 8, "0b": 2}.get(body[:2], 10)
    value = ScalarInt(sign * int(body, base))  # int() reads the 0x / 0o / 0b prefix of its own base
    value.text = text
    return value


def _float(loader: yaml.SafeLoader, node: yaml.ScalarNode) -> Any:
    """serde_yaml_ng's `parse_f64`."""
    text = loader.construct_scalar(node)
    if _LEADING_ZERO.match(text):
        return text
    word = text.lstrip("+-").lower()  # pragma: no mutate -- a float scalar never starts with `X`, so `XX+-XX` strips alike
    if word in (".inf", ".nan"):
        number = float(("-" if text.startswith("-") else "") + word[1:])
    else:
        number = float(text)
        if math.isinf(number):
            return text
    value = ScalarFloat(number)
    value.text = text
    return value


ConfigLoader.add_constructor("tag:yaml.org,2002:int", _int)
ConfigLoader.add_constructor("tag:yaml.org,2002:float", _float)


def load(text: str) -> Any:
    """Parse config text (already substituted)."""
    return yaml.load(text, Loader=ConfigLoader)


def text_of(value: Any) -> str | None:
    """A scalar as rivet's string field reads it: the written text (`name: 2024` is `"2024"`, `name: 08` is `"08"`)."""
    if value is None or isinstance(value, str):
        return value
    if isinstance(value, bool):
        return "true" if value else "false"
    return getattr(value, "text", None) or str(value)


def substitute(text: str, value_of: Callable[[str], str | None], mask: tuple[str, str] = MASK) -> tuple[str, list[str]]:
    """rivet's one scan of `${name}`, with an unresolved name masked and returned."""
    unresolved: list[str] = []
    out, i = [], 0
    while (start := text.find("${", i)) >= 0:
        end = text.find("}", start)
        if end < 0:  # pragma: no mutate -- a found `}` is at start + 2 or later, so `< 0`, `<= 0` and `< 1` agree
            break
        name = text[start + 2 : end]
        value = "" if not name else value_of(name)
        if value is None:
            unresolved.append(name)
            value = f"{mask[0]}{name}{mask[1]}"
        out += [text[i:start], value]
        i = end + 1
    out.append(text[i:])
    return "".join(out), unresolved


def unmasked(value: Any, mask: tuple[str, str]) -> Any:
    """A parsed value with its unresolved placeholders spelled `${name}` again."""
    if isinstance(value, str):
        return value.replace(mask[0], "${").replace(mask[1], "}")
    if isinstance(value, list):
        return [unmasked(v, mask) for v in value]
    if isinstance(value, dict):
        return {unmasked(k, mask): unmasked(v, mask) for k, v in value.items()}
    return value


def params_of(args: Sequence[str]) -> dict[str, str]:
    """The `-p KEY=VALUE` / `--param KEY=VALUE` values among extra arguments, in every spelling clap accepts; last wins."""
    params: dict[str, str] = {}
    args = list(args)
    for i, arg in enumerate(args):
        if arg in ("-p", "--param"):
            item = args[i + 1] if i + 1 < len(args) else ""  # pragma: no mutate -- any default without `=` is ignored alike
        elif arg.startswith("--param="):
            item = arg[len("--param=") :]
        elif arg.startswith("-p"):
            item = arg[2:].removeprefix("=")
        else:
            continue
        if "=" in item:
            key, value = item.split("=", 1)
            params[key] = value
    return params


def redacted_argv(args: Sequence[str]) -> list[str]:
    """The argv with every `-p` / `--param` value masked and its key kept: what a log record may show of it."""
    out = list(args)

    def mask(item: str) -> str:
        key, eq, _ = item.partition("=")
        return f"{key}=***" if eq else "***"

    for i, arg in enumerate(args):
        if arg in ("-p", "--param"):
            if i + 1 < len(args) and args[i + 1]:
                out[i + 1] = mask(args[i + 1])
        elif arg.startswith("--param="):
            out[i] = "--param=" + mask(arg[len("--param=") :])
        elif arg.startswith("-p"):
            eq = "=" if arg.startswith("-p=") else ""
            out[i] = "-p" + eq + mask(arg[2:].removeprefix("="))
    return out


def resolver(env: Mapping[str, str], params: Mapping[str, str]) -> Callable[[str], str | None]:
    """A placeholder's value as rivet takes it: a `-p` param wins over the environment."""
    return lambda name: params[name] if name in params else env.get(name)


def _free_mask(taken: str) -> tuple[str, str]:
    """Two private-use characters that occur in neither the text nor any resolved value."""
    marks = map(chr, range(0xE000, 0xF900))  # pragma: no mutate -- the bounds only pick which free marks come first
    free = (char for char in marks if char not in taken)
    return next(free), next(free)


def resolve_and_load(text: str, value_of: Callable[[str], str | None]) -> tuple[Any, list[str]]:
    """The document rivet would build from this text, and the placeholders nothing resolved (spelled `${name}`)."""
    values = [value_of(name) for name in _PLACEHOLDER.findall(text)]
    mask = _free_mask(text + "".join(v for v in values if v))
    resolved, unresolved = substitute(text, value_of, mask)
    return unmasked(load(resolved), mask), unresolved
