"""The sources file: one YAML file that names every source database of a deployment and its settings, read strictly.

`read_sources_file` reads it and refuses what it cannot read with the file, line and column, and with what was most
likely meant: text pasted from a messenger (typographic quotes, no-break spaces, tabs), shell commands a heredoc
swallowed, a glob that starts with `*` (YAML reads it as an alias), an unknown key, a placeholder other than `{name}`.
The shape is `schemas/rivet-sources.schema.json`, which an editor can use to check the file as it is typed.
"""

from __future__ import annotations

import copy
import difflib
import re
from collections.abc import Iterator, Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

from . import _yaml
from .exports import entries_of, lists_tables, load_tables, mode_of, name_of, tables_of
from .preflight import PreflightRefusal, read_config

SCHEMA_FILE = Path(__file__).with_name("schemas") / "rivet-sources.schema.json"
FROM_CONNECTION = "connection"
TYPOGRAPHIC_QUOTES = "“”„‟‘’‚‛«»″"
INVISIBLE = {
    " ": "a no-break space",
    " ": "a figure space",
    " ": "a narrow no-break space",
    "​": "a zero-width space",
    "‌": "a zero-width non-joiner",
    "‍": "a zero-width joiner",
    "⁠": "a word joiner",
    "﻿": "a byte-order mark",
}
SHELL_LINE = re.compile(
    r"^\s*(?:\$\s|sudo\s|docker(?:\s|-compose\s)|cat\s*<<|cat\s+>|EOF\s*$|cd\s|export\s+[A-Za-z_][A-Za-z_0-9]*=|"
    r"airflow\s+(?:dags|tasks|connections|variables|db)\b|rivet\s+(?:init|run|check|state)\b|kubectl\s|chmod\s|chown\s|mkdir\s)"
)
HEREDOC = re.compile(r"<<-?\s*['\"]?[A-Za-z_]+['\"]?\s*$")
ALIAS_START = re.compile(r"(?:^|[\[,]|:\s|-\s)\s*(\*[^\s,\]}#]*)")
PLACEHOLDER = re.compile(r"\{([^{}]*)\}")
INIT_FLAGS = {
    "include": "--include",
    "exclude": "--exclude",
    "schema": "--schema",
    "tls": "--tls",
    "tls_ca": "--tls-ca",
    "gcs_bucket": "--gcs-bucket",
    "bigquery_project": "--bigquery-project",
    "bigquery_dataset": "--bigquery-dataset",
}
KEY_MARK = "\0key"
SETTING_ALIASES = {
    "dataset": "bigquery_dataset",
    "project": "bigquery_project",
    "bucket": "gcs_bucket",
    "tls_mode": "tls",
    "tables": "include",
    "state_db": "state_database",
    "database": "state_database",
}


class SourcesFileError(ValueError):
    """A sources file this reader refuses; the text names the file, the line and column, and the remedy."""


@dataclass(frozen=True)
class Source:
    """One source of the file: its name and its settings, the template's under the defaults under its own, `{name}`
    replaced."""

    name: str
    settings: Mapping[str, Any]

    def init_args(self) -> list[str]:
        """The `rivet init` arguments its settings make, then its own `init_args`; include and exclude are left to the
        builder, which also compares them with the config once it exists."""
        args: list[str] = []
        for key, flag in INIT_FLAGS.items():
            value = self.settings.get(key)
            if key in ("include", "exclude") or value is None:
                continue
            args += [flag, str(value)]
        return args + list(self.settings.get("init_args") or ())

    def state_database(self) -> str | None:
        """The state database the builder names, or None for the one the state Connection names itself."""
        value = self.settings.get("state_database", "rivet_state_{name}".replace("{name}", self.name))
        return None if value == FROM_CONNECTION else value


@dataclass(frozen=True)
class SourcesFile:
    """A sources file as read: where it is, the deployment's settings and each source."""

    path: Path
    mode: str
    config_dir: Path
    state_root: str
    state_connection: str
    source_connection: str
    odbc_ini: str | None
    template: str | None
    sources: Mapping[str, Source]
    template_missing: str | None = field(default=None)


def _where(path: Path, line: int, column: int) -> str:
    """`file:line:column`, both counted from 1."""
    return f"{path}:{line}:{column}"


def _code_part(line: str) -> str:
    """A line without its comment: from a `#` at the start or after a blank, outside quotes."""
    quote = None
    for index, char in enumerate(line):
        if quote:
            quote = None if char == quote else quote
        elif char in "\"'":
            quote = char
        elif char == "#" and (index == 0 or line[index - 1] in " \t"):
            return line[:index]
    return line


def _pasted(path: Path, text: str) -> Iterator[str]:
    """What text pasted from a chat, a word processor or a terminal leaves in YAML, each with where it is."""
    for number, line in enumerate(text.splitlines(), start=1):
        code = _code_part(line)
        stripped = line.lstrip(" ")
        if stripped.startswith("\t"):
            yield f"{_where(path, number, len(line) - len(stripped) + 1)}: a tab in the indentation; YAML indents with spaces only"
        for column, char in enumerate(line, start=1):
            if char in INVISIBLE and not (number == 1 and column == 1 and char == "﻿"):
                yield (
                    f"{_where(path, number, column)}: {INVISIBLE[char]} (U+{ord(char):04X}), which a messenger or a word "
                    "processor puts in copied text; retype the line with plain spaces"
                )
                break
        for column, char in enumerate(code, start=1):
            if char in TYPOGRAPHIC_QUOTES:
                yield (
                    f"{_where(path, number, column)}: `{char}` is a typographic quote (copied from a messenger or a word "
                    "processor); YAML quotes with a straight \" or '"
                )
                break
        if not stripped.startswith("#") and (SHELL_LINE.search(line) or HEREDOC.search(code)):
            yield (
                f"{_where(path, number, 1)}: this line looks like a shell command, not YAML: `{line.strip()[:60]}`. A "
                "heredoc (`cat <<EOF`) pasted together with the commands after it writes them into the file; keep only "
                "the YAML here and run the commands in the shell"
            )
        match = ALIAS_START.search(code)
        if match and not _quoted_at(code, match.start(1)):
            glob = match.group(1)
            anchored = re.search(rf"&{re.escape(glob[1:])}\b", text) if len(glob) > 1 else None
            yield (
                f"{_where(path, number, match.start(1) + 1)}: `{glob}` is a YAML alias of the anchor `&{glob[1:]}`; a "
                "sources file reads no anchors or aliases: write the value out"
                if anchored
                else f"{_where(path, number, match.start(1) + 1)}: `{glob}` starts with `*`, which YAML reads as an alias, "
                f'not text: quote the glob, "{glob}"'
            )


def _quoted_at(line: str, index: int) -> bool:
    """Whether `index` of `line` is inside a quoted scalar."""
    quote = None
    for char in line[:index]:
        if quote:
            quote = None if char == quote else quote
        elif char in "\"'":
            quote = char
    return quote is not None


def _plain(node: yaml.Node, path: tuple[Any, ...], marks: dict[tuple[Any, ...], yaml.Mark], file: Path) -> Any:
    """The document of a composed node, with the start of each value by its path; a key written twice is refused."""
    marks[path] = node.start_mark
    if isinstance(node, yaml.MappingNode):
        out: dict[Any, Any] = {}
        for key_node, value_node in node.value:
            key = _yaml.text_of(_scalar(key_node))
            marks[(KEY_MARK, *path, key)] = key_node.start_mark
            if key in out:
                mark = key_node.start_mark
                raise SourcesFileError(
                    f"{_where(file, mark.line + 1, mark.column + 1)}: `{key}` is written twice in one mapping; YAML would "
                    "keep only the last one silently; write it once"
                )
            out[key] = _plain(value_node, (*path, key), marks, file)
        return out
    if isinstance(node, yaml.SequenceNode):
        return [_plain(item, (*path, index), marks, file) for index, item in enumerate(node.value)]
    return _scalar(node)


def _scalar(node: yaml.Node) -> Any:
    """A scalar as rivet's reader reads it (YAML 1.2: `off` and `2026-10-01` stay text); a key that is a collection is
    its text."""
    if not isinstance(node, yaml.ScalarNode):
        return str(node.start_mark)
    return _yaml.ConfigLoader("").construct_object(node)


def _composed(path: Path, text: str) -> tuple[Any, dict[tuple[Any, ...], yaml.Mark]]:
    """The document and the mark of each value; a YAML error is refused with its line and column."""
    try:
        node = yaml.compose(text, Loader=_yaml.ConfigLoader)
    except yaml.MarkedYAMLError as exc:
        mark = exc.problem_mark or exc.context_mark
        where = _where(path, mark.line + 1, mark.column + 1) if mark else str(path)
        hint = ""
        if "alias" in str(exc.problem or ""):
            hint = '; a glob that starts with `*` must be quoted: "*_tmp"'
        raise SourcesFileError(f"{where}: not YAML: {exc.problem or exc}{hint}") from None
    except yaml.YAMLError as exc:
        raise SourcesFileError(f"{path}: not YAML: {exc}") from None
    marks: dict[tuple[Any, ...], yaml.Mark] = {}
    if node is None:
        raise SourcesFileError(f"{path}: the file is empty; see docs/dags-from-file.md for its shape")
    return _plain(node, (), marks, path), marks


def schema() -> dict[str, Any]:
    """The JSON Schema of a sources file."""
    return dict(yaml.safe_load(SCHEMA_FILE.read_text(encoding="utf-8")))


def _keys_of(schema_doc: Mapping[str, Any], path: Sequence[Any]) -> list[str]:
    """The keys the schema allows in the mapping at `path`."""
    if not path:
        return sorted(schema_doc["properties"])
    if path[0] in ("defaults", "sources"):
        return sorted(schema_doc["$defs"]["settings"]["properties"])
    return []


def _mark_of(marks: Mapping[tuple[Any, ...], yaml.Mark], path: Sequence[Any]) -> yaml.Mark | None:
    """The mark of the deepest value of `path` the file has."""
    for end in range(len(path), -1, -1):
        mark = marks.get(tuple(path[:end]))
        if mark is not None:
            return mark
    return None


def _schema_errors(path: Path, doc: Any, marks: Mapping[tuple[Any, ...], yaml.Mark]) -> list[str]:
    """Each place the document breaks the schema, as `file:line:column: key: what is wrong`. A source written with no
    settings is read as `{}`, so an error in a source's settings is reported as that setting's own."""
    import jsonschema

    schema_doc = schema()
    checked = copy.deepcopy(schema_doc)
    checked["properties"]["sources"]["additionalProperties"] = {"$ref": "#/$defs/settings"}
    if isinstance(doc, Mapping) and isinstance(doc.get("sources"), Mapping):
        doc = {**doc, "sources": {k: {} if v is None else v for k, v in doc["sources"].items()}}
    out: list[tuple[int, int, str]] = []
    for error in jsonschema.Draft202012Validator(checked).iter_errors(doc):
        at = list(error.absolute_path)
        if error.validator == "additionalProperties" and isinstance(error.instance, Mapping):
            allowed = _keys_of(schema_doc, at)
            for key in sorted(set(error.instance) - set(allowed), key=str):
                close = _meant(str(key), allowed)
                said = f"; did you mean `{close}`?" if close else f"; the keys here are {', '.join(allowed)}"
                mark = marks.get((KEY_MARK, *at, key)) or _mark_of(marks, [*at, key])
                out.append((*_position(mark), f"unknown key `{key}`{said}"))
            continue
        out.append((*_position(_mark_of(marks, at)), f"`{'.'.join(map(str, at)) or '(top)'}`: {_said(error)}"))
    return [f"{_where(path, line, column)}: {text}" for line, column, text in sorted(set(out))]


def _meant(key: str, allowed: Sequence[str]) -> str | None:
    """The allowed key most likely meant by `key`: a known alias, else the closest key or key ending (`datset` is
    `bigquery_dataset`)."""
    if SETTING_ALIASES.get(key) in allowed:
        return SETTING_ALIASES[key]
    names = {k: k for k in allowed} | {k.split("_", 1)[-1]: k for k in allowed if "_" in k}
    close = difflib.get_close_matches(key, list(names), n=1, cutoff=0.6)
    return names[close[0]] if close else None


def _position(mark: yaml.Mark | None) -> tuple[int, int]:
    """(line, column) from 1 of a mark; (1, 1) without one."""
    return (mark.line + 1, mark.column + 1) if mark else (1, 1)


def _said(error: Any) -> str:
    """A schema error in words an operator reads."""
    kind = error.validator
    if kind == "required":
        missing = str(error.message).split("'")[1] if "'" in str(error.message) else str(error.message)
        return f"`{missing}` is required"
    if kind == "enum":
        return f"`{error.instance}` is not one of {', '.join(map(str, error.validator_value))}"
    if kind == "const":
        return f"is {error.instance!r}; the only one is {error.validator_value!r}"
    if kind == "type":
        wanted = error.validator_value
        wanted = " or ".join(wanted) if isinstance(wanted, list) else wanted
        hint = ""
        if wanted == "array" and isinstance(error.instance, str):
            hint = f": write [{error.instance}] or one `- {error.instance}` per line"
        return f"is {type(error.instance).__name__} {error.instance!r}, expected {wanted}{hint}"
    if kind == "pattern":
        if error.validator_value == "^[a-z0-9_]+$":
            return f"`{error.instance}` is not a name of lower-case letters, digits and `_`"
        return f"`{error.instance}` does not read as {error.validator_value}"
    if kind == "minProperties":
        return "names no source; add one under `sources:`"
    if kind == "minLength":
        return "is empty"
    return str(error.message)


def _placeholders(path: Path, doc: Any, marks: Mapping[tuple[Any, ...], yaml.Mark], at: tuple[Any, ...] = ()) -> list[str]:
    """Every `{...}` in a text value other than `{name}`, the one placeholder the file has."""
    out: list[str] = []
    if isinstance(doc, Mapping):
        for key, value in doc.items():
            out += _placeholders(path, value, marks, (*at, key))
    elif isinstance(doc, list):
        for index, value in enumerate(doc):
            out += _placeholders(path, value, marks, (*at, index))
    elif isinstance(doc, str):
        for found in PLACEHOLDER.findall(doc):
            if found != "name":
                line, column = _position(_mark_of(marks, at))
                out.append(
                    f"{_where(path, line, column)}: `{{{found}}}` is no placeholder of this file; the one placeholder is "
                    f"`{{name}}`, the source's name"
                )
    return out


def flag_of(arg: str) -> str:
    """The flag a command-line token sets as clap reads it: `--output=x` is `--output`, `-oX` and `-o=X` are `-o`."""
    if arg.startswith("--"):
        return arg.partition("=")[0]
    if arg.startswith("-"):
        return arg[:2]
    return arg


def _merged(layers: Sequence[Mapping[str, Any] | None], name: str) -> dict[str, Any]:
    """The settings of a source: each layer over the one before, `{name}` replaced in every text."""
    out: dict[str, Any] = {}
    for layer in layers:
        out.update(layer or {})
    return _named(out, name)


def _named(value: Any, name: str) -> Any:
    """`value` with `{name}` replaced in every text, keys of a mapping included."""
    if isinstance(value, str):
        return value.replace("{name}", name)
    if isinstance(value, Mapping):
        return {_named(k, name): _named(v, name) for k, v in value.items()}
    if isinstance(value, list):
        return [_named(v, name) for v in value]
    return value


def template_settings(config: Path) -> dict[str, Any]:
    """The GCS bucket, BigQuery project and TLS mode a source's existing config writes: what `template:` gives."""
    doc = read_config(str(config))
    load = doc.get("load") or {}
    source = doc.get("source") or {}
    tls = source.get("tls") if isinstance(source, Mapping) else None
    buckets = sorted(
        {
            str(_yaml.text_of(dest.get("bucket")))
            for e in entries_of(doc)
            for dest in [e.get("destination")]
            if isinstance(dest, Mapping) and dest.get("type") == "gcs" and dest.get("bucket")
        }
    )
    if len(buckets) > 1:
        raise SourcesFileError(
            f"{config} writes to more than one GCS bucket ({', '.join(buckets)}): name `gcs_bucket` in `defaults`"
        )
    out: dict[str, Any] = {}
    if buckets:
        out["gcs_bucket"] = buckets[0]
    if isinstance(load, Mapping) and _yaml.text_of(load.get("target")) == "bigquery" and load.get("project"):
        out["bigquery_project"] = str(_yaml.text_of(load["project"]))
    if isinstance(tls, Mapping) and tls.get("mode"):
        out["tls"] = str(_yaml.text_of(tls["mode"]))
    return out


def _source_errors(path: Path, name: str, settings: Mapping[str, Any], line: int, column: int) -> list[str]:
    """What a source's merged settings may not hold: an `init_args` flag a setting owns."""
    where = _where(path, line, column)
    owned = {INIT_FLAGS[k] for k in INIT_FLAGS if settings.get(k)}
    taken = sorted({flag_of(a) for a in settings.get("init_args") or () if flag_of(a) in owned})
    if taken:
        return [f"{where}: source `{name}`: init_args sets {', '.join(taken)}, which its settings already set; keep one"]
    return []


def read_sources_file(path: str | Path) -> SourcesFile:
    """The sources file at `path`, checked; anything it cannot read is a `SourcesFileError` naming every place."""
    path = Path(path)
    try:
        raw = path.read_bytes()
    except OSError as exc:
        raise SourcesFileError(f"{path}: cannot read the sources file: {exc.strerror or exc}") from None
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise SourcesFileError(f"{path}: not UTF-8 at byte {exc.start}; save the file as UTF-8") from None
    pasted = list(_pasted(path, text))
    if pasted:
        raise SourcesFileError("\n".join(pasted))
    doc, marks = _composed(path, text.lstrip("﻿"))
    errors = _schema_errors(path, doc, marks) + _placeholders(path, doc, marks)
    if errors:
        raise SourcesFileError("\n".join(errors))
    names = list(doc["sources"])
    template = doc.get("template")
    if template is not None and template not in names:
        line, column = _position(marks.get(("template",)))
        errors.append(f"{_where(path, line, column)}: template `{template}` is not a source of this file ({', '.join(names)})")
    sources: dict[str, Source] = {}
    base = path.parent
    config_dir = Path(doc.get("config_dir") or "rivet")
    config_dir = config_dir if config_dir.is_absolute() else base / config_dir
    template_layer: dict[str, Any] = {}
    template_missing = None
    if template is not None and template in names:
        config = config_dir / f"{template}.yaml"
        try:
            template_layer = template_settings(config)
        except (OSError, PreflightRefusal) as exc:
            template_missing = f"template `{template}`: its config {config} cannot be read yet ({exc})"
    for name in names:
        own = _merged([doc.get("defaults"), doc["sources"][name]], name)
        flags = {flag_of(a) for a in own.get("init_args") or ()}
        inherited = {k: v for k, v in template_layer.items() if name != template and INIT_FLAGS[k] not in flags}
        settings = _merged([inherited, doc.get("defaults"), doc["sources"][name]], name)
        line, column = _position(marks.get(("sources", name)))
        errors += _source_errors(path, name, settings, line, column)
        sources[name] = Source(name, settings)
    if errors:
        raise SourcesFileError("\n".join(errors))
    return SourcesFile(
        path=path,
        mode=doc.get("mode") or "cdc",
        config_dir=config_dir,
        state_root=doc["state_root"],
        state_connection=doc["state_connection"],
        source_connection=doc.get("source_connection") or "{name}",
        odbc_ini=doc.get("odbc_ini", "/etc/odbc.ini"),
        template=template,
        sources=sources,
        template_missing=template_missing,
    )


def glob_matches(pattern: str, name: str) -> bool:
    """rivet init's table glob (src/init/mod.rs `glob_match`): the whole name, `*` any run, `?` one character, every
    other character literal, case-sensitive."""
    regex = "".join(".*" if c == "*" else "." if c == "?" else re.escape(c) for c in pattern)
    return re.fullmatch(regex, name, re.S) is not None


def kept(name: str, include: Sequence[str], exclude: Sequence[str]) -> bool:
    """Whether `rivet init --include ... --exclude ...` keeps a table: exclude wins, and no include keeps all."""
    if any(glob_matches(g, name) for g in exclude):
        return False
    return not include or any(glob_matches(g, name) for g in include)


@dataclass(frozen=True)
class Drift:
    """How a source's config differs from the include and exclude of its sources file: the tables it captures that
    they leave out, and the tables an include names literally that it does not capture."""

    extra: Sequence[str]
    missing: Sequence[str]
    places: Mapping[str, Sequence[str]]

    def __bool__(self) -> bool:
        return bool(self.extra or self.missing)

    def text(self, config: Path) -> str:
        """The drift in words, with where each extra table is written in the config."""
        parts = []
        if self.extra:
            where = "; ".join(f"`{t}`: {', '.join(self.places[t])}" for t in self.extra)
            parts.append(f"{config} captures {', '.join(self.extra)}, which the sources file leaves out ({where})")
        if self.missing:
            parts.append(f"the sources file includes {', '.join(self.missing)}, which {config} does not capture")
        return "; ".join(parts)


def skipped_by_init(text: str) -> list[str]:
    """The tables `rivet init` left out of the stream it wrote, from its `# SKIPPED <table>:` lines."""
    return [m.strip("'\"") for m in re.findall(r"^\s*# SKIPPED (.+?): ", text, re.M)]


def drift(
    doc: Mapping[str, Any], mode: str, include: Sequence[str], exclude: Sequence[str], skipped: Sequence[str] = ()
) -> Drift:
    """How the config `doc` of a source of `mode` differs from the include and exclude its sources file names. A table
    is compared by its name without the schema, as init matched it; an include glob that may match a table the
    source has and the config does not cannot be judged without the source, and is left alone."""
    exports = [e for e in entries_of(doc) if (mode_of(e) == "cdc") == (mode == "cdc")]
    places: dict[str, list[str]] = {}
    for entry in exports:
        for table in tables_of(entry):
            where = f"`tables:` of export `{name_of(entry)}`" if lists_tables(entry) else f"export `{name_of(entry)}`"
            places.setdefault(table, []).append(where)
    for table in load_tables(doc):
        if table in places:
            places[table].append(f"`load.tables.{table}`")
    source = doc.get("source") or {}
    mongo = str(_yaml.text_of(source.get("type")) or "").startswith("mongo") if isinstance(source, Mapping) else False
    bare = {table: table if mongo else table.rsplit(".", 1)[-1] for table in places}
    extra = [t for t in places if not kept(bare[t], include, exclude)]
    literal = [g for g in include if "*" not in g and "?" not in g and kept(g, (), exclude)]
    missing = [g for g in literal if g not in bare.values() and g not in places and g not in skipped]
    return Drift(extra=extra, missing=missing, places=places)
