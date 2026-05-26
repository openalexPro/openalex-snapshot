# AI Skills Usage

This project supports a repo-local `skills/` folder to help AI coding agents execute commands and development workflows consistently.

## Goal

Provide operational and development skills specific to `openalex-snapshot` behavior, not generic AI prompting advice.

## Core operational flow

1. `download` / `verify_download`
2. `convert` (with built-in auto-repair) / `verify_convert`
3. `index` / `verify_index`
4. `extract`
5. `schema` / `verify_schema`

## Runtime requirements

- `aws` CLI — required for `download` / `verify_download` only
- No external `duckdb` binary needed — DuckDB is statically linked in the binary

## Configuration precedence (follow in all skills)

1. explicit CLI flags
2. config subcommand section values
3. config `defaults` section values
4. built-in defaults

Note: `--config` is a **global** flag and must precede the subcommand:
```bash
openalex-snapshot --config ./openalex-snapshot.yaml report --latest
```

## Bootstrapping

Create starter skills:

```bash
openalex-snapshot skills --root-dir .
```

Safe defaults:
- creates only missing files,
- preserves existing files unless `--overwrite` is used.

## Expected Skill Structure

- `skills/README.md`
- `skills/cli-operations/SKILL.md`
- `skills/pipeline-runbook/SKILL.md`
- `skills/debug-and-recovery/SKILL.md`
- `skills/development/SKILL.md`
- `skills/release-and-docs/SKILL.md`
- `skills/_templates/skill-template.md`

## Skill Quality Rules

Each skill should include:
1. purpose,
2. required inputs,
3. concrete command patterns,
4. decision rules,
5. failure handling,
6. done criteria,
7. links to canonical docs.

## Canonical References

Skills should reference these docs as source-of-truth:
- `ARCHITECTURE_AND_DECISIONS.md`
- `CLAUDE.md`
- `NEWS.md`
- command docs/man pages
- `docs/commands/extract.md` for ID routing and output behavior

## Maintenance

When command behavior changes:
- update affected skills templates in `skills_templates()` in `src/main.rs`,
- update `AI_SKILLS_USAGE.md` if the skill structure changes,
- update `ARCHITECTURE_AND_DECISIONS.md` if invariants changed,
- record in `NEWS.md`.
