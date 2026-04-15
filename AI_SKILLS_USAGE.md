# AI Skills Usage

This project supports a repo-local `skills/` folder to help AI coding agents execute commands and development workflows consistently.

## Goal

Provide operational skills that are specific to `openalex-snapshot` behavior, not generic AI prompting advice.

Core operational flow:
1. `download` / `verify_download`
2. `convert` / `verify_convert` / `repair_convert`
3. `index` / `verify_index`
4. `extract`
5. `schema` / `verify_schema`

Configuration precedence to follow in all skills:
1. explicit CLI flags
2. config subcommand section values
3. config `defaults` section values
4. built-in defaults

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
- `NEWS.md`
- command docs/man pages
- `docs/commands/extract.md` for ID routing and output behavior

## Maintenance

When command behavior changes:
- update affected skills,
- update architecture/decision doc if invariants changed,
- record in `NEWS.md`.
