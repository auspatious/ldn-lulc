# AI Instructions

## Project context
- Stored in README.md.
- Configured by pyproject.toml.
- Helpful commands stored in Makefile.
- Testing using pytest. Update tests as needed. Don't mock too much that the test only really tests itself.

## General
- Never use emojis or uncommon characters like "→" or "—".
- Use f strings, not %s, %d.
- No code bloat. Implement succinctly.
- No hacks.
- Use /ponytail to help minimise bloat.

## Comments
- Keep comments simple and minimal when needed.
- Never use decorative or padded line comments (e.g. `# 1. Title ───────────`).
- Never add comments describing iterative improvements (e.g. `# improved from previous version`).
- Preserve existing comments when iterating on code, but update where needed.

## Functions
- Always add docstrings to functions, even if simple.
- Don't unnecesarily give function parameters default values. Defaulting to None is good for optional params.
- When updating python code in /ldn folder, also update Makefile and Argo workflow templates in /templates folder. For example if command parameters change.

## Quality
- Give advice on improving code quality.
- Use best practices.
