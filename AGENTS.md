# AI Instructions

## Project context
- Stored in README.md.
- Configured by pyproject.toml.
- Test commands stored in Makefile.
- Testing using pytest. Update tests as needed.

## General
- Never use emojis or uncommon characters like "→" or "—".
- Use f strings, not %s, %d.

## Comments
- Keep comments simple and minimal when needed.
- Never use decorative or padded line comments (e.g. `# 1. Title ───────────`).
- Never add comments describing iterative improvements (e.g. `# improved from previous version`).
- Preserve existing comments when iterating on code.

## Functions
- Always add docstrings to functions, even if simple.
- Don't unnecesarily give function parameters default values. Defaulting to None is good for optional params.

## Quality
- Give advice on improving code quality.
- Use best practices.
- Advise if a proposed solution is a "hack".
