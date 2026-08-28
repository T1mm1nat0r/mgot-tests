"""
Nothing hand-written may live inside a `<!-- gitnexus:start ... end -->` block.

`npx gitnexus analyze` **regenerates that whole block from scratch**, and a
PostToolUse hook runs it automatically after every `git commit` and `git merge`.
Anything sitting between the markers is therefore deleted without warning, on a
schedule nobody is watching.

That is not hypothetical. On 2026-08-28 an analyze run silently removed 82 lines
from the root CLAUDE.md — the `## Skills` table and the whole `## Wrong answers
this project keeps producing` section, which that file itself calls *"the most
valuable thing in this file, because it is the only part that could not be
re-derived from the code"*. It was caught by reading `git diff --stat` before
pushing; the next occurrence might not be.

The fix was structural — move those sections below `<!-- gitnexus:end -->`,
where the tool does not reach. This test pins that arrangement, because the
failure is invisible: the file still looks plausible afterwards, just shorter.

It works by heading, so it also catches a *new* section written into the block
by someone who did not know the rule.
"""

from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[2]

START = '<!-- gitnexus:start -->'
END = '<!-- gitnexus:end -->'

# The H1 and `##` headings GitNexus itself writes. Anything else found inside a
# block is hand-written and will be destroyed on the next analyze.
GENERATED_HEADINGS = {
    '# GitNexus — Code Intelligence',
    '## Always Do',
    '## When Debugging',
    '## When Refactoring',
    '## Never Do',
    '## Tools Quick Reference',
    '## Impact Risk Levels',
    '## Resources',
    '## Self-Check Before Finishing',
    '## Keeping the Index Fresh',
    '## CLI',
}

# Sections whose loss would be worst, named explicitly so the guard says what is
# at stake rather than only that "a heading moved".
MUST_SURVIVE = (
    '## Wrong answers this project keeps producing',
    '## Skills',
)


def _marker_files():
    for path in sorted(REPO.rglob('*.md')):
        if any(part in {'.git', 'node_modules', '.venv'} for part in path.parts):
            continue
        try:
            text = path.read_text(encoding='utf-8')
        except (UnicodeDecodeError, OSError):
            continue
        if START in text and END in text:
            yield path, text


def _headings_inside_block(text):
    inside = text.split(START, 1)[1].split(END, 1)[0]
    return [line.rstrip() for line in inside.splitlines()
            if line.startswith('# ') or line.startswith('## ')]


def test_at_least_one_marker_file_is_scanned():
    """A guard that silently matches nothing guards nothing."""
    assert list(_marker_files()), 'no files with gitnexus markers found — has the format changed?'


def test_no_handwritten_section_sits_inside_a_gitnexus_block():
    offenders = {}
    for path, text in _marker_files():
        stray = [h for h in _headings_inside_block(text) if h not in GENERATED_HEADINGS]
        if stray:
            offenders[str(path.relative_to(REPO))] = stray

    assert not offenders, (
        'these sections sit inside a gitnexus block and will be DELETED by the '
        'next `npx gitnexus analyze` (which the commit hook runs automatically). '
        f'Move them below {END}:\n  '
        + '\n  '.join(f'{f}: {", ".join(h)}' for f, h in sorted(offenders.items()))
    )


@pytest.mark.parametrize('heading', MUST_SURVIVE)
def test_the_irreplaceable_sections_are_outside_the_block(heading):
    text = (REPO / 'CLAUDE.md').read_text(encoding='utf-8')
    assert heading in text, f'{heading} has gone missing from CLAUDE.md entirely'
    assert text.index(heading) > text.index(END), (
        f'{heading} is inside the gitnexus block and will not survive the next '
        'analyze run'
    )
