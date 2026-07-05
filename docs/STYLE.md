# Documentation style guide

Use this guide when you write prose for HFX documentation.
HFX, HydroFabric Exchange, is an open specification and Rust toolkit for a compiled drainage format.
Documentation authors include maintainers, contributors, and LLM agents, which are language-model tools that draft or revise text.
Write for readers who need exact contracts, fast orientation, and reproducible commands.

## Registers

Choose one prose register before you edit a file.
A register is a set of writing rules for a specific documentation surface.
Do not mix registers inside one file.

## Normative specification register

Use this register only in `spec/HFX_SPEC.md` and auxiliary schema documents under `spec/aux/`.
An auxiliary schema document is a specification page that defines a supporting file format used by HFX.
Normative text defines requirements for producers, validators, and consumers.

Define RFC-2119 keywords once near the top of the document.
RFC-2119 keywords are requirement terms such as MUST, MUST NOT, SHOULD, SHOULD NOT, and MAY.
Use those keywords only for requirements.

Write one rule per sentence.
Split compound requirements into separate sentences.

Use present-tense declarative statements.
Prefer `The member table contains one row per member`.
Avoid future tense and process narration.

Do not use second person.
Name the actor instead.
Use `A validator MUST reject the file`.
Do not write `You MUST reject the file`.

Present field definitions as tables.
Give each field one row.
Include the field name, type, requirement level, and meaning.

Give rationale only when a rule surprises the reader.
Rationale is explanatory context that says why a rule exists.
Let unsurprising rules stand alone.

## Guides, overview, and concept register

Use this register for every other documentation source.
Site pages are documentation pages served by the upcoming MkDocs Material site.
Use this register for this file.

Address the reader as `you`.
Use active voice.
Name the actor when a sentence needs one.

Write short declarative sentences.
Prefer one idea per sentence.
Split dense explanations before they hide the action.

Define each domain term on first use with an appositive gloss.
A domain term is project-specific language that a new reader may not know.
Write `a hydrofabric, a dataset describing a region's river network`.
Write `a catchment, the land area that drains to one outlet`.

Use ordered narration for processes.
Start with `First`.
Continue with `Next`.
End with `Finally`.

Give runnable snippets inline `#` comments.
A runnable snippet is a command or code block that a reader can execute.
Follow each runnable snippet with a plain-language recap.

Use semantic line breaks in Markdown source.
A semantic line break means one sentence per source line.
This keeps each sentence on a single line.
That shape makes the line-based style greps reliable.

## Global rules

These rules apply to both registers.
They are also mechanically checkable or reviewable.

## No em dash

Never use the em dash character, U+2014, in documentation sources.
Use a comma, an appositive, or two sentences.
The mechanical check is a plain character match.
The check has no awareness of code fences.
The placeholder `<U+2014>` stands for the raw em dash character, which this file cannot contain.

Bad:

```text
HFX links adapters to engines <U+2014> one compiled contract.
```

Good:

```text
HFX links adapters to engines, one compiled contract.
```

## No contrastive negation

State what a thing is.
Do not pair a negation with a contrast in one sentence.
The banned shape is `X is not A, X is B`.

Bad:

```text
HFX is not a database, HFX is a contract.
```

Good:

```text
HFX is a contract between adapters and engines.
```

The grep catches common pronoun forms, including `it`, `this`, `that`, and `they`.
The rule bans every form, including noun subjects.

## No unprovable claims

Make claims that the repository can prove.
Do not invent performance numbers.
Do not claim better-than comparisons against other formats or tools.
Do not cite papers that do not exist.

Bad:

```text
HFX is faster than every native hydrofabric format.
```

Good:

```text
HFX lets engines read one compiled drainage contract.
```

## Gloss or link jargon

Gloss or link every jargon term on first use.
Jargon is specialized language that a reader outside this repository may miss.
Maintain a Glossary page on the site.
The Glossary page is authored in a later change.

Bad:

```text
The adapter writes Pfafstetter codes.
```

Good:

```text
The adapter writes Pfafstetter codes, hierarchical basin identifiers used for watershed nesting.
```

## Micro-conventions

Write `HFX` in uppercase.
Do not write `hfx` in prose unless you name the `hfx` binary or crate.

Use American English spelling.
Write `color`, `neighbor`, and `center`.

Use sentence-case section titles.
Capitalize the first word and proper nouns only.
Prefer `Flow direction tables`.
Avoid title case such as `Flow Direction Tables`.

## Review checklist

Before you commit documentation, read the changed lines in source form.
Check that each sentence sits on its own line.
Check that the selected register matches the file path.
Check that new jargon has a gloss or link.
Check that commands are runnable from the repository root unless the page names another directory.
Check that examples use HFX terms consistently.

## Enforcement

`mkdocs build --strict` is the link and nav gate for the site.
The MkDocs gate is wired in an upcoming change.
MkDocs Material is the documentation site generator used by this repository.

Two grep commands enforce the em dash and contrastive negation bans.
They become a CI gate in an upcoming change.
Run them locally before committing any documentation change.
Both greps scan all `*.md` files under `docs/` and `spec/`.
That scope is exactly the set of sources shipped to the documentation site plus this guide.

The current exclusion list is deliberate.
`docs/decisions/`, `docs/plans/`, and `docs/releases/` are repository-internal records.
Those records are never shipped to the site.
`docs/VERSIONING.md` and `docs/ADAPTER_GUIDE.md` are legacy pages pending rewrite.
Remove those exclusions when their replacements land.
Any file named `CHANGELOG.md` is permanently excluded by basename.
Changelog files are historical records and are never rewritten, per the repository release policy.

On a conforming tree, both commands exit with code 1 because grep found no match.
Three pre-existing em dash hits currently remain in the normative spec.
Those hits are `spec/HFX_SPEC.md` lines 485 and 486 and `spec/aux/d8_raster/v1.md` line 115.
An upcoming change removes those hits before the CI gate lands.
Before that change lands, the em dash command exits with code 0 and reports exactly those hits.
Treat anything beyond those three hits as a new violation.

```bash
# Ban 1: em dash (U+2014). The pattern is built with printf so this
# file never contains the raw character.
EMDASH="$(printf '\342\200\224')"
grep -rn --include='*.md' \
  --exclude-dir=decisions --exclude-dir=plans --exclude-dir=releases \
  --exclude=VERSIONING.md --exclude=ADAPTER_GUIDE.md --exclude=CHANGELOG.md \
  -e "$EMDASH" docs/ spec/

# Ban 2: contrastive negation (pronoun forms).
grep -rniE --include='*.md' \
  --exclude-dir=decisions --exclude-dir=plans --exclude-dir=releases \
  --exclude=VERSIONING.md --exclude=ADAPTER_GUIDE.md --exclude=CHANGELOG.md \
  "\b(it|this|that|they)('s|'re| is| are| was| were)?(n't| not)\b[^.]{0,80}[,;:] ?(it|this|that|they)('s|'re| is| are| was| were)\b" \
  docs/ spec/
```

The contrastive-negation regex is frozen.
Embed it exactly as written here when you wire the CI gate:

```text
\b(it|this|that|they)('s|'re| is| are| was| were)?(n't| not)\b[^.]{0,80}[,;:] ?(it|this|that|they)('s|'re| is| are| was| were)\b
```

The regex has been verified under GNU grep ERE semantics.
GNU grep ERE semantics are the extended regular expression rules used by GNU grep.
The pattern matches the canonical pronoun forms covered by the ban.
Do not tune, extend, or improve the regex.
Do not add language suggesting later adjustment.
The `\b` token requires GNU grep or ugrep.
On macOS, run the command with GNU grep.
