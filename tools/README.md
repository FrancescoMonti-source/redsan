# DOCEDS trimming audit

Three scripts, outside `R/` because they are workflows rather than package API.
They answer different questions and are easy to confuse — doing so cost three
rounds of wrong conclusions during the work that produced the current rules.

What they audit is `trim_doceds_text()` and the families in
`R/doceds-trim-patterns.R`. They need a real corpus, which is why they are
scripts and not tests: every defect these found was a document shape nobody had
thought to write a test for.

| script | question | when |
|---|---|---|
| `explore_doceds_repetition.R` | what noise still reaches the model? | discovery |
| `measure_doceds_families.R` | does the rule I wrote fire, and what does it earn? | verification |
| `audit_doceds_prose.R` | does any rule take clinical text with it? | before shipping |

Reading the first to answer the second is indirect and misleads. A family can be
absent from a candidate list because it works, because the sample changed, or
because nobody looked far enough down the list. The measurement answers it
directly: **a family reported with zero documents is broken.**

Neither of those two can answer the third question, and the third is the one
that decides whether a rule may ship at all. Leaving noise in the prompt costs
tokens; removing a sentence costs evidence that no later stage can recover.

## The loop

```
explore  →  read the top clusters  →  inspect one RAW document
                                            ↓
        re-explore  ←  measure  ←  write a bounded family
```

Every rule that works makes its cluster disappear from the next exploration.
That is what `pre_trim = TRUE` buys: coverage stops being a list of anchors this
directory has to keep in step with `R/doceds-trim-patterns.R` and becomes a
property of the input.

## How the exploration works

Each document is normalized (casefolded, dates and page numbers folded,
de-identification placeholders folded to `<ph>`), then the first 15 percent and
last 30 percent are cut into overlapping 8-word shingles. A shingle is counted
**at most once per EVTID**, which is the safety argument for everything that
follows: a passage repeated verbatim across hundreds of distinct stays cannot be
patient-specific content. Shingles clearing `min_evtids` are stitched back into
passages, tolerating `max_gap` missing fingerprints, and passages are then
clustered by shingle overlap — Jaccard or containment, union-find — so the
dozen variants of one letterhead collapse into one row instead of competing with
each other in the ranking.

Ranking is by `yield_chars`, the characters a rule matching that passage would
remove, not by support. Support says which passage is most common; yield says
which is worth the risk of writing a regex for. `corpus_share` says when to
stop.

```r
devtools::load_all(".")                    # required: pre_trim calls the package
source("tools/explore_doceds_repetition.R")
result <- explore_repeated_doceds(docs, sample_evtids = 5000L)
clustered <- cluster_repeated_spans(result$spans, result$summary$corpus_chars)
clustered$clusters[!(contains_clinical_lead)][1:40]
```

Scale `min_evtids` with the corpus so the relative support stays constant: 10
for a 5,000-EVTID sample, 50 for the full 26,000. Leaving it low on the full
corpus buries the head of the ranking under thousands of marginal clusters.

## How the measurement works

```r
source("tools/measure_doceds_families.R")
yields <- measure_doceds_families(docs)          # a DOCEDS frame or a chr vector
yields$families                                  # `standalone_chars`, by yield
yields$inline_rules                              # the two within-line rules
yields$removed_share                             # the distribution, not a mean
yields$near_total_match_detected                 # documents left untrimmed
show_doceds_removals(docs$RECTXT[[i]])           # one document, cuts in context
```

`standalone_chars` is what a family would remove **on its own**. The figures
overlap and do not sum to the total, deliberately: the question a family has to
answer is what would stay behind if it alone were dropped, so a span two rules
both matched is counted once for each. Reading them off the merged spans instead
credits every family with the union of everything it happened to touch, which
makes a rule that only ever fires inside another one look exactly as valuable as
the rule containing it.

`show_doceds_removals()` is the check to run before trusting a new family. A
regex written from exploration output is written from lowercased,
whitespace-collapsed text and **cannot see line structure**. Reading one raw
document settled in five minutes what three rounds of inference had got wrong:
the letterhead is 48 physical lines, the letter date sits at line 49 rather than
at the top, and merge fields appear as ` MERGEFIELD Adresse_1 ` as well as
`«adresse_1»`.

## How the prose audit works

```r
source("tools/audit_doceds_prose.R")
audit <- audit_doceds_prose(docs)          # 20,000 documents by default
audit$by_family                            # `with_prose` is the column
audit$warnings                             # must be 0, see below
show_doceds_prose_hits(audit)              # read every hit, in context
```

It re-derives every span the rules removed from a real corpus and looks inside
each one for clinical narrative. The discriminator is grammar, not vocabulary:
`DOCEDS_PROSE_MARKERS` matches a subject with a tense — someone observing,
deciding, reporting a change. Drug names and diagnoses appear in consent forms
and laboratory panels, so a vocabulary list flags hundreds of correct removals
and buries the real hits.

Two results are not optional. The target for `with_prose` is not zero but **as
low as reasonably possible**: every hit has to be read and classed — a correct
removal, or a defect — and whatever is left has to be a residue somebody decided
to accept, in writing, with the number next to it. Zero is not the stronger
claim it looks like, because a gate returns zero when the rules keep prose and
also when the markers stopped matching. `warnings` has to be zero too: it counts documents where
the regex engine ran out of backtracking budget, and those are trimmed
silently and incompletely — a non-zero count invalidates the audit rather than
merely annoying it. Thirty-five documents were failing that way, and the
failure is invisible in every other measurement.

`prose` is a parameter, and swapping it is how a second class of loss was
found. Narrative is not the only thing worth keeping: a vital sign is a label
and a number with no verb in it, invisible to the narrative markers. Passing a
pattern for filled constants instead — `Poids : 144 kg`, `TA : 130/80` — showed
which families were removing measurements, and the fix that followed is in the
package: a line carrying a constant survives whatever family claimed it, and
the removal splits around it. Any class of content can be audited the same way;
write the pattern for it and read what comes back.

It reads `removed_intervals`, which the projection reports and which covers
**every** destructive rule: the block families, the preamble, and the two rules
that act inside a line. Nothing is reconstructed.

That last part was not always true, and the gap is worth remembering because it
is the shape of the worst bug this work produced. The placeholder and fill-run
rules used to be `gsub()` substitutions applied *after* the cut, which put them
outside the audit, outside the protection for measured constants, and after the
near-total check — so a document the check had rescued was edited anyway. A
field pattern that was too broad therefore cost clinical text in the one place
nothing was watching, and one did: `«[^»]{1,40}»` read every French quotation as
an unfilled Word merge field and deleted `« C4d positif sans évidence de rejet »`
along with the template leaks. An audit that covers four of five destructive
rules does not report four fifths of the risk; it reports none of the risk in
the rule it cannot see.

`doceds_removed_lines(docs, "lab_table", pattern = ...)` answers the companion
question: not how much a family removed but *what*. A rule with a permissive
alternative earns its characters from the table it was written for and then
quietly takes whatever else has that shape. Two dozen panel titles repeated
thousands of times is a rule doing its job; a long tail of sentences is not.
Read the longest lines first — a swallowed sentence occurs once, so it sits at
the bottom of a frequency ranking and at the top of a length ranking.

## Reading what a family removes

Three commands, three minutes, and they answer a question no aggregate can:

```r
lines <- doceds_removed_lines(docs, "establishment_letterhead", sample_size = NULL)
head(lines, 12)                          # is it doing its job?
head(lines[order(-lines$chars), ], 12)   # has it swallowed a sentence?
sum(lines$chars[lines$n == 1L])          # how much unique content does it take?
```

The first two are reading, the third is a number. A healthy rule looks like
`establishment_letterhead` does: 388 distinct lines over 283,092 occurrences,
so roughly 730 repetitions each; the frequency ranking is `Dr [DOCTOR]` 138,651
times and the hospital's own name; and the twelve longest lines are all ward
titles, none of them a sentence.

The third number needs its absolute value, not its share. That family's unique
lines are 27.6 percent of the distinct ones, which sounds alarming until you
see they are 3,073 characters in total across the corpus, median 32 each — a
ward-name variant, not a sentence. Large *and* full of sentences is a problem;
large and full of labels is a variable layout.

Reading also surfaces things nobody asked about. Here it showed the upstream
de-identifier replacing the *Guillaume* of "Bois-Guillaume" — a place — with
`[LASTNAME]` in 20 documents out of 21.

## Pricing a bound

A `[\s\S]{0,N}?` gap is not a coverage parameter. It is the distance the rule
travels when it is wrong, and the two directions cost differently: too small
leaves noise in the prompt, too large joins an opening anchor to a closing one
that belongs somewhere else and takes the text between them. So the bound wants
to be as small as the yield allows, and it is worth knowing which bounds are
loose.

Measuring it means capturing the gap, because the length of the match does not
measure it. Widen every gap in the pattern and wrap each one in a **named**
group — named so nothing has to know how many capture groups the pattern
already had — then read `capture.length`:

```r
GAP <- "(?<=\\[\\\\s\\\\S\\]\\{0,)[0-9]+(?=\\})"   # the digits, and only those
```

Two things inflate the match and would ruin a simpler instrument. **`\s+`
between the words of an anchor is itself an unbounded gap**, so
`ald_prescription` shows matches of 5,285 characters behind a `{0,140}` bound;
it is harmless, since `\s` can only consume whitespace and never clinical text,
but it makes match length useless as a proxy. And a **gap inside a repeated
group** — `form_noise`'s `{0,150}` — is a bound per iteration, not a total.

Measured on the whole corpus, the families with a single non-recurring anchor
use a fraction of what they have:

| family | bound | needed at p99 |
|---|---|---|
| `medical_phone` | 500 | 39 |
| `secretariat_header` | 1800 | 201 |
| `rgpd` | 4000 | 643 |
| `patient_documents` | 2500 | 441 |
| `correspondence_block` | 900 | 289 |
| `admissions_notice` | 600 | 226 |

Loose, and left alone: on this corpus the prose audit clears every one of them,
so tightening would be insurance against a future layout rather than repair of
a present defect.

### The trap, which reverses the obvious reading

Widening a bound and counting the documents that newly match looks like it
measures what the bound is costing. It does not, and the number it produces
argues for exactly the wrong change.

`establishment_letterhead` gains 1,803 documents when its gap is widened, and
`ald_prescription` 252. Read naively, both are too tight. But `ald_prescription`
also reports 6,856 of 6,870 captured gaps exceeding its bound — while 6,618
documents match at the bound. Both cannot be true of the same match, and they
are not: **when the gap cannot reach a close, PCRE does not give up on the
document, it retries from the next position** and finds a later opening that
sits nearer its own close. Narrow and wide find different blocks, not the same
block at different sizes. The ALD form carries two headings, so the widened
rule runs from the first across the body to the close after the second.

**For a family whose anchors recur inside one document, the bound is not
coverage — it is what forces each opening to pair with its nearest close.** For
page furniture and form headings, which recur by definition, a tight bound is
semantics rather than caution.

`oltre_limite` is the discriminator: where it is zero the narrow and wide
patterns agree and the measured gap is real; where it is large the number
describes a match the rule never makes. Here that splits the table cleanly, and
the four families it excludes — `contact_header`, `establishment_letterhead`,
`ald_prescription`, `results_header` — are exactly the ones with repeating
anchors.

## What was learned, and generalizes

- **Rank by characters, not by frequency.** The first version of the exploration
  ranked by distinct EVTIDs and never said how much of the corpus a rule would
  recover, which is the only number that tells you when to stop.
- **Never write a regex from normalized text alone.** Get one raw document.
- **Character-class rules beat template rules.** The de-identification
  placeholders, the Word controls and the runs of fill characters generalize to
  any site; every rule naming a service or a phrase is Rouen-specific and will
  need rewriting elsewhere.
- **Both anchors, bounded gap.** A family is removed only when its opening and
  closing boundary are both recognized, with a bounded `[\s\S]{0,N}?` between.
  Removing clinical text is much worse than leaving noise.
- **A counted repetition is the wrong bound for a run.** `{2,12}` stopped the
  header a third of the way in; `{2,60}` made PCRE refuse to compile, because a
  counted repetition expands the group that many times. What makes a run safe is
  stopping at the first line that is not header-shaped, so it is `{2,}+`.
- **The clinical-lead flag is a brake, not a filter.** It flagged 155 of 2,990
  clusters and still missed an operative report sitting sixth in the "safe"
  list. Read the candidates; do not trust the flag to have read them for you.
- **A threshold that was measured carries its measurement.** The one that was
  guessed carries nothing, and the difference between the two is visible at a
  glance in the source. `.DOCEDS_PREAMBLE_LIMIT` sat three lines above a
  threshold with eight lines of justification and had none of its own; measuring
  it took ten minutes and found it inert, excluding 7 documents in 64,871.
  Write the number down whichever way it comes out — an inert threshold nobody
  can prove is inert gets re-litigated every time somebody notices it.
- **A count of what a change would gain is not a measurement of what it costs.**
  Widening a bound and counting new matches says "too tight" about rules whose
  bound is the only thing keeping them honest. Before acting on any count,
  check whether the two versions are finding the same thing.
- **`\b` is not a word boundary in French.** PCRE counts an accented letter as a
  non-word character here, so `pr\b` matches the opening of "prévoir" and
  "prélèvement" while leaving "prescription" alone — a hole that only opens on
  the accented words, which is why no test caught it. Any abbreviation that can
  prefix a word needs `(?![A-Za-zÀ-ÿ])` instead.
- **Audit the exact span, not a reconstruction of it.** The first version of the
  prose audit rebuilt the preamble span as `1:removed_prefix_chars`. The
  preamble does not start at 1 — the walk-back stops at the first line that is
  not header-shaped — so the audit read the kept opening instead of the cut
  frame and cleared a rule it had never looked at. `trim_doceds_text()` now
  returns `removed_prefix_start` so nothing has to be reconstructed.
- **Test the gate, not only what it gates.** `DOCEDS_PROSE_MARKERS` truncates its
  alternatives to stems on purpose, so that one entry covers `réalisé`,
  `réalisée` and `réalisation` — and closed the group with `\b`, which demands a
  word boundary exactly where the stem stops, in the middle of the inflections
  it was shortened to reach. Seven of the fourteen commonest constructions never
  matched: `a été réalisée`, `a été débutée`, `a été arrêté`, `pas de signes`,
  `absence de complications`, `bien tolérée`. `with_prose == 0` was the evidence
  that the trimming keeps no clinical text, and half the evidence was not being
  collected. A gate returns zero when it is working and zero when it is broken;
  the two are told apart only by feeding it text it must flag.
- **A rule keyed on a delimiter inherits every use of that delimiter.** Word
  renders an unfilled merge field as `«Adresse_1»`, and French quotes with the
  same guillemets — so `«[^»]{1,40}»` read `« pied diabétique »` as a template
  leak. In this corpus that is 118,357 spans of which 117,894 are ten field
  names and every one of the other 463 is clinical, because a clinician reaches
  for quotation marks precisely when the wording matters: the histology verdict,
  the wound, the patient's own words. The fix is not a list of field names,
  which is site-specific, but the shape of what sits between the delimiters — a
  merge field has no spaces in it and a quotation almost always does.
- **Every destructive rule needs the same path, or the audit means less than it
  says.** Four of the five rules here contributed spans in original coordinates
  and went through protection, the near-total check, one application and the
  audit. The fifth and sixth — placeholders and fill runs — were `gsub()` calls
  applied after the cut, and so were outside all four. The audit did not report
  four fifths of the risk; it reported none of the risk in the rules it could not
  see, and that is where the worst defect of this work lived for months. A
  pipeline with an exception has the coverage of its exception.
- **Name a diagnostic for what it diagnoses.** The removed-share ceiling was
  called a guard, which invited the reading that it guarantees something about
  clinical text. It does not: a document losing 99.4 percent is not clinically
  different from one losing 99.6. It detects one failure — a rule that matched
  essentially the whole document — and it is now called
  `near_total_match_detected`. Likewise `family_chars` became
  `family_standalone_chars`, because those figures overlap and are not additive,
  and a name that hides that invites somebody to sum them.
- **Measure a guard before believing it.** The removed-share ceiling was written to
  catch a rule running away. It fired on a quarter of documents, every one of
  them a correct trim, and put a megabyte of letterhead back into the prompt —
  partly because it summed overlapping intervals and double-counted, and partly
  because a share test cannot distinguish a runaway from a letter that really is
  all frame.

## Measure on the whole corpus, not on one ward

Rules written against one ward are rules tuned to one ward's templates, and the
only way to know whether they misbehave elsewhere is to run them everywhere.
Two things to look at:

- **Families at zero.** A family that fires on no document across the corpus is
  not ward-specific, it is wrong. `results_pagination` and
  `emergency_letterhead` each matched zero of 62,444 documents and were removed;
  the first had been subsumed by another family, and the emergency letterhead
  turned out to have the same shape as every other one.
- **Per-ward removed share.** Look for a ward that is out of line rather than
  for a high number. Nephrology, where all the rules were written, sits in the
  middle of the ranking at 34 percent while dermatology reaches 55 and radiology
  15. That spread is layout, not damage. A ward whose share jumped past the
  others would be the signal to look for a rule eating prose.

Documents losing more than 98 percent are worth reading once. In this corpus
they are 2.2 percent of the total and every one inspected was an empty shell:
1,600 characters of letterhead, no clinical content, correctly reduced to its
date line.

**Slice by the exclusion the consumer applies, not by the field you happen to
group on.** The trimmer does not care about `RECTYPE` — the exclusion belongs to
redsancoding, which never shows `OPROOM` and `BT` documents to its language
model. The audits copy it so that the population they measure is the one the
baseline below was taken on. Only those two are excluded, so a document whose
`RECTYPE` is `NA` is model-visible; a per-ward measurement that drops missing
values silently loses those documents. Here that hid 2,427 of them — a different
population, mostly patient-facing, that the rules barely touch at 11 percent
removed and where one family fires 512 times and appeared to be dead everywhere
else. `doceds_audit_corpus()` applies the exclusion, which is why both audits go
through it; `DOCEDS_MODEL_EXCLUDED_RECTYPES` is a hand-kept copy and has to be
changed here when redsancoding changes it.

## Reusing this on a new corpus

The exploration and the measurement are corpus-agnostic; only
`.DOCEDS_BOILERPLATE_PATTERNS` in `R/doceds-trim-patterns.R` is site-specific.
For a new site:

1. Run `measure_doceds_families()` first, on everything. Families at zero do not
   apply there.
2. Run the exploration with `pre_trim = TRUE` to see what the existing rules
   miss.
3. Inspect raw documents behind the top clusters before writing anything.
4. Add families, measure, re-explore. Stop when the marginal yield stops paying
   for the risk — below roughly 0.3 percent of the corpus each, it does not.

## Baseline

Whole corpus, 64,871 documents and 205 M characters: **36.9 percent removed**,
**zero PCRE warnings**, **`near_total_match_detected` on zero documents**, and
the per-document share is 0.34 at the median, 0.74 at the ninth decile. The
largest single family is `lab_table` at 12.5 percent of the corpus, spread over
4,420 documents. Per-ward shares run from 10 percent (`RADB`) to 41 percent
(`NEPH`, where every rule was written); documents with no `RECTYPE` sit apart at
11 percent.

Every per-rule figure below is **standalone** — what that rule would remove if
it alone ran — so they overlap and do not sum to 36.9 percent. The inline rules
make that obvious: `fields` is 20.2 M characters against a whole-corpus removal
of 75 M, because most placeholders sit inside a letterhead that a block family
removes anyway, and both are credited. `rule_runs` is 1.0 M and the preamble
4.7 M on the same basis. Only `overall$removed` is a total.

That 36.9 replaces 37.2, and the difference is not a rule doing less work. Most
of it is a global tidy-up that no longer runs: `\h+` before punctuation used to
be collapsed across the whole document after any substitution, which edited
lines no rule had touched and removed a few hundred thousand characters that
were never boilerplate. Anchoring the header grammar accounts for the rest.

The prose audit reports **39 spans out of 360,813 carrying narrative, and zero
warnings**. Two things about that denominator: it was 179,766 before, and it
doubled because the placeholder and fill-run rules are now spans like everything
else and the audit can finally see them. **The 39 hits are unchanged, marker for
marker** — the rules that had never been audited turn out to remove no prose at
all, once the guillemet rule stopped reading French quotations as merge fields.
Coverage doubling without the hit set moving is the strongest form this result
can take.

An earlier baseline recorded 3, which was not the better number it looked like:
the markers were crippled by a trailing `\b` and the gate was reading half of
what it claimed to. The 39 are two groups, and the split is the point — a raw
`with_prose` is a reading list, not a verdict.

- **35 are one sentence**, in `patient_documents`: *"si les symptômes qui vous
  ont amené aux urgences persistent ou s'aggravent"*. It is the discharge advice
  handed to the patient, and it trips `s'aggrave` because grammatically it is a
  clinical sentence — it just is not about this patient. Removing it is correct.
  The marker is not narrowed to suppress it: a gate tuned until it stops
  reporting is a gate tuned back to blindness.
- **4 are genuine annotations** typed into nurse questionnaires between the
  checkboxes — `Prise de poids : 3 Kg du a l'arrêt du tabac`, `Nous avons
  discuté de l'intérêt de perte du poids`, `pas de signes d'alertes`. This is
  the `form_noise` trade-off, already stated where that family is defined, and
  it is **accepted**: four spans in 360,813 is 99.999 percent, the package
  supports quality control of coding rather than reconstructing the record, and
  the alternative costs an eighth of the family's yield. Do not re-litigate it
  without a measurement that beats those numbers.

No header, letterhead, identity, table or inline family produced a single hit.

One reading trap in that table, since it looks like a regression and is not:
`form_noise` shows 118 spans at 3.39 percent where it used to show 2,765 at
0.14. Nothing about the family changed. Inline field spans that sit against a
form block now merge into it and the merged span carries both labels, so those
spans moved to `form_noise+field` — 118 + 2,636 is the 2,754 that were always
there. When labels combine, a percentage changes because its denominator split,
not because its risk moved. Read the counts, not the shares.

Keep these next to a future run. A median that falls, or upper quantiles that
climb, both mean a layout changed. A `with_prose` that rises means a rule
started eating text — but read the hits before believing either direction, since
a zero can also mean the markers stopped matching.

Exploration output and any saved result carry patient-derived text. `.gitignore`
excludes `*.rds`; keep them local.
