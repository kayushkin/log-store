package server

// Projection of a materialized TurnModel down to what a reader actually renders.
//
// WHY THIS EXISTS. Materialization is non-destructive by design (D9): every stored
// event becomes exactly one Entry, annotated rather than dropped, so the Raw pane can
// reconstruct the stream. That is the right rule for the model and the wrong rule for
// the wire. Measured 2026-08-25 on a real 30-message page (br_1787087723841263691,
// 5000 entries, 9.91 MB of JSON):
//
//	raw                            7.82 MB   78.9%
//	entries flagged duplicate      5.64 MB   56.9%
//	toolInput + toolResult         0.56 MB    5.6%
//	text                           0.08 MB    0.8%
//	what the Turns view renders    0.97 MB    9.7%
//
// The client was being sent ten times what it draws. `raw` is a full second copy of
// the source event stapled to every entry, and the Turns view renders none of it —
// only the Raw pane does, and that now has an endpoint of its own. The duplicates are
// the OTel copies the dedup pass identified; the collapsed view hides every one.
//
// So the projection drops both, and the non-destructive rule is kept where it belongs:
// `/messages/raw` still serves the model exactly as built.
//
// WHAT SURVIVES, AND WHY IT HAS TO. Dropping a duplicate outright would take the
// "2 sources" badge with it — the badge reads the dedup GROUP, and the hidden copy is
// where the second source name lives. Rather than ship 3957 hollowed entries to carry
// that (912 KB, measured), the same fact rides as a group→sources index: 8 groups,
// 0.3 KB. Keeping the skeletons instead lands at 1.81 MB; the index lands at 0.92 MB.

// SourceGroups maps a dedup groupId to the distinct sources that reported it.
//
// This is the whole reason a projected page can drop duplicate entries. dash's Turns
// badge asks "how many sources reported this message?", which it answers by reading
// `groupId` and `source` off every entry INCLUDING the hidden copies. With the copies
// gone the question is unanswerable from the entries alone, so the answer is carried
// directly — the only thing those entries were still being shipped for.
type SourceGroups map[string][]string

// projectForReading returns the model with `raw` stripped from every surviving entry,
// duplicate entries dropped, `turn.entryIds` pruned to match, and a `sourceGroups`
// index carrying what the dropped entries were needed for.
//
// The input is not mutated: `Entries` and `Turns` are rebuilt. Callers hand this a
// freshly materialized model, but a projection that quietly edited its argument would
// be a trap for the next caller that does not.
func projectForReading(m TurnModel) TurnModel {
	// Built from EVERY entry, before any are dropped — that is the point of it.
	groups := SourceGroups{}
	for _, e := range m.Entries {
		if e.GroupID == "" {
			continue
		}
		if !containsString(groups[e.GroupID], e.Source) {
			groups[e.GroupID] = append(groups[e.GroupID], e.Source)
		}
	}

	kept := make(map[string]Entry, len(m.Entries))
	for id, e := range m.Entries {
		if e.Duplicate {
			continue
		}
		e.Raw = nil
		kept[id] = e
	}

	// Prune the turns to the entries that survived. A turn referencing an id that is
	// no longer in the map is not cosmetic: chat-core's collapsed-view selector looks
	// each id up and reads `.duplicate` off the result, so a missing entry reads as
	// "not a duplicate" and the view tries to render nothing.
	turns := make([]Turn, 0, len(m.Turns))
	for _, t := range m.Turns {
		entryIDs := make([]string, 0, len(t.EntryIDs))
		for _, id := range t.EntryIDs {
			if _, ok := kept[id]; ok {
				entryIDs = append(entryIDs, id)
			}
		}
		// A turn whose every entry was a duplicate is still a turn that happened, and
		// dropping it would leave a hole in the transcript rather than a collapsed
		// row. It rides through empty.
		t.EntryIDs = entryIDs
		turns = append(turns, t)
	}

	m.Entries = kept
	m.Turns = turns
	if len(groups) > 0 {
		m.SourceGroups = groups
	}
	return m
}

func containsString(haystack []string, needle string) bool {
	for _, s := range haystack {
		if s == needle {
			return true
		}
	}
	return false
}
